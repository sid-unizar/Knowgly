package sid.MetricsAggregation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.exception.ConvergenceException;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.MultiKMeansPlusPlusClusterer;
import org.apache.jena.rdf.model.Resource;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sid.MetricsGeneration.MetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * An endpoint-independent metrics aggregator, which creates VirtualDocumentTemplate instances based on the metrics
 * stored in the endpoint.
 * <p>
 * All engines are aware of the location of the metrics they query, checking whether the endpoint supports subgraphs
 * or not (HDT metrics are not stored in subgraphs, but the rest of them are). This makes them source-independent.
 * <p>
 * The base implementation takes care of running KMeans, with the specializations only providing the input based on
 * the metrics they use
 */
public abstract class MetricsAggregator implements Cloneable {
    // Number of KMeans++ executions to test, returning the one with the best clustering
    public static final int K_MEANS_PLUS_PLUS_ATTEMPTS = 5;

    /**
     * Optional value returned by getClusterInput(), which can be either the input to KMeans or a template meant to
     * be returned instead in order to skip KMeans calculation. The latter is only done for now on entity-based
     * aggregators, which will return a cached global template for entities with no allowed types
     */
    public static class OptionalClusterInputOrTemplate {
        private final List<ResourceWrapper> clusterInput;
        private final VirtualDocumentTemplate template;

        public OptionalClusterInputOrTemplate(List<ResourceWrapper> clusterInput) {
            this.clusterInput = clusterInput;
            this.template = null;
        }

        public OptionalClusterInputOrTemplate(VirtualDocumentTemplate template) {
            this.clusterInput = null;
            this.template = template;
        }

        public List<ResourceWrapper> getClusterInput() {
            return clusterInput;
        }

        public VirtualDocumentTemplate getTemplate() {
            return template;
        }

        public boolean isClusterInput() {
            return clusterInput != null;
        }
    }

    /**
     * Wrapper over a Resource to include a n-dimensional score, to be used
     * in the k-means algorithm
     */
    public static class ResourceWrapper implements Clusterable, Comparable<ResourceWrapper> {
        public final double[] points;
        private final Resource resource;

        public ResourceWrapper(Resource resource, double importance) {
            this.resource = resource;
            this.points = new double[]{importance};
        }

        public ResourceWrapper(Resource resource, double[] points) {
            this.resource = resource;
            this.points = points;
        }

        public Resource getResource() {
            return resource;
        }

        public double[] getPoint() {
            return points;
        }

        @Override
        public int compareTo(MetricsAggregator.ResourceWrapper other) {
            return Double.compare(points[0], other.points[0]);
        }
    }

    public static final String CONFIGURATION_FILE = "configuration/metricsAggregatorConfiguration.json";
    public static final String CLUSTERS_CONF = "buckets";
    public static final String K_MEANS_ITERATIONS_CONF = "kMeansIterations";
    public static final String PREDICATES_OVERRIDE_CONF = "predicatesOverride";
    public static final String TYPE_PREDICATES_OVERRIDE_CONF = "typePredicatesOverride";
    public static final String TYPE_PREFIXES_CONF = "typeNamePrefixes";
    public static final String TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_CONF = "typePredicatesOverrideFieldWeight";
    public static final String DIVIDE_DATATYPE_AND_OBJECT_PROPERTIES_CONF = "divideDataTypeAndObjectProperties";
    public static final String CREATE_RELATIONS_FIELDS_CONF = "createRelationsFields";

    public static final String INDEX_CONFIGURATION_FILE = "configuration/indexConfiguration.json";
    public static final String FIELD_NAME_CONF = "fieldName";
    public static final String FIELD_WEIGHTS_CONF = "bucketWeights";
    public static final String DATATYPE_PROPERTIES_WEIGHTS = "dataTypePropertiesWeights";
    public static final String OBJECT_PROPERTIES_WEIGHTS = "objectPropertiesWeights";
    public static final String RELATIONS_FIELDS_WEIGHTS = "relationsFieldWeights";
    public static final String RELUSTERIZE_CONF = "reclusterize";

    public static final String GET_DATA_TYPE_PROPERTIES_QUERY = "get_data_type_properties.sparql";
    public static final String GET_OBJECT_PROPERTIES_QUERY = "get_object_properties.sparql";

    // Galago doesn't like longer index field names, so they are shortened to these suffixes
    public static final String DATATYPE_PROPERTIES_SUFFIX = "dp";
    public static final String OBJECT_PROPERTIES_SUFFIX = "op";
    public static final String TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_NAME = "typesField";
    public static final String RELATIONS_FIELD_NAME = "relations";

    protected int kMeansClusters;
    public int kMeansIterations;
    protected String fieldName;

    // Weights for each field, in descending order
    // If !divideDataTypeAndObjectProperties
    private List<Double> fieldWeights;
    // If divideDataTypeAndObjectProperties
    private List<Double> datatypePropertiesFieldWeights;
    private List<Double> objectPropertiesFieldWeights;

    // If createRelationsFields
    private List<Double> relationsFieldWeights;

    // Predicates to directly add to the highest priority cluster, without runnning them through KMeans
    //
    // They are filtered by the allowedPredicates list
    private Set<String> predicatesOverride;

    // Type predicates to include in a separate 'types' field, with a fixed 1.0 weight. If empty, it will not be created
    private Set<String> typePredicatesOverride;
    private double typePredicatesOverrideFieldWeight;

    // Type prefixes which dictate which ones we will take into account when calculating importance metrics
    private Set<String> typePrefixes;

    // Predicates to include in the clustering, with others being excluded. Can block predicates in predicatesOverride
    // If empty, all predicates will be included
    //
    // Not controlled via the configuration file (this is meant to separe datatype properties from object properties
    // in the pipeline, so that we have a different clusterization for each)
    private Set<String> allowedPredicates;

    // Clusterize datatype properties and object properties separately, creating a set of fields for each (essentially
    // duplicating them)
    private boolean divideDataTypeAndObjectProperties;

    // Create additional relations fields for entity linking which will only contain URIs, by clustering object
    // properties only
    private boolean createRelationsFields;
    // After clustering the predicates, remove the last cluster and reclusterize again (potentially filters spurious 
    // data and reorders the remaining predicates)
    private boolean reclusterize;

    // Types which should be ignored/filtered by all metrics Aggregator implementations
    private Set<String> typesToIgnore = Set.of(SPARQLEndpoint.VIRTUAL_TYPE);

    // Types to be allowed exclusively by all metrics Aggregator implementations. If non-empty, typesToIgnore
    // must be ignored
    private Set<String> allowedTypes = new HashSet<>();
    // Only used for subsuming predicates
    //private Set<String> subsumedPredicates = new HashSet<>();

    private void fromConfigurationFile() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));
        byte[] mapDataIndex = Files.readAllBytes(Paths.get(INDEX_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);
        JsonNode rootNodeIndex = objectMapper.readTree(mapDataIndex);

        this.kMeansClusters = rootNode.get(CLUSTERS_CONF).asInt();
        this.kMeansIterations = rootNode.get(K_MEANS_ITERATIONS_CONF).asInt();
        this.fieldName = rootNodeIndex.get(FIELD_NAME_CONF).asText();
        this.fieldWeights = objectMapper.convertValue(rootNode.get(FIELD_WEIGHTS_CONF),
                new TypeReference<List<Double>>() {
                });
        this.datatypePropertiesFieldWeights = objectMapper.convertValue(rootNode.get(DATATYPE_PROPERTIES_WEIGHTS),
                new TypeReference<List<Double>>() {
                });
        this.objectPropertiesFieldWeights = objectMapper.convertValue(rootNode.get(OBJECT_PROPERTIES_WEIGHTS),
                new TypeReference<List<Double>>() {
                });
        this.relationsFieldWeights = objectMapper.convertValue(rootNode.get(RELATIONS_FIELDS_WEIGHTS),
                new TypeReference<List<Double>>() {
                });
        this.predicatesOverride = objectMapper.convertValue(rootNode.get(PREDICATES_OVERRIDE_CONF),
                new TypeReference<HashSet<String>>() {
                });
        this.typePredicatesOverride = objectMapper.convertValue(rootNode.get(TYPE_PREDICATES_OVERRIDE_CONF),
                new TypeReference<HashSet<String>>() {
                });
        this.typePredicatesOverrideFieldWeight = rootNode.get(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_CONF).asDouble();
        this.typePrefixes = objectMapper.convertValue(rootNode.get(TYPE_PREFIXES_CONF),
                new TypeReference<HashSet<String>>() {
                });

        // Ensure that all weights are in descending order
        fieldWeights.sort(Collections.reverseOrder());
        datatypePropertiesFieldWeights.sort(Collections.reverseOrder());
        objectPropertiesFieldWeights.sort(Collections.reverseOrder());
        relationsFieldWeights.sort(Collections.reverseOrder());

        this.allowedPredicates = new HashSet<>(); // All predicates allowed
        this.divideDataTypeAndObjectProperties = rootNode.get(DIVIDE_DATATYPE_AND_OBJECT_PROPERTIES_CONF).asBoolean();
        this.createRelationsFields = rootNode.get(CREATE_RELATIONS_FIELDS_CONF).asBoolean();
        this.reclusterize = rootNode.get(RELUSTERIZE_CONF).asBoolean();

        // Only used for subsuming predicates
        /*var ep = LocalHDTSPARQLEndpoint.fromConfigurationFile();
        var pair = ImportanceMetricsGeneratorHDT.getSubsumedPredicates(ep);
        var subsumedPredicateIDs = pair.getValue();

        for (long subsumedPredicateID : subsumedPredicateIDs) {
            subsumedPredicates.add(ep.hdt.getDictionary().idToString(subsumedPredicateID, TripleComponentRole.PREDICATE).toString());
        }*/
    }

    private static boolean divideDataTypeAndObjectProperties() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return rootNode.get(DIVIDE_DATATYPE_AND_OBJECT_PROPERTIES_CONF).asBoolean();
    }

    private static boolean createRelationsFields() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return rootNode.get(CREATE_RELATIONS_FIELDS_CONF).asBoolean();
    }

    /**
     * Set the predicates to include in the clustering, with others being excluded. Can block predicates in
     * predicatesOverride.
     * <p>
     * If empty (default), all predicates will be included
     */
    public void setAllowedPredicates(Set<String> allowedPredicates) {
        this.allowedPredicates = allowedPredicates;
    }

    /**
     * Set the predicates to directly add to the highest priority cluster, without runnning them through KMeans
     * <p>
     * They are filtered by the allowedPredicates list
     * <p>
     * Empty by default
     */
    public void setPredicatesOverride(Set<String> predicatesOverride) {
        this.predicatesOverride = predicatesOverride;
    }

    /**
     * Basic constructor. Initializes the clustering configuration via its configuration file
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public MetricsAggregator() throws IOException {
        fromConfigurationFile();
    }

    public MetricsAggregator(int kMeansClusters, int kMeansIterations) {
        this.kMeansClusters = kMeansClusters;
        this.kMeansIterations = kMeansIterations;
        this.allowedPredicates = new HashSet<>();
        this.allowedTypes = new HashSet<>();
        this.predicatesOverride = new HashSet<>();
        this.typePredicatesOverride = new HashSet<>();
        this.typePrefixes = new HashSet<>();
        this.typesToIgnore = new HashSet<>();
    }

    /**
     * The only method to implement, which will run any custom SPARQL/HDT queries and use any metrics
     * in order to generate the cluster inputs (a predicate - weight list) for the k-means algorithm or
     * a template (to avoid calculating KMeans, used in entity-based aggregators)
     * <p>
     * The weights can be n-dimensional, but if combined with a different metrics aggregator both of
     * them should have the same dimensions
     *
     * @param forceReturnClusterInput If true, the returned value is expected to be a clusterInput list
     * @throws IOException If there is any IO error happens when loading a SPARQL query, if used
     */
    public abstract OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint,
                                                                           boolean forceReturnClusterInput)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Get an empty vdoc template with no contents inside its fields, and the weights dictated by the
     * configuration file. Useful when searching after indexing, where we only need to know
     * the number and weights for each field.
     * <p>
     * Every metrics aggregator will have assigned these weights.
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static VirtualDocumentTemplate getEmptyVirtualDocumentTemplate() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));
        byte[] mapDataIndex = Files.readAllBytes(Paths.get(INDEX_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);
        JsonNode rootNodeIndex = objectMapper.readTree(mapDataIndex);

        int kMeansClusters = rootNode.get(CLUSTERS_CONF).asInt();
        String fieldName = rootNodeIndex.get(FIELD_NAME_CONF).asText();
        List<Double> fieldWeights = objectMapper.convertValue(rootNode.get(FIELD_WEIGHTS_CONF),
                new TypeReference<List<Double>>() {
                });
        List<Double> datatypePropertiesFieldWeights = objectMapper.convertValue(rootNode.get(DATATYPE_PROPERTIES_WEIGHTS),
                new TypeReference<List<Double>>() {
                });
        List<Double> objectPropertiesFieldWeights = objectMapper.convertValue(rootNode.get(OBJECT_PROPERTIES_WEIGHTS),
                new TypeReference<List<Double>>() {
                });
        List<Double> relationsFieldWeights = objectMapper.convertValue(rootNode.get(RELATIONS_FIELDS_WEIGHTS),
                new TypeReference<List<Double>>() {
                });

        HashSet<String> typePredicatesOverride = objectMapper.convertValue(rootNode.get(TYPE_PREDICATES_OVERRIDE_CONF),
                new TypeReference<HashSet<String>>() {
                });

        double typePredicatesOverrideFieldWeight = rootNode.get(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_CONF).asDouble();

        // Ensure that all weights are in descending order
        fieldWeights.sort(Collections.reverseOrder());
        datatypePropertiesFieldWeights.sort(Collections.reverseOrder());
        objectPropertiesFieldWeights.sort(Collections.reverseOrder());
        relationsFieldWeights.sort(Collections.reverseOrder());

        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < kMeansClusters; i++) {
            if (divideDataTypeAndObjectProperties()) {
                fields.add(new Field(fieldName + i + DATATYPE_PROPERTIES_SUFFIX,
                        new HashSet<>(),
                        datatypePropertiesFieldWeights.get(i),
                        false,
                        false));
                fields.add(new Field(fieldName + i + OBJECT_PROPERTIES_SUFFIX,
                        new HashSet<>(),
                        objectPropertiesFieldWeights.get(i),
                        true,
                        false));
            } else {
                fields.add(new Field(fieldName + i,
                        new HashSet<>(),
                        fieldWeights.get(i),
                        false,
                        false));
            }

            if (createRelationsFields()) {
                fields.add(new Field(RELATIONS_FIELD_NAME + i,
                        new HashSet<>(),
                        relationsFieldWeights.get(i),
                        true,
                        true));
            }
        }

        if (!typePredicatesOverride.isEmpty())
            fields.add(new Field(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_NAME,
                    new HashSet<>(),
                    typePredicatesOverrideFieldWeight,
                    true,
                    false));

        return new VirtualDocumentTemplate(fields, false);
    }

    /**
     * Create a template using only this metrics aggregator
     *
     * @param endpoint Endpoint containing the metrics to be used
     * @return
     * @throws IOException          If there is any IO error happens when loading a SPARQL query, if used
     * @throws ConvergenceException If the KMeans algorithm doesn't converge (empty clusters...)
     * @throws ExecutionException   If threaded execution is interrupted while querying object and data properties
     * @throws InterruptedException If threaded execution is interrupted while querying object and data properties
     */
    public VirtualDocumentTemplate createVirtualDocumentTemplate(SPARQLEndpoint endpoint) throws IOException,
            ConvergenceException, ExecutionException, InterruptedException {
        VirtualDocumentTemplate result;

        if (this.divideDataTypeAndObjectProperties) {
            // If a property somehow acts both as an object AND datatype property, there's a chance that it has spurious
            // metrics (in InfoRank, it will have two separate IW metrics, in the Importance metrics there will be no
            // issues).
            //
            // If that happens, it will be considered a datatype property and get assigned a random metric

            var objectProperties = getObjectProperties(endpoint);
            var datatypeProperties = getDataTypeProperties(endpoint);

            var allowedPredicatesBackup = new HashSet<>(allowedPredicates);

            // Object properties template
            if (!allowedPredicates.isEmpty())
                this.allowedPredicates.retainAll(objectProperties); // Intersection of allowed predicates which are object properties
            else
                this.allowedPredicates = objectProperties;

            VirtualDocumentTemplate vdocForObjectProperties =
                    getVirtualDocumentTemplate(runKMeans(filterClusterInput(getOptionalClusterInput(endpoint, true).clusterInput), true));

            // Datatype properties template
            this.allowedPredicates = new HashSet<>(allowedPredicatesBackup);

            if (!allowedPredicates.isEmpty())
                this.allowedPredicates.retainAll(datatypeProperties); // Intersection of allowed predicates which are datatype properties
            else
                this.allowedPredicates = datatypeProperties;

            VirtualDocumentTemplate vdocForDatatypeProperties =
                    getVirtualDocumentTemplate(runKMeans(filterClusterInput(getOptionalClusterInput(endpoint, true).clusterInput), true));


            this.allowedPredicates = allowedPredicatesBackup; // Restore the original allowed predicates

            result = joinPropertyTypeVdocs(vdocForObjectProperties, vdocForDatatypeProperties);
        } else {
            OptionalClusterInputOrTemplate opt = getOptionalClusterInput(endpoint, false);
            if (opt.isClusterInput())
                result = getVirtualDocumentTemplate(runKMeans(filterClusterInput(opt.clusterInput), true));
            else
                result = opt.template;
        }

        // Add the relations fields on top of the final vdoc
        if (createRelationsFields) {
            var objectProperties = getObjectProperties(endpoint);

            var allowedPredicatesBackup = new HashSet<>(allowedPredicates);

            // Object properties template
            if (!allowedPredicates.isEmpty())
                this.allowedPredicates.retainAll(objectProperties); // Intersection of allowed predicates which are object properties
            else
                this.allowedPredicates = objectProperties;

            VirtualDocumentTemplate vdocForObjectProperties =
                    getVirtualDocumentTemplate(runKMeans(filterClusterInput(getOptionalClusterInput(endpoint, true).clusterInput), false));

            this.allowedPredicates = allowedPredicatesBackup; // Restore the original allowed predicates

            int i = 0;
            for (var f : vdocForObjectProperties.fields) {
                f.name = RELATIONS_FIELD_NAME + i;
                f.isForEntityLinking = true;
                f.weight = relationsFieldWeights.get(i);

                result.fields.add(f);
                i++;
            }
        }

        return result;
    }

    /**
     * Create a template combining its own results with those from another metrics aggregator, with the
     * resulting weight of a predicate, used during k-means, being given by the following formula:
     * <p>
     * weight(p) = weight(p, this_engine)^ownWeight + weight(p, second_engine)^(1 - ownWeight)
     * <p>
     * The weight should be in the range [0.0, 1.0]
     *
     * @param endpoint Endpoint containing the metrics to be used
     * @return
     * @throws IOException          If there is any IO error happens when loading a SPARQL query, if used
     * @throws ConvergenceException If the KMeans algorithm doesn't converge (empty clusters...)
     * @throws ExecutionException   If threaded execution is interrupted while querying object and data properties
     * @throws InterruptedException If threaded execution is interrupted while querying object and data properties
     */
    public VirtualDocumentTemplate createVirtualDocumentTemplate(SPARQLEndpoint endpoint,
                                                                 MetricsAggregator engine,
                                                                 double ownWeight) throws IOException,
            ConvergenceException, ExecutionException, InterruptedException {
        if (ownWeight < 0.0 || ownWeight > 1.0) {
            throw new RuntimeException("Invalid weight. It should be in the range [0.0, 1.0]");
        }

        if (this.divideDataTypeAndObjectProperties) {
            // If a property somehow acts both as an object AND datatype property, there's a chance that it has spurious
            // metrics (in InfoRank, it will have two separate IW metrics, in the Importance metrics there will be no
            // issues).
            //
            // In that happens, it will get assigned a random metric in the inforank engines, and it will get associated
            // to both fields. During indexing, it will randomly get indexed to one of those two fields (NOTE: this is a
            // different behavior than when only using one engine! In that case, it's guaranteed to land in the datatype
            // properties field, here it will land on both)

            this.allowedPredicates = getObjectProperties(endpoint);
            engine.allowedPredicates = this.allowedPredicates;
            VirtualDocumentTemplate vdocForObjectProperties = getCombinedEnginesVdoc(endpoint, engine, ownWeight);


            this.allowedPredicates = getDataTypeProperties(endpoint);
            engine.allowedPredicates = this.allowedPredicates;
            VirtualDocumentTemplate vdocForDatatypeProperties = getCombinedEnginesVdoc(endpoint, engine, ownWeight);


            return joinPropertyTypeVdocs(vdocForObjectProperties, vdocForDatatypeProperties);
        } else {
            return getCombinedEnginesVdoc(endpoint, engine, ownWeight);
        }
    }

    // Return a VirtualDocumentTemplate combining the results of two MetricsAggregator instances
    private VirtualDocumentTemplate getCombinedEnginesVdoc(SPARQLEndpoint endpoint,
                                                           MetricsAggregator engine,
                                                           double ownWeight) throws IOException, ExecutionException, InterruptedException {
        List<ResourceWrapper> ownClusterInput;
        List<ResourceWrapper> clusterInputSecondEngine;
        List<ResourceWrapper> newClusterInput;

        ownClusterInput = filterClusterInput(getOptionalClusterInput(endpoint, true).clusterInput);
        clusterInputSecondEngine = filterClusterInput(engine.getOptionalClusterInput(endpoint, true).clusterInput);

        newClusterInput = new ArrayList<>(ownClusterInput.size());

        for (var entry : ownClusterInput) {
            for (var secondInputEntry : clusterInputSecondEngine) {
                if (entry.resource.getURI().equals(secondInputEntry.resource.getURI())) {
                    newClusterInput.add(
                            new ResourceWrapper(
                                    entry.resource,
                                    joinPoints(
                                            entry.points,
                                            secondInputEntry.points,
                                            ownWeight)));
                }
            }
        }

        return getVirtualDocumentTemplate(runKMeans(newClusterInput, true));
    }

    // Create a VirtualDocumentTemplate with its fields duplicated, separating datatype properties from object properties
    private static VirtualDocumentTemplate joinPropertyTypeVdocs(VirtualDocumentTemplate vdocForObjectProperties,
                                                                 VirtualDocumentTemplate vdocForDatatypeProperties) throws IOException {
        VirtualDocumentTemplate result = getEmptyVirtualDocumentTemplate();

        for (Field f : result.fields) {
            if (f.isForObjectProperties) {
                // Search for the corresponding field in the objectProperties vdoc
                for (Field objectPropertiesField : vdocForObjectProperties.fields) {
                    if (f.name.replaceAll(OBJECT_PROPERTIES_SUFFIX, "").equals(objectPropertiesField.name)) {
                        f.predicates = objectPropertiesField.predicates;
                        break;
                    }
                }
            } else {
                // Search for the corresponding field in the datatypeProperties vdoc
                for (Field dataTypePropertiesField : vdocForDatatypeProperties.fields) {
                    if (f.name.replaceAll(DATATYPE_PROPERTIES_SUFFIX, "").equals(dataTypePropertiesField.name)) {
                        f.predicates = dataTypePropertiesField.predicates;
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Join the points (scores) from two different MetricsAggregator instances, weighing each one separately
     * with points1Weight
     */
    private double[] joinPoints(double[] points1, double[] points2, double points1Weight) {
        if (points1.length != points2.length) {
            throw new RuntimeException("The two engine's weight dimensions differ and cannot be combined");
        }

        double[] joinedPoints = new double[points1.length];
        for (int i = 0; i < points1.length; i++) {
            //joinedPoints[i] = (points1Weight * points1[i]) + ((1 - points1Weight) * points2[i]);
            // Weighted geometric mean
            joinedPoints[i] = (Math.pow(points1[i], points1Weight) * Math.pow(points2[i], (1 - points1Weight)));
        }

        return joinedPoints;
    }

    // Runs KMeans for the given input. reclusteringAllowed is used to forbid reclustering on intermediate clustering
    // steps
    protected List<CentroidCluster<ResourceWrapper>> runKMeans(List<ResourceWrapper> clusterInput, boolean reclusteringAllowed) throws ConvergenceException {
        MultiKMeansPlusPlusClusterer<ResourceWrapper> clusterer =
                new MultiKMeansPlusPlusClusterer<>(
                        new KMeansPlusPlusClusterer<>(kMeansClusters, kMeansIterations),
                        K_MEANS_PLUS_PLUS_ATTEMPTS);
        List<CentroidCluster<ResourceWrapper>> unfilteredResults = clusterer.cluster(clusterInput);

        if (this.reclusterize && reclusteringAllowed) { // Remove last cluster and run KMeans again
            // For some reason, using reversed() in a lambda doesn't work...
            unfilteredResults.sort((cluster1, cluster2) -> {
                double center1 = cluster1.getCenter().getPoint()[0];
                double center2 = cluster2.getCenter().getPoint()[0];
                return Double.compare(center2, center1);
            });

            if (unfilteredResults.size() > 1) {
                //System.out.println("Removed "+ unfilteredResults.get(unfilteredResults.size() -1).getPoints().size());
                //System.out.println("Removed "+ unfilteredResults.get(unfilteredResults.size() -2).getPoints().size());
                unfilteredResults.remove(unfilteredResults.size() - 1);

                List<ResourceWrapper> filteredClusterInput = new ArrayList<>();

                for (var cluster : unfilteredResults) {
                    filteredClusterInput.addAll(cluster.getPoints());
                }

                filteredClusterInput.sort((points1, points2) -> {
                    double p1 = points1.getPoint()[0];
                    double p2 = points2.getPoint()[0];
                    return Double.compare(p2, p1);
                });

                return clusterer.cluster(clusterInput);
            }
        }

        return unfilteredResults;
    }

    /**
     * Run KMeans based on a list of scores for each predicate. Note: This is only exposed for the IndexingPipeline,
     * metrics aggregators should be used in any other case
     *
     * @param clusterInput List of ResourceWrappers containing every predicate we want to cluster
     * @return The KMeansPlusPlusClusterer results
     * @throws ConvergenceException If KMeans fails to converge
     * @throws IOException          If any IO error occurs when reading the configuration file
     */
    public static List<CentroidCluster<ResourceWrapper>> runKMeansWithoutAggregator(List<ResourceWrapper> clusterInput) throws ConvergenceException, IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        int kMeansClusters = rootNode.get(CLUSTERS_CONF).asInt();
        int kMeansIterations = rootNode.get(K_MEANS_ITERATIONS_CONF).asInt();

        MultiKMeansPlusPlusClusterer<ResourceWrapper> clusterer =
                new MultiKMeansPlusPlusClusterer<>(
                        new KMeansPlusPlusClusterer<>(kMeansClusters, kMeansIterations),
                        K_MEANS_PLUS_PLUS_ATTEMPTS);
        return clusterer.cluster(clusterInput);
    }

    /**
     * Create a VirtualDocumentTemplate based on clustering results. All intermediate getVirtualDocumentTemplate methods
     * should call this one. Note: This is only exposed for the IndexingPipeline, metrics aggregators should be used in
     * any other case
     *
     * @param clusterResults The KMeansPlusPlusClusterer results
     */
    public VirtualDocumentTemplate getVirtualDocumentTemplate(List<CentroidCluster<ResourceWrapper>> clusterResults) {
        List<Field> fields = new ArrayList<>();

        HashMap<Double, Set<Field.FieldElement>> fieldsMap = new HashMap<>();
        for (CentroidCluster<ResourceWrapper> clusterResult : clusterResults) {
            Set<Field.FieldElement> predicates = new HashSet<>();

            for (ResourceWrapper resourceWrapper : clusterResult.getPoints()) {
                //System.out.println(resourceWrapper.getResource() + " (imp: "+resourceWrapper.points[0]+")");
                predicates.add(new Field.FieldElement(resourceWrapper.getResource().getURI(), 1));
            }

            double weight = clusterResult.getCenter().getPoint()[0];

            if (fieldsMap.containsKey(weight))
                // This happends if there are multiple empty clusters with a score of 0.0
                // In this case, we simply add a new field with a tiny weight
                weight = new Random().nextDouble(0.0, 0.01);

            fieldsMap.put(weight, predicates);
        }

        // Sort the map's keys, which correspond to each centroid's average score, in descending order
        List<Double> orderedWeights = new ArrayList<>(fieldsMap.keySet());
        orderedWeights.sort(Collections.reverseOrder());

        // Add all overridden predicates to the best cluster
        fieldsMap.get(orderedWeights.get(0)).addAll(
                predicatesOverride.stream()
                        .filter(p -> allowedPredicates.contains(p)) // Excluding those non-allowed
                        .toList()
                        .stream()
                        .map(p -> new Field.FieldElement(p, 1))
                        .toList()
        );

        int i = 0;
        for (Double key : orderedWeights) {
            // Each field will be ordered the same way across regardless of the metrics aggregator being used,
            // with the lowest indexed fields having the biggest weights, which are dictated by the configuration
            // and not the centroid scores (they only affect the ordering)
            fields.add(new Field(fieldName + i, fieldsMap.get(key), fieldWeights.get(i), false, false));
            i++;
        }

        // Add the types to a separate field if the override is non-empty
        if (!typePredicatesOverride.isEmpty())
            fields.add(new Field(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_NAME,
                    typePredicatesOverride.stream().map(p -> new Field.FieldElement(p, 1)).collect(Collectors.toSet()),
                    typePredicatesOverrideFieldWeight,
                    true,
                    false));

        return new VirtualDocumentTemplate(fields, false);
    }


    /**
     * Returns a new clusterInput list without any overridden or non-allowed predicates in it
     * <p>
     * This is done automatically on all createVirtualDocumentTemplate methods, and is only run locally
     * on PredicateClassBasedAggregator
     */
    protected List<ResourceWrapper> filterClusterInput(List<ResourceWrapper> clusterInput) {
        List<ResourceWrapper> filteredList = new ArrayList<>();

        for (ResourceWrapper rw : clusterInput) {
            if ((allowedPredicates.isEmpty() || allowedPredicates.contains(rw.resource.getURI()))
                    && !predicatesOverride.contains(rw.resource.getURI())
                    && !typePredicatesOverride.contains(rw.resource.getURI())) {
                // Only used for subsuming predicates
                //&& !subsumedPredicates.contains(rw.resource.getURI())) {
                filteredList.add(rw);
            }
        }

        return filteredList;
    }

    /**
     * Useful for cloning EntityBasedMetricsAggregator instances, in order to make a copy of
     * said engine for a different entity
     */
    @Override
    public MetricsAggregator clone() {
        try {
            return (MetricsAggregator) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    // Return a set containing all predicates which are associated with at least one literal object
    public static Set<String> getDataTypeProperties(SPARQLEndpoint endpoint) throws ExecutionException,
            InterruptedException, IOException {
        if (endpoint instanceof LocalHDTSPARQLEndpoint) {
            return getDataTypePropertiesHDT((LocalHDTSPARQLEndpoint) endpoint);
        } else {
            return getDataTypePropertiesSPARQL(endpoint);
        }
    }

    private static Set<String> getDataTypePropertiesSPARQL(SPARQLEndpoint endpoint) throws IOException {
        HashSet<String> datatypeProperties = new HashSet<>();

        String objQuery = MetricsGenerator.getQueryString(GET_DATA_TYPE_PROPERTIES_QUERY);
        var results = endpoint.runSelectQuery(objQuery);
        while (results.hasNext()) {
            var res = results.next();
            datatypeProperties.add(res.get("p").asResource().getURI());
        }

        return datatypeProperties;
    }

    private static Set<String> getDataTypePropertiesHDT(LocalHDTSPARQLEndpoint endpoint) throws InterruptedException {
        Set<String> datatypeProperties = ConcurrentHashMap.newKeySet();

        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1);
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        for (Iterator<Long> it = predicatesStream.iterator(); it.hasNext(); ) {
            long predicateID = it.next();

            completionService.submit(() -> {
                IteratorTripleID objectsOfP = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));
                while (objectsOfP.hasNext()) {
                    TripleID next = objectsOfP.next();
                    if (endpoint.hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT).charAt(0) == '"') {
                        datatypeProperties.add(endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString());
                        break;
                    }
                }
                return null;
            });
        }
        executor.shutdown();

        if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS)) {
            throw new RuntimeException("Timeout when waiting for the indexing task to complete!");
        }
        executor.shutdownNow();

        return datatypeProperties;
    }

    // Return a set containing all predicates which NOT are associated with literal objects
    public static Set<String> getObjectProperties(SPARQLEndpoint endpoint) throws ExecutionException,
            InterruptedException, IOException {
        if (endpoint instanceof LocalHDTSPARQLEndpoint) {
            return getObjectPropertiesHDT((LocalHDTSPARQLEndpoint) endpoint);
        } else {
            return getObjectPropertiesSPARQL(endpoint);
        }
    }

    private static Set<String> getObjectPropertiesSPARQL(SPARQLEndpoint endpoint) throws IOException {
        HashSet<String> objectProperties = new HashSet<>();

        String objQuery = MetricsGenerator.getQueryString(GET_OBJECT_PROPERTIES_QUERY);
        var results = endpoint.runSelectQuery(objQuery);
        while (results.hasNext()) {
            var res = results.next();
            objectProperties.add(res.get("p").asResource().getURI());
        }

        return objectProperties;
    }

    private static Set<String> getObjectPropertiesHDT(LocalHDTSPARQLEndpoint endpoint) throws InterruptedException {
        Set<String> objectProperties = ConcurrentHashMap.newKeySet();

        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1);
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        for (Iterator<Long> it = predicatesStream.iterator(); it.hasNext(); ) {
            long predicateID = it.next();

            completionService.submit(() -> {
                boolean isObjectProperty = true;

                IteratorTripleID objectsOfP = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));
                while (objectsOfP.hasNext()) {
                    TripleID next = objectsOfP.next();
                    if (endpoint.hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT).charAt(0) == '"') {
                        isObjectProperty = false;
                        break;
                    }
                }

                if (isObjectProperty)
                    objectProperties.add(endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString());

                return null;
            });
        }
        executor.shutdown();

        if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS)) {
            throw new RuntimeException("Timeout when waiting for the indexing task to complete!");
        }
        executor.shutdownNow();

        return objectProperties;
    }

    protected static String getQueryString(String queryFile) throws IOException {
        Path factFrequencyQueryPath = Path.of(queryFile);
        return Files.readString(factFrequencyQueryPath);
    }

    /**
     * Method to be used by the importance metrics aggregators and by the indexingPipeline when calculating
     * type-based or entity-based templates, in order to know if the type should contribute to the score or not
     * Current configurable filters:
     * - Allowed types list (for entity-based aggregators, unused since switching to caching via HDT)
     * - Ignored types list (virtualType and any other type added by the metric generators)
     * - Type prefixes (via the configuration file, for excluding external types in the KG)
     *
     * @param type Jena's reference to the type
     * @return true if it is to be taken into account as a KMeans clustering input or part of it (it is not in the
     * ignored types list and its prefix is in the typePrefixes list if not empty)
     */
    public boolean isTypeAllowed(Resource type) {
        // allowedTypes has priority over typesToIgnore
        if (!allowedTypes.isEmpty())
            return allowedTypes.contains(type.getURI());

        if (typesToIgnore.contains(type.getURI()))
            return false;

        if (typePrefixes.isEmpty()) {
            return true;
        } else {
            for (var typeStringPrefix : typePrefixes) {
                if (type.getURI().contains(typeStringPrefix))
                    return true;
            }
        }

        return false;
    }

    public Set<String> getTypesToIgnore() {
        return typesToIgnore;
    }

    public void setTypesToIgnore(Set<String> typesToIgnore) {
        this.typesToIgnore = typesToIgnore;
    }

    public Set<String> getAllowedTypes() {
        return allowedTypes;
    }

    public void setAllowedTypes(Set<String> allowedTypes) {
        this.allowedTypes = allowedTypes;
    }
}

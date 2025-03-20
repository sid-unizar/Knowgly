package sid.Pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.exception.ConvergenceException;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.triples.TripleID;
import sid.Connectors.Elastic.ElasticConnector;
import sid.Connectors.Galago.GalagoConnector;
import sid.Connectors.IndexConnector;
import sid.Connectors.Lucene.LuceneConnector;
import sid.Connectors.Terrier.TerrierConnector;
import sid.EntityExtractor.ExtractedEntity;
import sid.EntityExtractor.HDT.EntityExtractorHDT;
import sid.EntityExtractor.SPARQL.SPARQLEntityExtractor;
import sid.MetricsAggregation.DummyMetricsAggregator;
import sid.MetricsAggregation.EntityBasedAggregator.EntityBasedMetricsAggregator;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.TypeBasedAggregator.TypeBasedMetricsAggregator;
import sid.MetricsAggregation.VirtualDocumentTemplate;
import sid.MetricsGeneration.MetricsGenerator;
import sid.MetricsGeneration.SPARQL.ImportanceMetricsGenerator;
import sid.MetricsGeneration.SPARQL.InfoRankMetricsGenerator;
import sid.SPARQLEndpoint.EmbeddedSPARQLServerEndpoint;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.RemoteSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpoint;
import sid.utils.Pair;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static sid.SPARQLEndpoint.SPARQLEndpoint.RDF_TYPE_URI;

/**
 * Pipeline class for indexing, which takes care of metrics aggregation and indexing according to its configuration,
 * handling internally any kind of conversion and class specialization instantiations
 * <p>
 * Needs to start execution explicitly with the run method
 * <p>
 * Note that, on the contrary to the underlying modules, the features supported by the pipeline depend on the
 * underlying SPARQLEndpoint. Currently, the RDF endpoint doesn't allow to:
 * <p>
 * - Use Type-based or Entity-based metrics aggregators, for efficiency reasons (this will result in a TypeBasedMetricsAggregatorInSPARQLException)
 */
public class IndexingPipeline {
    public enum TypeBasedTemplateCombinationMethod {
        Union,
        MostAppearances,
        MaximumValues,
        GeometricMean,
        Repetitions,
        Invalid
    }

    public static class TypeBasedMetricsAggregatorInSPARQLException extends Exception {
        public TypeBasedMetricsAggregatorInSPARQLException() {
            super("Indexing via SPARQL with type-based metrics aggregators is not supported");
        }
    }

    private static final String CONFIGURATION_FILE = "configuration/indexingPipelineConfiguration.json";
    // Configuration keys
    private static final String SOURCE_CONF = "source";
    private static final String DESTINATION_CONF = "destination";
    private static final String MUST_HAVE_PREDICATES_CONF = "mustHavePredicates";
    private static final String TYPE_BASED_COMBINATION_METHOD_CONF = "typeBasedCombinationMethod";

    // Options
    private static final String HDT_OPTION = "HDT";
    private static final String ELASTIC_OPTION = "elastic";
    private static final String TERRIER_OPTION = "terrier";
    private static final String GALAGO_OPTION = "galago";
    private static final String LUCENE_OPTION = "lucene";
    private static final String EMBEDDED_SPARQL_ENDPOINT_OPTION = "EmbeddedSPARQLServerEndpoint";
    private static final String REMOTE_SPARQL_ENDPOINT_OPTION = "RemoteSPARQLEndpoint";

    private static final String TYPE_BASED_COMBINATION_METHOD_MOST_APPEARANCES_OPTION = "mostAppearances";
    private static final String TYPE_BASED_COMBINATION_METHOD_UNION_OPTION = "union";
    private static final String TYPE_BASED_COMBINATION_METHOD_MAXIMUM_VALUES_OPTION = "maximumValues";
    private static final String TYPE_BASED_COMBINATION_METHOD_GMEAN_OPTION = "geometricMean";
    private static final String TYPE_BASED_COMBINATION_METHOD_REPETITIONS_OPTION = "repetitions";

    // Maximum amount of time to wait for indexing to finish (on DBPedia-entity using HDT, it takes ~5 hours)
    private static final long INDEXING_TIMEOUT = Long.MAX_VALUE;


    private final SPARQLEndpoint endpoint;
    private final IndexConnector indexConnector;
    private final MetricsAggregator aggregator1;
    private final MetricsAggregator aggregator2;
    private final double weight;
    private final List<String> mustHavePredicateURIs;
    private final TypeBasedTemplateCombinationMethod method;
    private VirtualDocumentTemplate fallback = null;

    // Non-null if we are doing type-based indexing for a single type ("index slices")
    private String indexOnlyTypeURI = null;
    private TypeBasedMetricsAggregator indexOnlyTypeURIAggregator = null;

    public static List<String> getMustHavePredicateURIs() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return objectMapper.convertValue(rootNode.get(MUST_HAVE_PREDICATES_CONF), new TypeReference<List<String>>() {
        });
    }

    /**
     * IndexingPipeline with one MetricsAggregator.
     *
     * @param fallback Fallback template in case of clustering issues, only for type-based indexing (e.g. cases where
     *                 there are less predicates associated to a type t than there are fields)
     */
    public static IndexingPipeline fromConfigurationFile(MetricsAggregator engine, VirtualDocumentTemplate fallback) throws IOException,
            NotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, URISyntaxException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        String source = rootNode.get(SOURCE_CONF).asText();
        SPARQLEndpoint endpoint = switch (source) {
            case HDT_OPTION -> LocalHDTSPARQLEndpoint.fromConfigurationFile();
            case EMBEDDED_SPARQL_ENDPOINT_OPTION -> EmbeddedSPARQLServerEndpoint.fromConfigurationFile();
            case REMOTE_SPARQL_ENDPOINT_OPTION -> RemoteSPARQLEndpoint.fromConfigurationFile();
            default -> throw new RuntimeException("Unknown option for source: " + source);
        };

        String destination = rootNode.get(DESTINATION_CONF).asText();
        IndexConnector indexer = switch (destination) {
            case ELASTIC_OPTION -> ElasticConnector.fromConfigurationFile();
            case TERRIER_OPTION -> TerrierConnector.fromConfigurationFile();
            case GALAGO_OPTION -> GalagoConnector.fromConfigurationFile();
            case LUCENE_OPTION -> LuceneConnector.fromConfigurationFile();
            default -> throw new RuntimeException("Unknown option for destination: " + destination);
        };

        String methodOption = rootNode.get(TYPE_BASED_COMBINATION_METHOD_CONF).asText();

        TypeBasedTemplateCombinationMethod method = switch (methodOption) {
            case TYPE_BASED_COMBINATION_METHOD_MOST_APPEARANCES_OPTION ->
                    TypeBasedTemplateCombinationMethod.MostAppearances;
            case TYPE_BASED_COMBINATION_METHOD_UNION_OPTION -> TypeBasedTemplateCombinationMethod.Union;
            case TYPE_BASED_COMBINATION_METHOD_MAXIMUM_VALUES_OPTION ->
                    TypeBasedTemplateCombinationMethod.MaximumValues;
            case TYPE_BASED_COMBINATION_METHOD_GMEAN_OPTION -> TypeBasedTemplateCombinationMethod.GeometricMean;
            case TYPE_BASED_COMBINATION_METHOD_REPETITIONS_OPTION -> TypeBasedTemplateCombinationMethod.Repetitions;
            default -> throw new RuntimeException("Unknown option for typeBasedCombinationMethod: " + methodOption);
        };

        return new IndexingPipeline(endpoint,
                indexer,
                engine,
                objectMapper.convertValue(rootNode.get(MUST_HAVE_PREDICATES_CONF), new TypeReference<List<String>>() {
                }),
                method,
                fallback);
    }

    /**
     * IndexingPipeline with two metrics aggregators.
     * Warning: Combining type-based engines is not supported. If the first engine is type-based,
     * the second engine will be ignored. Subsequently, if the second engine is type-based, it will be ignored.
     */
    public static IndexingPipeline fromConfigurationFile(MetricsAggregator engine1,
                                                         MetricsAggregator engine2,
                                                         double weight) throws IOException,
            NotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, URISyntaxException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        String source = rootNode.get(SOURCE_CONF).asText();
        SPARQLEndpoint endpoint = switch (source) {
            case HDT_OPTION -> LocalHDTSPARQLEndpoint.fromConfigurationFile();
            case EMBEDDED_SPARQL_ENDPOINT_OPTION -> EmbeddedSPARQLServerEndpoint.fromConfigurationFile();
            case REMOTE_SPARQL_ENDPOINT_OPTION -> RemoteSPARQLEndpoint.fromConfigurationFile();
            default -> throw new RuntimeException("Unknown option for source: " + source);
        };

        String destination = rootNode.get(DESTINATION_CONF).asText();
        IndexConnector indexer = switch (destination) {
            case ELASTIC_OPTION -> ElasticConnector.fromConfigurationFile();
            case TERRIER_OPTION -> TerrierConnector.fromConfigurationFile();
            case GALAGO_OPTION -> GalagoConnector.fromConfigurationFile();
            case LUCENE_OPTION -> LuceneConnector.fromConfigurationFile();
            default -> throw new RuntimeException("Unknown option for destination: " + destination);
        };

        return new IndexingPipeline(endpoint,
                indexer,
                engine1,
                engine2,
                weight,
                objectMapper.convertValue(rootNode.get(MUST_HAVE_PREDICATES_CONF), new TypeReference<List<String>>() {
                }));
    }

    /**
     * IndexingPipeline with two metrics aggregators.
     * Warning: Combining type-based engines is not supported. If the first engine is type-based,
     * the second engine will be ignored. Subsequently, if the second engine is type-based, it will be ignored.
     *
     * @param mustHavePredicateURIs List of predicates an entity needs to have in order to be indexed. This can be filled
     *                              automatically from the configurationFile methods. If empty, all entities will be
     *                              indexed
     */
    public IndexingPipeline(SPARQLEndpoint endpoint,
                            IndexConnector indexConnector,
                            MetricsAggregator aggregator1,
                            MetricsAggregator aggregator2,
                            double weight,
                            List<String> mustHavePredicateURIs) {
        if (aggregator1 instanceof TypeBasedMetricsAggregator)
            System.err.println("Warning: combining type-based engines is not supported. The second engine will be ignored");

        this.endpoint = endpoint;
        this.indexConnector = indexConnector;
        this.aggregator1 = aggregator1;
        this.aggregator2 = aggregator2;
        this.weight = weight;
        this.mustHavePredicateURIs = mustHavePredicateURIs;
        this.method = TypeBasedTemplateCombinationMethod.Invalid;
    }

    /**
     * IndexingPipeline with one metrics aggregator.
     * Warning: Combining type-based engines is not supported. If the first engine is type-based,
     * the second engine will be ignored. Subsequently, if the second engine is type-based, it will be ignored.
     *
     * @param mustHavePredicateURIs List of predicates an entity needs to have in order to be indexed. This can be filled
     *                              automatically from the configurationFile methods. If empty, all entities will be
     *                              indexed
     */
    public IndexingPipeline(SPARQLEndpoint endpoint,
                            IndexConnector indexConnector,
                            MetricsAggregator aggregator1,
                            List<String> mustHavePredicateURIs,
                            TypeBasedTemplateCombinationMethod method,
                            VirtualDocumentTemplate fallback) {
        this.endpoint = endpoint;
        this.indexConnector = indexConnector;
        this.aggregator1 = aggregator1;
        this.aggregator2 = null;
        this.weight = 0.0;
        this.mustHavePredicateURIs = mustHavePredicateURIs;
        this.method = method;
        this.fallback = fallback;
    }

    public void indexOnlyTypeURI(String typeURI, TypeBasedMetricsAggregator indexOnlyTypeURIAggregator) {
        this.indexOnlyTypeURI = typeURI;
        this.indexOnlyTypeURIAggregator = indexOnlyTypeURIAggregator;
    }

    /**
     * Run the pipeline
     *
     * @throws IOException                                 If there is any IO error happens when loading a SPARQL query, if used
     * @throws ExecutionException                          If threaded execution is interrupted while querying object and data properties
     * @throws InterruptedException                        If threaded execution is interrupted while querying object and data properties
     * @throws TypeBasedMetricsAggregatorInSPARQLException If indexing via SPARQL is attempted with a type-based metrics aggregator (not supported due to efficiency reasons)
     */
    public void run() throws IOException, ExecutionException, InterruptedException, TypeBasedMetricsAggregatorInSPARQLException {
        // If the first engine is not type-based, we can use the same template for every entity
        boolean globalVdocs = !(aggregator1 instanceof TypeBasedMetricsAggregator || aggregator1 instanceof EntityBasedMetricsAggregator);

        VirtualDocumentTemplate template;
        if (globalVdocs) {
            if (aggregator2 == null) {
                template = aggregator1.createVirtualDocumentTemplate(endpoint);
            } else {
                template = aggregator1.createVirtualDocumentTemplate(endpoint, aggregator2, weight);
            }
        } else {
            template = null;
        }

        System.out.println("Retrieving all entity URIs...");

        if (endpoint instanceof LocalHDTSPARQLEndpoint)
            indexEntitiesHDT(EntityExtractorHDT.fromConfigurationFile((LocalHDTSPARQLEndpoint) endpoint),
                    globalVdocs,
                    template);
        else {
            if (!globalVdocs)
                throw new TypeBasedMetricsAggregatorInSPARQLException();

            indexEntitiesSPARQL(SPARQLEntityExtractor.fromConfigurationFile(endpoint), template);
        }


        endpoint.close();
    }

    /**
     * HDT specialization for indexing, which avoids caching all entity URIs beforehand, and excludes any URI which may
     * be part of the metrics auxiliary triples. It also uses an HDTEntityExtractor
     */
    private void indexEntitiesHDT(EntityExtractorHDT entityExtractor,
                                  boolean globalVdocs,
                                  VirtualDocumentTemplate template) throws IOException, InterruptedException, ExecutionException {
        LocalHDTSPARQLEndpoint endpointHDT = (LocalHDTSPARQLEndpoint) endpoint;

        Map<Long, VirtualDocumentTemplate> typeBasedTemplates;
        if (!globalVdocs) {
            if (aggregator1 instanceof TypeBasedMetricsAggregator) { // Type-based aggregator, cache all type-based templates now
                typeBasedTemplates = getTypeBasedTemplates(endpointHDT, (TypeBasedMetricsAggregator) aggregator1);
            } else { // Entity-based aggregator, cache all metrics now
                typeBasedTemplates = null;
                ((EntityBasedMetricsAggregator) aggregator1).cacheAllMetrics(endpointHDT, fallback);
            }
        } else {
            typeBasedTemplates = null;
        }

        long typePredicateID = endpointHDT.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.RDF_TYPE_URI, TripleComponentRole.PREDICATE);

        System.out.println("Retrieving all entity IDs...");
        Pair<Stream<Long>, Long> subjectIDsToIndex = getSubjectsWithMustHavePredicates(endpointHDT, mustHavePredicateURIs);

        if (indexOnlyTypeURI != null) {
            // Override the template with the type-based one
            indexOnlyTypeURIAggregator.typeURI = indexOnlyTypeURI;
            template = indexOnlyTypeURIAggregator.createVirtualDocumentTemplate(endpointHDT);
        }

        AtomicInteger count = new AtomicInteger(1);

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);


        // Submit a task for each entity, since a parallel stream would split its contents into even
        // chunks, leaving some threads doing nothing while a few of them do all the hard work for
        // big entities (the indexing task time heavily depends on each entity: the more relations
        // and properties, the worse)
        for (Iterator<Long> it = subjectIDsToIndex.getKey().iterator(); it.hasNext(); ) {
            long entityID = it.next();

            VirtualDocumentTemplate finalTemplate = template;
            completionService.submit(() -> {
                try {
                    String entityURI = endpointHDT.hdt.getDictionary().idToString(entityID, TripleComponentRole.SUBJECT).toString();

                    // Exclude auxiliary metrics entities
                    if (!(entityURI.contains(MetricsGenerator.BASE_IMPORTANCE_SUBGRAPH_URI) || entityURI.contains(MetricsGenerator.BASE_INFORANK_SUBGRAPH_URI))) {
                        VirtualDocumentTemplate templateForEntity;
                        if (!globalVdocs) {
                            if (aggregator1 instanceof TypeBasedMetricsAggregator)
                                templateForEntity = generateTypeBasedTemplateForEntity(entityID,
                                        typePredicateID,
                                        endpointHDT,
                                        typeBasedTemplates,
                                        method,
                                        fallback);
                            else { // Entity-based aggregator
                                EntityBasedMetricsAggregator clonedAggregator = (EntityBasedMetricsAggregator) aggregator1.clone();
                                clonedAggregator.setEntityURI(entityURI);

                                // The entity-based aggregator will internally decide whether to return an entity-based
                                // template or a fallback global template (if the entity doesn't have any allowed type)
                                templateForEntity = clonedAggregator.createVirtualDocumentTemplate(endpointHDT);
                            }

                        } else {
                            templateForEntity = finalTemplate;
                        }

                        ExtractedEntity extractedEntity = entityExtractor.extractEntity(entityURI);

                        // Create and index a vdoc for the entity, using the inferred vdoc template and its extracted entity
                        indexConnector.addDocumentToIndex(indexConnector.createEntityDocument(
                                extractedEntity,
                                templateForEntity,
                                // Use the extractor's URI renaming strategy, which was defined in its config file
                                entityExtractor.config.uriRenamingStrategy));
                    }

                    System.out.print("Indexed entity " + count.getAndIncrement() + " of " + subjectIDsToIndex.getValue() + '\r');
                } catch (ConvergenceException convExcp) {
                    try { // Force indexing with the fallback template, as the entity-based or type-based template couldn't converge
                        if (!globalVdocs) {
                            String entityURI = endpointHDT.hdt.getDictionary().idToString(entityID, TripleComponentRole.SUBJECT).toString();
                            ExtractedEntity extractedEntity = entityExtractor.extractEntity(entityURI);

                            indexConnector.addDocumentToIndex(indexConnector.createEntityDocument(
                                    extractedEntity,
                                    fallback,
                                    entityExtractor.config.uriRenamingStrategy));
                        }
                    } catch (Exception e) { // Give up: print an error message and count it as indexed
                        e.printStackTrace();
                        System.out.print("Indexed entity " + count.getAndIncrement() + " of " + subjectIDsToIndex.getValue() + '\r');
                        return null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    // Count it as indexed anyway!
                    // Reasons it can fail:
                    //      Its URI is too long (>512 characters, which is the maximum for a docID in elastic)
                    System.out.print("Indexed entity " + count.getAndIncrement() + " of " + subjectIDsToIndex.getValue() + '\r');
                    return null;
                }

                return null;
            });
        }

        waitAndFinishIndexing(executor);

        System.out.println("Finished indexing! Entities indexed: " + count.get() + "/" + endpointHDT.hdt.getDictionary().getNsubjects());
    }


    /**
     * Returns a Pair containing a stream of subject IDs which contain all must-have predicates, along its size. If there
     * were no predicates, it will return a stream of all possible subject IDs
     * <p>
     * Note:  Made static in order to be able to use it from MetricsBulkTesting
     */
    public Pair<Stream<Long>, Long> getSubjectsWithMustHavePredicates(LocalHDTSPARQLEndpoint endpoint,
                                                                      List<String> mustHavePredicateURIs) {
        Deque<Set<Long>> sets = new ArrayDeque<>();

        for (String mustHavePredicateURI : mustHavePredicateURIs) {
            Set<Long> subjectIDs = new HashSet<>();
            long mustHavePredicateID = endpoint.hdt.getDictionary().stringToId(mustHavePredicateURI, TripleComponentRole.PREDICATE);

            if (mustHavePredicateID != -1) {
                var pQueryResults = endpoint.hdt.getTriples().search(new TripleID(0, mustHavePredicateID, 0));
                while (pQueryResults.hasNext())
                    subjectIDs.add(pQueryResults.next().getSubject());
            }

            sets.add(subjectIDs);
        }

        if (this.indexOnlyTypeURI != null) {
            long typeID = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);
            long personID = endpoint.hdt.getDictionary().stringToId(this.indexOnlyTypeURI, TripleComponentRole.OBJECT);

            // Look for all entites of type indexOnlyTypeURI
            Set<Long> subjectIDs = new HashSet<>();
            var typeQ = endpoint.hdt.getTriples().search(new TripleID(0, typeID, personID));
            while (typeQ.hasNext())
                subjectIDs.add(typeQ.next().getSubject());
            sets.add(subjectIDs); // An easy way to do it, we will end up with only entities containing this type
        }

        if (!sets.isEmpty()) {
            // Return an intersection of all sets
            Set<Long> result = sets.pop();
            while (!sets.isEmpty()) {
                result.retainAll(sets.pop());
            }

            return new Pair<>(result.stream(), (long) result.size());
        } else { // There were no must-have predicates, it shouldn't have been called
            return new Pair<>(LongStream.range(1, endpoint.hdt.getDictionary().getNsubjects() + 1).boxed(), endpoint.hdt.getDictionary().getNsubjects());
        }
    }

    private void indexEntitiesSPARQL(SPARQLEntityExtractor entityExtractor,
                                     VirtualDocumentTemplate template) throws IOException, InterruptedException {
        List<Resource> entities = entityExtractor.getAllEntityURIs();
        AtomicInteger count = new AtomicInteger(1);

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        // Submit a task for each entity, since the entities list's parallelStream() will split its contents into even
        // chunks, leaving some threads doing nothing while a few of them do all the hard work for big entities (the
        // indexing task time heavily depends on each entity: the more relations and properties, the worse)
        for (Resource entity : entities) {
            completionService.submit(() -> {
                        try {
                            ExtractedEntity extractedEntity = entityExtractor.extractEntity(entity);

                            // Create and index a vdoc for the entity, using the inferred vdoc template and its extracted entity
                            indexConnector.addDocumentToIndex(indexConnector.createEntityDocument(
                                    extractedEntity,
                                    template,
                                    // Use the extractor's URI renaming strategy, which was defined in its config file
                                    entityExtractor.config.uriRenamingStrategy));

                            System.out.print("Indexed entity " + count.getAndIncrement() + " of " + entities.size() + '\r');
                        } catch (Exception e) {
                            // Count it as indexed anyway!
                            // Reasons it can fail:
                            //      Its URI is too long (>512 characters, which is the maximum for a docID in elastic)
                            System.out.print("Indexed entity " + count.getAndIncrement() + " of " + entities.size() + '\r');
                            return null;
                        }

                        return null;
                    }
            );
        }

        waitAndFinishIndexing(executor);

        System.out.println("Finished indexing!");
    }

    private void waitAndFinishIndexing(ExecutorService executor) throws InterruptedException, IOException {
        executor.shutdown();

        if (!executor.awaitTermination(INDEXING_TIMEOUT, TimeUnit.HOURS)) {
            throw new RuntimeException("Timeout when waiting for the indexing task to complete!");
        }

        System.out.println("Finished waiting for all indexing jobs...");

        executor.shutdownNow();

        System.out.println("Running last bulk update...");
        indexConnector.finishIndexing();
    }

    /**
     * Generate a VirtualDocumentTemplate for the entity, given the list of templates per type generated by
     * getTypeBasedTemplates
     *
     * @param typePredicateID    ID for the rdf:type predicate (or any other), used to get the types of this entity
     * @param typeBasedTemplates List of templates per type generated by getTypeBasedTemplates
     * @param method             Combination method to use
     * @param fallback           VirtualDocumentTemplate to return if the entity has no types
     * @return Type-based VirtualDocumentTemplate for the entity.
     */
    public static VirtualDocumentTemplate generateTypeBasedTemplateForEntity(long entityID,
                                                                             long typePredicateID,
                                                                             LocalHDTSPARQLEndpoint endpoint,
                                                                             Map<Long, VirtualDocumentTemplate> typeBasedTemplates,
                                                                             TypeBasedTemplateCombinationMethod method,
                                                                             VirtualDocumentTemplate fallback) throws IOException {
        // Get all types for this entity
        Set<Long> entityTypes = new HashSet<>();
        var typesQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, typePredicateID, 0));
        while (typesQuery.hasNext())
            entityTypes.add(typesQuery.next().getObject());

        List<VirtualDocumentTemplate> templatesForEntity = new ArrayList<>();
        for (long typeID : entityTypes) {
            if (typeBasedTemplates.containsKey(typeID)) {
                templatesForEntity.add(typeBasedTemplates.get(typeID));
            }
        }

        // Return at least the fallback if the entity had no types (ignoring the virtualType)
        if (templatesForEntity.isEmpty())
            return fallback;

        return joinTypeBasedVdocTemplates(entityID, endpoint, templatesForEntity, method);
    }

    /**
     * Join a list of type-based VirtualDocumentTemplates assigned to an entity
     * <p>
     * Currently, the following schema is followed:
     * - Count the predicate appearances in each field among all templates
     * - Assign each predicate to the field with the most appearances, assigning it to the one with the highest
     * weight in case of ties
     * - Filter out the predicates which do not appear for this entity
     */
    public static VirtualDocumentTemplate joinTypeBasedVdocTemplates(long entityID,
                                                                     LocalHDTSPARQLEndpoint endpoint,
                                                                     List<VirtualDocumentTemplate> templates,
                                                                     TypeBasedTemplateCombinationMethod method) throws IOException {
        // Avoid parsing it from the configuration file (avoids I/O penalties)
        VirtualDocumentTemplate tCombined = new VirtualDocumentTemplate();
        for (Field f : templates.get(0).fields)
            tCombined.fields.add(new Field(f.name, new HashSet<>(), f.weight, f.isForObjectProperties, f.isForEntityLinking));

        if (method == TypeBasedTemplateCombinationMethod.MostAppearances) {
            return combineTemplatesWithMostAppearances(entityID, endpoint, templates, tCombined);
        } else if (method == TypeBasedTemplateCombinationMethod.Union) {
            return combineTemplatesWithUnion(templates, tCombined);
        } else if (method == TypeBasedTemplateCombinationMethod.MaximumValues) {
            return combineTemplatesWithMaximumValues(entityID, endpoint, templates);
        } else if (method == TypeBasedTemplateCombinationMethod.GeometricMean) {
            return combineTemplatesWithGeometricMean(entityID, endpoint, templates);
        } else if (method == TypeBasedTemplateCombinationMethod.Repetitions) {
            return combineTemplatesWithRepetitions(entityID, endpoint, templates, tCombined);
        }

        throw new RuntimeException("Invalid Type-based template combination method specified!");
    }

    private static VirtualDocumentTemplate combineTemplatesWithRepetitions(long entityID,
                                                                           LocalHDTSPARQLEndpoint endpoint,
                                                                           List<VirtualDocumentTemplate> templates,
                                                                           VirtualDocumentTemplate tCombined) {
        Set<String> predicatesForEntity = new HashSet<>();
        var predicatesForEntityQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, 0, 0));
        while (predicatesForEntityQuery.hasNext()) {
            long p = predicatesForEntityQuery.next().getPredicate();
            String pString = endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();

            // Filter the InfoRank and PageRank predicates which may be associated to this entity (if its metrics
            // were calculated via the HDT methods)
            if (!(pString.equals(InfoRankMetricsGenerator.INFORANK_URI) || pString.equals(InfoRankMetricsGenerator.PAGERANK_URI)))
                predicatesForEntity.add(pString);
        }

        List<Map<String, Long>> predicateAppearances = new ArrayList<>();

        // Fields are ordered by weight, so all templates have them in the same positions
        for (int i = 0; i < tCombined.fields.size(); i++) {
            Map<String, Long> predicateAppearancesForField = new HashMap<>();

            for (var vdoc : templates) {
                Field f = vdoc.fields.get(i);

                for (var fieldElement : f.predicates) {
                    // Only add those that the entity contains
                    if (predicatesForEntity.contains(fieldElement.getPredicateURI()))
                        predicateAppearancesForField.merge(fieldElement.getPredicateURI(), 1L, Long::sum);
                }
            }

            predicateAppearances.add(predicateAppearancesForField);
        }

        for (String p : predicatesForEntity) {
            // Add fieldElements based on the appearances of each predicate on every field
            for (int i = 0; i < tCombined.fields.size(); i++) {
                if (predicateAppearances.get(i).containsKey(p)) // The predicate appears in the field!
                    tCombined.fields.get(i).predicates.add(new Field.FieldElement(p, predicateAppearances.get(i).get(p)));
            }
        }

        return tCombined;
    }

    private static VirtualDocumentTemplate combineTemplatesWithGeometricMean(long entityID, LocalHDTSPARQLEndpoint endpoint, List<VirtualDocumentTemplate> templates) throws IOException {
        Set<String> predicatesForEntity = new HashSet<>();
        var predicatesForEntityQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, 0, 0));
        while (predicatesForEntityQuery.hasNext()) {
            long p = predicatesForEntityQuery.next().getPredicate();
            String pString = endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();

            // Filter the InfoRank and PageRank predicates which may be associated to this entity (if its metrics
            // were calculated via the HDT methods)
            if (!(pString.equals(InfoRankMetricsGenerator.INFORANK_URI) || pString.equals(InfoRankMetricsGenerator.PAGERANK_URI)))
                predicatesForEntity.add(pString);
        }

        Map<String, List<Double>> valuesToAverage = new HashMap<>();

        for (var vdoc : templates) {
            for (var field : vdoc.fields) {
                for (var fieldElement : field.predicates) {
                    if (predicatesForEntity.contains(fieldElement.getPredicateURI())) {
                        if (valuesToAverage.containsKey(fieldElement.getPredicateURI())) {
                            valuesToAverage.get(fieldElement.getPredicateURI()).add(field.weight);
                        } else {
                            List<Double> gmeansForPredicate = new ArrayList<>();
                            gmeansForPredicate.add(field.weight);

                            valuesToAverage.put(fieldElement.getPredicateURI(), gmeansForPredicate);
                        }
                    }
                }
            }
        }

        List<MetricsAggregator.ResourceWrapper> kMeansEntries = new ArrayList<>();
        for (var entry : valuesToAverage.entrySet()) {
            double gmean = 0.0;
            for (var valueToAverage : entry.getValue()) {
                if (gmean == 0.0)
                    gmean = valueToAverage;
                else
                    gmean *= valueToAverage;
            }

            gmean = Math.pow(gmean, 1.0 / (double) entry.getValue().size());


            kMeansEntries.add(new MetricsAggregator.ResourceWrapper(ResourceFactory.createResource(entry.getKey()), gmean));
        }

        MetricsAggregator dummyAggregator = new DummyMetricsAggregator(MetricsAggregator.getEmptyVirtualDocumentTemplate());

        return dummyAggregator.getVirtualDocumentTemplate(MetricsAggregator.runKMeansWithoutAggregator(kMeansEntries));
    }

    private static VirtualDocumentTemplate combineTemplatesWithMaximumValues(long entityID, LocalHDTSPARQLEndpoint endpoint, List<VirtualDocumentTemplate> templates) throws IOException {
        Set<String> predicatesForEntity = new HashSet<>();
        var predicatesForEntityQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, 0, 0));
        while (predicatesForEntityQuery.hasNext()) {
            long p = predicatesForEntityQuery.next().getPredicate();
            String pString = endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();

            // Filter the InfoRank and PageRank predicates which may be associated to this entity (if its metrics
            // were calculated via the HDT methods)
            if (!(pString.equals(InfoRankMetricsGenerator.INFORANK_URI) || pString.equals(InfoRankMetricsGenerator.PAGERANK_URI)))
                predicatesForEntity.add(pString);
        }

        Map<String, Double> maximumValues = new HashMap<>();

        for (var vdoc : templates) {
            for (var field : vdoc.fields) {
                for (var fieldElement : field.predicates) {
                    if (predicatesForEntity.contains(fieldElement.getPredicateURI())) {
                        if (maximumValues.containsKey(fieldElement.getPredicateURI())) {
                            if (maximumValues.get(fieldElement.getPredicateURI()) < field.weight) {
                                maximumValues.put(fieldElement.getPredicateURI(), field.weight);
                            }
                        } else {
                            maximumValues.put(fieldElement.getPredicateURI(), field.weight);
                        }
                    }
                }
            }
        }

        List<MetricsAggregator.ResourceWrapper> kMeansEntries = new ArrayList<>();
        for (var entry : maximumValues.entrySet()) {
            kMeansEntries.add(new MetricsAggregator.ResourceWrapper(ResourceFactory.createResource(entry.getKey()), entry.getValue()));
        }

        MetricsAggregator dummyAggregator = new DummyMetricsAggregator(MetricsAggregator.getEmptyVirtualDocumentTemplate());

        return dummyAggregator.getVirtualDocumentTemplate(MetricsAggregator.runKMeansWithoutAggregator(kMeansEntries));
    }

    private static VirtualDocumentTemplate combineTemplatesWithUnion(List<VirtualDocumentTemplate> templates, VirtualDocumentTemplate tCombined) {
        // Fields are ordered by weight, so all templates have them in the same positions
        for (int i = 0; i < templates.get(0).fields.size(); i++) {
            Field f = templates.get(0).fields.get(i);
            Field unionField = new Field(f.name, new HashSet<>(), f.weight, f.isForObjectProperties, f.isForEntityLinking);

            for (var vdoc : templates) { // Union of all fields of this priority
                unionField.predicates.addAll(vdoc.fields.get(i).predicates);
            }

            // Remove predicates which appear in higher priority (previous) fields
            for (int j = 0; j < i; j++) {
                unionField.predicates.removeAll(tCombined.fields.get(j).predicates);
            }

            tCombined.fields.set(i, unionField);
        }

        return tCombined;
    }

    private static VirtualDocumentTemplate combineTemplatesWithMostAppearances(long entityID, LocalHDTSPARQLEndpoint endpoint, List<VirtualDocumentTemplate> templates, VirtualDocumentTemplate tCombined) {
        Set<String> predicatesForEntity = new HashSet<>();
        var predicatesForEntityQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, 0, 0));
        while (predicatesForEntityQuery.hasNext()) {
            long p = predicatesForEntityQuery.next().getPredicate();
            String pString = endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();

            // Filter the InfoRank and PageRank predicates which may be associated to this entity (if its metrics
            // were calculated via the HDT methods)
            if (!(pString.equals(InfoRankMetricsGenerator.INFORANK_URI) || pString.equals(InfoRankMetricsGenerator.PAGERANK_URI)))
                predicatesForEntity.add(pString);
        }

        List<Map<String, Long>> predicateAppearances = new ArrayList<>();

        // Fields are ordered by weight, so all templates have them in the same positions
        for (int i = 0; i < tCombined.fields.size(); i++) {
            Map<String, Long> predicateAppearancesForField = new HashMap<>();

            for (var vdoc : templates) {
                Field f = vdoc.fields.get(i);

                for (var fieldElement : f.predicates) {
                    // Only add those that the entity contains
                    if (predicatesForEntity.contains(fieldElement.getPredicateURI()))
                        predicateAppearancesForField.merge(fieldElement.getPredicateURI(), 1L, Long::sum);
                }
            }

            predicateAppearances.add(predicateAppearancesForField);

        }

        for (String p : predicatesForEntity) {
            long max = 0;
            int fieldIndex = 0;

            // Check which field had the most appearances for this predicate
            int i = 0;
            for (var predicateAppearancesForField : predicateAppearances) {
                if (max < predicateAppearancesForField.getOrDefault(p, 0L)) {
                    max = predicateAppearancesForField.get(p);
                    fieldIndex = i;
                }

                i++;
            }

            // Assign this predicate to the field with the most appearances
            // If some or all fields had the same highest appearances, the one with the biggest weight will be chosen
            tCombined.fields.get(fieldIndex).predicates.add(new Field.FieldElement(p, 1));
        }

        return tCombined;
    }

    /**
     * Generate and return a type-based VirtualDocumentTemplate for every type, using the provided engine. It will
     * automatically exclude the virtualType, if it exists
     *
     * @throws InterruptedException If the threaded execution is interrupted
     */
    public static Map<Long, VirtualDocumentTemplate> getTypeBasedTemplates(LocalHDTSPARQLEndpoint endpoint,
                                                                           TypeBasedMetricsAggregator engine) throws InterruptedException {
        Map<Long, VirtualDocumentTemplate> typeBasedTemplates = new ConcurrentHashMap<>();

        long typePredicateID = endpoint.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.TYPE_URI, TripleComponentRole.PREDICATE);
        long virtualTypeID = endpoint.hdt.getDictionary().stringToId(SPARQLEndpoint.VIRTUAL_TYPE, TripleComponentRole.OBJECT);

        // Get all (allowed) types
        Set<Long> types = new HashSet<>();
        var typesQuery = endpoint.hdt.getTriples().search(new TripleID(0, typePredicateID, 0));
        while (typesQuery.hasNext()) {
            long t = typesQuery.next().getObject();
            Resource typeResource = ResourceFactory.createResource(endpoint.hdt.getDictionary().idToString(t, TripleComponentRole.OBJECT).toString());

            if (engine.isTypeAllowed(typeResource))
                types.add(t);
        }

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        AtomicInteger count = new AtomicInteger(1);
        AtomicInteger countFailed = new AtomicInteger(0);

        for (long type : types) {
            completionService.submit(() -> {
                try {
                    String typeString = endpoint.hdt.getDictionary().idToString(type, TripleComponentRole.OBJECT).toString();

                    // Clone the engine and change its type
                    MetricsAggregator clonedEngine = engine.clone();
                    ((TypeBasedMetricsAggregator) clonedEngine).typeURI = typeString;

                    VirtualDocumentTemplate t = clonedEngine.createVirtualDocumentTemplate(endpoint);

                    typeBasedTemplates.put(type, t);
                    System.out.print("Generated template for type " + count.getAndIncrement() + " of " + types.size() + '\r');
                } catch (Exception e) {
                    countFailed.getAndIncrement();

                    count.getAndIncrement();
                    String typeString = endpoint.hdt.getDictionary().idToString(type, TripleComponentRole.PREDICATE).toString();
                    // Its KMeans run probably didn't have enough entries and couldn't converge
                    System.err.println("Warning: Couldn't create a template for type " + typeString + ", reason: " + e);
                    return null;
                }

                return null;
            });
        }

        executor.shutdown();

        if (!executor.awaitTermination(INDEXING_TIMEOUT, TimeUnit.HOURS)) {
            throw new RuntimeException("Timeout when waiting for the type-based template generation tasks to complete!");
        }

        System.out.println("Finished waiting for type-based template generation jobs...");
        System.out.println("Failed types: " + countFailed + "/" + count);

        executor.shutdownNow();

        return typeBasedTemplates;
    }
}

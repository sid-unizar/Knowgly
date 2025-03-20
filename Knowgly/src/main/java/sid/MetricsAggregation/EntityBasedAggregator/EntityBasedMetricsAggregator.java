package sid.MetricsAggregation.EntityBasedAggregator;

import org.apache.jena.rdf.model.ResourceFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.VirtualDocumentTemplate;
import sid.MetricsGeneration.MetricsGenerator;
import sid.MetricsGeneration.SPARQL.ImportanceMetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;


/**
 * An entity-based metrics aggregator, which generates a VirtualDocumentTemplate based on the entity types with a given
 * importance metric. It can be weighted by its equivalent global metrics aggregator.
 * <p>
 * Note: The EntityBasedMetricsAggregator has a strict requirement of only being able to calculate metrics for a given
 * one at the same time, as the cache is shared statically. Before calculating any VirtualDocumentTemplate with
 * it, the cacheAllMetrics method should be called once in a single-threaded context.
 * This has been done for performance reasons, as querying the same metrics for every entity without caching is
 * extremely time-consuming.
 * <p>
 * For the same reasons, it only supports HDT endpoints.
 * <p>
 * In cases where the entity doesn't have any allowed type types assigned to it, a fallback global template
 * will be returned. Otherwise, the clusterization would fail.
 */
public abstract class EntityBasedMetricsAggregator extends MetricsAggregator {
    public String entityURI;
    private final boolean combineWithGlobalTemplate;
    private final double combinationWeight;
    // Cache of predicate ID -> type ID -> metric value
    protected static ConcurrentHashMap<Long, ConcurrentHashMap<Long, Double>> metricsCache = null;
    // Cache of predicate ID -> global metric value
    protected static ConcurrentHashMap<Long, Double> globalMetricsCache = null;
    // Cache of type IDs allowed by isTypeAllowed()
    protected static Set<Long> allowedTypesCache = null;
    // Cache of predicate ID -> URI String
    protected static ConcurrentHashMap<Long, String> predicateNamesCache = null;
    // Cached global template for entities with no allowed types
    public static VirtualDocumentTemplate globalTemplate = null;


    private long idOfTypePredicate;
    private long idOfEntity;

    /**
     * Basic constructor, with aggregator combination. Initializes the clustering configuration via its configuration file
     */
    public EntityBasedMetricsAggregator(String entityURI,
                                        boolean combineWithGlobalTemplate,
                                        double combinationWeight) throws IOException {
        super();
        this.entityURI = entityURI;
        this.combineWithGlobalTemplate = combineWithGlobalTemplate;
        this.combinationWeight = combinationWeight;
    }

    public String getEntityURI() {
        return entityURI;
    }

    public void setEntityURI(String entityURI) {
        this.entityURI = entityURI;
    }

    public abstract void cacheAllMetrics(LocalHDTSPARQLEndpoint endpoint, VirtualDocumentTemplate globalTemplate) throws IOException;

    /**
     * Helper cacheAllMetrics for implementors, which only need to provide the name of the metric, as it appears in the
     * metrics endpoint
     */
    protected void cacheAllMetrics(LocalHDTSPARQLEndpoint endpoint, String metricVariableName, VirtualDocumentTemplate globalTemplate) throws IOException {
        EntityBasedMetricsAggregator.globalTemplate = globalTemplate;

        metricsCache = new ConcurrentHashMap<>();
        globalMetricsCache = new ConcurrentHashMap<>();
        allowedTypesCache = ConcurrentHashMap.newKeySet();
        predicateNamesCache = new ConcurrentHashMap<>();

        long idOfRDFTypeURI = endpoint.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.RDF_TYPE_URI, TripleComponentRole.PREDICATE);
        long idOfPredicateTypeURI = endpoint.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.PREDICATE_TYPE_URI, TripleComponentRole.OBJECT);
        long idOfPredicateURI = endpoint.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.PREDICATE_URI, TripleComponentRole.PREDICATE);
        long idOfTypeURI = endpoint.hdt.getDictionary().stringToId(ImportanceMetricsGenerator.TYPE_URI, TripleComponentRole.PREDICATE);
        IteratorTripleID predicateTypeQuery = endpoint.hdt.getTriples().search(new TripleID(0, idOfRDFTypeURI, idOfPredicateTypeURI));

        // Metrics cache
        while (predicateTypeQuery.hasNext()) {
            long predicateTypeID = predicateTypeQuery.next().getSubject();
            long t;
            long p;
            double metric;

            var typeQuery = endpoint.hdt.getTriples().search(new TripleID(predicateTypeID, idOfTypeURI, 0));
            if (typeQuery.hasNext()) {
                t = typeQuery.next().getObject();

                String tString = endpoint.hdt.getDictionary().idToString(t, TripleComponentRole.OBJECT).toString();
                if (!isTypeAllowed(ResourceFactory.createResource(tString))) continue;
            } else {
                continue;
            }

            var predicateQuery = endpoint.hdt.getTriples().search(new TripleID(predicateTypeID, idOfPredicateURI, 0));
            if (predicateQuery.hasNext()) {
                // We need to convert the predicate's ID from "object-space" to "predicate-space" within the HDT dictionary
                // To achieve this, we do a temporary conversion from object id to string and look it up as a predicate
                p = endpoint.hdt.getDictionary().stringToId(
                        endpoint.hdt.getDictionary().idToString(predicateQuery.next().getObject(), TripleComponentRole.OBJECT),
                        TripleComponentRole.PREDICATE
                );
            } else {
                continue;
            }

            long metricID = endpoint.hdt.getDictionary().stringToId(
                    ImportanceMetricsGenerator.BASE_IMPORTANCE_METRIC_URI + "/" + metricVariableName,
                    TripleComponentRole.PREDICATE);
            var metricQuery = endpoint.hdt.getTriples().search(new TripleID(predicateTypeID, metricID, 0));
            if (metricQuery.hasNext()) {
                metric = MetricsGenerator.floatLiteralToDouble(endpoint.hdt, metricQuery.next());
            } else {
                continue;
            }

            if (metricsCache.containsKey(p)) {
                metricsCache.get(p).put(t, metric);
            } else {
                ConcurrentHashMap<Long, Double> innerMetricsMap = new ConcurrentHashMap<>();
                innerMetricsMap.put(t, metric);
                metricsCache.put(p, innerMetricsMap);
            }
        }

        // Global metrics cache
        for (var entry : metricsCache.entrySet()) {
            long p = entry.getKey();
            double globalImportance = 0.0;
            for (double impForType : entry.getValue().values())
                globalImportance += impForType;

            globalMetricsCache.put(p, globalImportance);
        }

        // Allowed types cache
        var typesQuery = endpoint.hdt.getTriples().search(new TripleID(0, idOfRDFTypeURI, 0));
        while (typesQuery.hasNext()) {
            long t = typesQuery.next().getObject();

            if (!allowedTypesCache.contains(t))
                if (isTypeAllowed(ResourceFactory.createResource(endpoint.hdt.getDictionary().idToString(t, TripleComponentRole.OBJECT).toString())))
                    allowedTypesCache.add(t);
        }

        // Predicate names cache
        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1);
        predicatesStream.forEach(p -> {
            predicateNamesCache.put(p, endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString());
        });
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        if (!(endpoint instanceof LocalHDTSPARQLEndpoint))
            throw new RuntimeException("Attempted to run an EntityBasedMetricsAggregator with a non-HDT endpoint!");

        if (metricsCache == null)
            throw new RuntimeException("Attempted to run an EntityBasedMetricsAggregator without running cacheAllMetrics before!");

        idOfEntity = ((LocalHDTSPARQLEndpoint) endpoint).hdt.getDictionary().stringToId(entityURI, TripleComponentRole.SUBJECT);
        idOfTypePredicate = ((LocalHDTSPARQLEndpoint) endpoint).hdt.getDictionary().stringToId(ImportanceMetricsGenerator.RDF_TYPE_URI, TripleComponentRole.PREDICATE);

        List<ResourceWrapper> clusterInput = new ArrayList<>();

        if (entityHasAllowedTypes((LocalHDTSPARQLEndpoint) endpoint)) { // Return an entity-based template
            Set<Long> typesOfEntity = getTypesOfEntity((LocalHDTSPARQLEndpoint) endpoint);
            Set<Long> predicatesOfEntity = getPredicatesOfEntity((LocalHDTSPARQLEndpoint) endpoint);

            for (long p : predicatesOfEntity) {
                String pString = predicateNamesCache.get(p);

                double importance = 0.0;

                if (metricsCache.containsKey(p)) {
                    var entriesForP = metricsCache.get(p);

                    for (long t : typesOfEntity) {
                        if (entriesForP.containsKey(t)) {
                            importance += entriesForP.get(t);
                        }
                    }
                } else { // It's an inforank/pagerank/any other metrics predicate associated to entities
                    continue;
                }

                if (combineWithGlobalTemplate) {
                    double globalImportance = globalMetricsCache.get(p);
                    double combinedImportance = Math.pow(importance, combinationWeight) * Math.pow(globalImportance, (1 - combinationWeight));

                    clusterInput.add(new ResourceWrapper(ResourceFactory.createResource(pString), combinedImportance));
                } else {
                    clusterInput.add(new ResourceWrapper(ResourceFactory.createResource(pString), importance));
                }
            }
        } else { // Return the cached fallback template if possible or build a global clusterInput
            if (forceReturnClusterInput) {
                for (var entry : globalMetricsCache.entrySet()) {
                    long p = entry.getKey();
                    double importance = entry.getValue();
                    String pString = predicateNamesCache.get(p);

                    clusterInput.add(new ResourceWrapper(ResourceFactory.createResource(pString), importance));
                }
            } else {
                return new OptionalClusterInputOrTemplate(globalTemplate);
            }
        }

        return new OptionalClusterInputOrTemplate(clusterInput);
    }

    /**
     * Returns true if the entity has one or more allowed type assigned to it
     */
    public boolean entityHasAllowedTypes(LocalHDTSPARQLEndpoint endpoint) {
        var typesQuery = endpoint.hdt.getTriples().search(new TripleID(idOfEntity, idOfTypePredicate, 0));
        while (typesQuery.hasNext()) {
            // If it doesn't contain an ignored type, it's OK
            if (allowedTypesCache.contains(typesQuery.next().getObject()))
                return true;
        }

        return false;
    }

    private Set<Long> getTypesOfEntity(LocalHDTSPARQLEndpoint endpoint) {
        Set<Long> typesOfEntity = new HashSet<>();

        var typesQuery = endpoint.hdt.getTriples().search(new TripleID(idOfEntity, idOfTypePredicate, 0));
        while (typesQuery.hasNext()) {
            long t = typesQuery.next().getObject();

            // We add it ignoring whether it's allowed or not, as the lookup in the cache will simply fail and get
            // ignored if it was not allowed
            typesOfEntity.add(t);
        }


        return typesOfEntity;
    }

    private Set<Long> getPredicatesOfEntity(LocalHDTSPARQLEndpoint endpoint) {
        Set<Long> predicatesOfEntity = new HashSet<>();

        var predicatesQuery = endpoint.hdt.getTriples().search(new TripleID(idOfEntity, 0, 0));
        while (predicatesQuery.hasNext())
            predicatesOfEntity.add(predicatesQuery.next().getPredicate());

        return predicatesOfEntity;
    }
}

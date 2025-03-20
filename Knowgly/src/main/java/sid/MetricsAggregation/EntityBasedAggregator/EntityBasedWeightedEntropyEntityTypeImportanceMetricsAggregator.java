package sid.MetricsAggregation.EntityBasedAggregator;

import org.apache.jena.rdf.model.ResourceFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sid.MetricsAggregation.VirtualDocumentTemplate;
import sid.MetricsGeneration.MetricsGenerator;
import sid.MetricsGeneration.SPARQL.ImportanceMetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;


/**
 * An entity based metrics aggregator, using a weighted entropyEntityTypeImportance metric
 */
public class EntityBasedWeightedEntropyEntityTypeImportanceMetricsAggregator extends EntityBasedMetricsAggregator {
    private final double predicateEntropyTypeWeight;

    /**
     * Basic constructor, with aggregator combination. Initializes the clustering configuration via its configuration file
     */
    public EntityBasedWeightedEntropyEntityTypeImportanceMetricsAggregator(String entityURI,
                                                                           boolean combineWithGlobalTemplate,
                                                                           double combinationWeight,
                                                                           double predicateEntropyTypeWeight) throws IOException {
        super(entityURI, combineWithGlobalTemplate, combinationWeight);
        this.predicateEntropyTypeWeight = predicateEntropyTypeWeight;
    }

    @Override
    public void cacheAllMetrics(LocalHDTSPARQLEndpoint endpoint, VirtualDocumentTemplate globalTemplate) throws IOException {
        cacheAllMetrics(endpoint, globalTemplate, predicateEntropyTypeWeight);
    }

    public void cacheAllMetrics(LocalHDTSPARQLEndpoint endpoint, VirtualDocumentTemplate globalTemplate, double predicateEntropyTypeWeight) throws IOException {
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
            double predEntropyTypeMtric;
            double entityTypeImpMetric;
            double finalMetric;

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

            // predicateEntropyType
            long metric1ID = endpoint.hdt.getDictionary().stringToId(
                    ImportanceMetricsGenerator.BASE_IMPORTANCE_METRIC_URI + "/" + "predicateEntropyType",
                    TripleComponentRole.PREDICATE);
            var metric1Query = endpoint.hdt.getTriples().search(new TripleID(predicateTypeID, metric1ID, 0));
            if (metric1Query.hasNext()) {
                predEntropyTypeMtric = MetricsGenerator.floatLiteralToDouble(endpoint.hdt, metric1Query.next());
            } else {
                continue;
            }

            // entityTypeImportance
            long metric2ID = endpoint.hdt.getDictionary().stringToId(
                    ImportanceMetricsGenerator.BASE_IMPORTANCE_METRIC_URI + "/" + "entityTypeImportance",
                    TripleComponentRole.PREDICATE);
            var metric2Query = endpoint.hdt.getTriples().search(new TripleID(predicateTypeID, metric2ID, 0));
            if (metric2Query.hasNext()) {
                entityTypeImpMetric = MetricsGenerator.floatLiteralToDouble(endpoint.hdt, metric2Query.next());
            } else {
                continue;
            }

            finalMetric = (Math.pow(predEntropyTypeMtric, predicateEntropyTypeWeight) * Math.pow(entityTypeImpMetric, (1 - predicateEntropyTypeWeight)));

            if (metricsCache.containsKey(p)) {
                metricsCache.get(p).put(t, finalMetric);
            } else {
                ConcurrentHashMap<Long, Double> innerMetricsMap = new ConcurrentHashMap<>();
                innerMetricsMap.put(t, finalMetric);
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
        return super.getOptionalClusterInput(endpoint, forceReturnClusterInput);
    }
}

package sid.MetricsAggregation.TypeBasedAggregator;

import sid.MetricsAggregation.MetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Metrics aggregator equivalent to EntityTypeImportanceMetricsAggregator, for a single type
 * <p>
 * Allows its combination with InfoRank via its specific constructors
 */
public class TypeBasedPredicateEntropyTypeMetricsAggregator extends TypeBasedMetricsAggregator {
    public static final String PREDICATE_ENTROPY_TYPE_FOR_TYPE_SPARQL = "configuration/queries/metrics_aggregator_queries/predicateEntropyType_for_type.sparql";
    public static final String PREDICATE_ENTROPY_TYPE_FOR_TYPE_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/predicateEntropyType_for_type_no_subgraph.sparql";

    /**
     * Full constructor, without combination
     *
     * @param typeURI type URI for which the metrics are calculated
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedPredicateEntropyTypeMetricsAggregator(String typeURI, int kMeansClusters, int kMeansIterations) throws IOException {
        super(typeURI, kMeansClusters, kMeansIterations);
    }

    /**
     * Full constructor, with combination
     *
     * @param typeURI           type URI for which the metrics are calculated
     * @param combinationWeight weight to assign to this aggregator vs. inforank's in the weighted geometric mean between both
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedPredicateEntropyTypeMetricsAggregator(String typeURI,
                                                          MetricsAggregator combinationAggregator,
                                                          double combinationWeight,
                                                          int kMeansClusters,
                                                          int kMeansIterations) throws IOException {
        super(typeURI, kMeansClusters, kMeansIterations);
        this.combinationAggregator = combinationAggregator;
        this.combinationWeight = combinationWeight;
    }

    /**
     * Basic constructor. Initializes the clustering configuration via its configuration file
     *
     * @param typeURI type URI for which the metrics are calculated
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedPredicateEntropyTypeMetricsAggregator(String typeURI) throws IOException {
        super(typeURI);
    }

    /**
     * Empty constructor. Use it only as input to the indexing pipeline, which will clone it and change its typeURI
     *
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedPredicateEntropyTypeMetricsAggregator() throws IOException {
        super("");
    }

    /**
     * Empty constructor with InfoRank combination. Use it only as input to the indexing pipeline, which will clone it and change its typeURI
     *
     * @param combinationWeight weight to assign to this aggregator vs. inforank's in the weighted geometric mean between both
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedPredicateEntropyTypeMetricsAggregator(MetricsAggregator combinationAggregator, double combinationWeight) throws IOException {
        super("");
        this.combinationAggregator = combinationAggregator;
        this.combinationWeight = combinationWeight;
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        return super.getClusterInput(endpoint,
                PREDICATE_ENTROPY_TYPE_FOR_TYPE_SPARQL,
                PREDICATE_ENTROPY_TYPE_FOR_TYPE_SPARQL_NO_SUBGRAPH,
                "predicateEntropyType");
    }
}

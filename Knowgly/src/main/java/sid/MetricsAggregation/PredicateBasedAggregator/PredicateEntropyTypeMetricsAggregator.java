package sid.MetricsAggregation.PredicateBasedAggregator;

import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Global measure for each predicate, not taking into account entities, and using the geometric mean:
 * sum(predicateEntropyType(p, t)), for each type t associated to a predicate p
 */
public class PredicateEntropyTypeMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String PREDICATE_ENTROPY_TYPE_SPARQL = "configuration/queries/metrics_aggregator_queries/predicateEntropyType.sparql";
    public static final String PREDICATE_ENTROPY_TYPE_NO_SUBGRAPH_SPARQL = "configuration/queries/metrics_aggregator_queries/predicateEntropyType_no_subgraph.sparql";

    public PredicateEntropyTypeMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    // Basic constructor. Initializes the clustering configuration via its configuration file
    public PredicateEntropyTypeMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        return super.getClusterInputImportanceMetricsBased(endpoint,
                PREDICATE_ENTROPY_TYPE_SPARQL,
                PREDICATE_ENTROPY_TYPE_NO_SUBGRAPH_SPARQL,
                "predicateEntropyType");
    }
}

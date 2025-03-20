package sid.MetricsAggregation.PredicateBasedAggregator;

import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Global measure for each predicate, not taking into account entities, and using the geometric mean:
 * sum(entropyTypeImportance(p, t)), for each type t associated to a predicate p
 */
public class PredicateEntropyTypeImportanceMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String ENTROPY_TYPE_IMPORTANCE_SPARQL = "configuration/queries/metrics_aggregator_queries/entropyTypeImportance.sparql";
    public static final String ENTROPY_TYPE_IMPORTANCE_NO_SUBGRAPH_SPARQL = "configuration/queries/metrics_aggregator_queries/entropyTypeImportance_no_subgraph.sparql";

    public PredicateEntropyTypeImportanceMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    // Basic constructor. Initializes the clustering configuration via its configuration file
    public PredicateEntropyTypeImportanceMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        return super.getClusterInputImportanceMetricsBased(endpoint,
                ENTROPY_TYPE_IMPORTANCE_SPARQL,
                ENTROPY_TYPE_IMPORTANCE_NO_SUBGRAPH_SPARQL,
                "entropyTypeImportance");
    }
}

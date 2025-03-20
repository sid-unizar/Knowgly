package sid.MetricsAggregation.PredicateBasedAggregator;

import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Global measure for each predicate, not taking into account entities, and using the geometric mean:
 * sum(entropyEntityTypeImportance(p, t)), for each type t associated to a predicate p
 */
public class PredicateEntropyEntityTypeImportanceMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL = "configuration/queries/metrics_aggregator_queries/entropyEntityTypeImportance.sparql";
    public static final String ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/entropyEntityTypeImportance_no_subgraph.sparql";

    public PredicateEntropyEntityTypeImportanceMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    // Basic constructor. Initializes the clustering configuration via its configuration file
    public PredicateEntropyEntityTypeImportanceMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        return super.getClusterInputImportanceMetricsBased(endpoint,
                ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL,
                ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL_NO_SUBGRAPH,
                "entropyEntityTypeImportance");
    }
}

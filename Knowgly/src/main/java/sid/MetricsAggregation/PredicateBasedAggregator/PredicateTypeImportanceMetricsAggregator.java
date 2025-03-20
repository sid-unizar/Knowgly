package sid.MetricsAggregation.PredicateBasedAggregator;

import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;

/**
 * Global measure for each predicate, not taking into account entities, and using the geometric mean:
 * sum(typeImportance(p, t)), for each type t associated to a predicate p
 */
public class PredicateTypeImportanceMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String G_MEANS_SPARQL = "configuration/queries/metrics_aggregator_queries/typeImportance.sparql";
    public static final String G_MEANS_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/typeImportance_no_subgraph.sparql";

    public PredicateTypeImportanceMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    // Basic constructor. Initializes the clustering configuration via its configuration file
    public PredicateTypeImportanceMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        return super.getClusterInputImportanceMetricsBased(endpoint,
                G_MEANS_SPARQL,
                G_MEANS_SPARQL_NO_SUBGRAPH,
                "typeImportance");
    }
}

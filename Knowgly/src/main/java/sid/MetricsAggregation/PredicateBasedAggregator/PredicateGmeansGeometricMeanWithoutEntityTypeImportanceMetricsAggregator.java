package sid.MetricsAggregation.PredicateBasedAggregator;

import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;

/**
 * Global Gmeans measure for each predicate, not taking into account entityTypeImportance, and using the geometric mean:
 * sum((typeImportance(p, t) * entropyTypeImportance(p, t))^(1/2)), for each type t associated to a predicate p
 */
public class PredicateGmeansGeometricMeanWithoutEntityTypeImportanceMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String G_MEANS_SPARQL = "configuration/queries/metrics_aggregator_queries/predicate_g_means_mult_without_entity_type_importance.sparql";
    public static final String G_MEANS_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/predicate_g_means_mult_without_entity_type_importance_no_subgraph.sparql";

    public PredicateGmeansGeometricMeanWithoutEntityTypeImportanceMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    // Basic constructor. Initializes the clustering configuration via its configuration file
    public PredicateGmeansGeometricMeanWithoutEntityTypeImportanceMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        return getClusterInputImportanceMetricsBasedGeometricMean(endpoint,
                G_MEANS_SPARQL,
                G_MEANS_SPARQL_NO_SUBGRAPH,
                "gmeans",
                1 / 2f);
    }
}

package sid.MetricsAggregation.PredicateBasedAggregator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import sid.MetricsAggregation.MetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;
import sid.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Global HMean measure for each predicate, not taking into account entities, and using the sum of both metrics:
 * HarmonicMean((typeImportance(p, t) + entropyEntityTypeImportance(p, t))), for each type t associated to a predicate p
 */
public class PredicateHarmonicMeansMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String H_MEANS_SPARQL = "configuration/queries/metrics_aggregator_queries/predicate_h_means.sparql";
    public static final String H_MEANS_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/predicate_h_means_no_subgraph.sparql";

    public PredicateHarmonicMeansMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    public PredicateHarmonicMeansMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        String hmeansQuery;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            hmeansQuery = MetricsAggregator.getQueryString(H_MEANS_SPARQL);
        else
            hmeansQuery = MetricsAggregator.getQueryString(H_MEANS_SPARQL_NO_SUBGRAPH);

        ResultSet rs = endpoint.runSelectQuery(hmeansQuery);

        // Resource -> (count of gmeans from each type, accum of Hmeans from each type)
        Map<Resource, Pair<Integer, Double>> hmeansPerPredicate = new HashMap<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("pred");
            Resource t = qs.getResource("type");
            Literal hmeans = qs.getLiteral("hmeans");

            if (!isTypeAllowed(t)) {
                continue;
            }

            if (hmeansPerPredicate.containsKey(p)) {
                Pair<Integer, Double> d = hmeansPerPredicate.get(p);
                hmeansPerPredicate.put(p, new Pair<>(d.getKey() + 1, hmeans.getDouble() + (3.0 / d.getValue())));

            } else {
                hmeansPerPredicate.put(p, new Pair<>(1, 3.0 / hmeans.getDouble()));
            }
        }

        List<ResourceWrapper> clusterInput = new ArrayList<>(hmeansPerPredicate.size());
        for (var entry : hmeansPerPredicate.entrySet())
            clusterInput.add(new ResourceWrapper(entry.getKey(), entry.getValue().getValue()));

        return new OptionalClusterInputOrTemplate(clusterInput);
    }
}

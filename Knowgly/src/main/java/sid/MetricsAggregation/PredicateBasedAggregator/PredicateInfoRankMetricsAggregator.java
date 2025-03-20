package sid.MetricsAggregation.PredicateBasedAggregator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import sid.MetricsAggregation.MetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Global predicate InfoRank measure for each predicate, using the IR(p) metric (NOT the entity InfoRank!)
 */
public class PredicateInfoRankMetricsAggregator extends PredicateBasedMetricsAggregator {
    public static final String PROPERTIES_INFORANK_SPARQL = "configuration/queries/metrics_aggregator_queries/properties_inforank.sparql";
    public static final String PROPERTIES_INFORANK_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/properties_inforank_no_subgraph.sparql";

    public PredicateInfoRankMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    public PredicateInfoRankMetricsAggregator() throws IOException {
        super();
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        String propertiesInforanksQuery;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            propertiesInforanksQuery = MetricsAggregator.getQueryString(PROPERTIES_INFORANK_SPARQL);
        else
            propertiesInforanksQuery = MetricsAggregator.getQueryString(PROPERTIES_INFORANK_SPARQL_NO_SUBGRAPH);

        ResultSet rs = endpoint.runSelectQuery(propertiesInforanksQuery);

        List<ResourceWrapper> clusterInput = new ArrayList<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("p");
            Literal ir = qs.getLiteral("ir");

            clusterInput.add(new ResourceWrapper(p, ir.getDouble()));
        }

        return new OptionalClusterInputOrTemplate(clusterInput);
    }
}

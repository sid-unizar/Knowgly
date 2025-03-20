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
 * A predicate-based metrics aggregator, which generates a global VirtualDocumentTemplate for all entities
 */
public abstract class PredicateBasedMetricsAggregator extends MetricsAggregator {
    public PredicateBasedMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
    }

    public PredicateBasedMetricsAggregator() throws IOException {
        super();
    }

    /**
     * Generic clusterInput method for any children (importance metrics-based) aggregator which does not need to modify
     * the metrics retrieved by its query
     *
     * @param endpoint                 Metrics endpoint
     * @param queryFile                SPARQL query file to be formatted with the typeURI, which returns pairs of "pred", "type" and metricVariableName
     * @param queryFileWithoutSubgraph Same query file, without using subgraphs (for HDT endpoints)
     * @param metricVariableName       metric variable name used in the SPARQL query
     * @return The cluster input to be used by the MetricsAggregator internal methods
     */
    protected OptionalClusterInputOrTemplate getClusterInputImportanceMetricsBased(SPARQLEndpoint endpoint,
                                                                                   String queryFile,
                                                                                   String queryFileWithoutSubgraph,
                                                                                   String metricVariableName) throws IOException {
        String query;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            query = MetricsAggregator.getQueryString(queryFile);
        else
            query = MetricsAggregator.getQueryString(queryFileWithoutSubgraph);

        ResultSet rs = endpoint.runSelectQuery(query);

        // Resource -> (count of types associated to the predicate, accum of metrics for the predicate)
        // Note: The count is kept for any future implementations. For now, we simply sum all values, but we could also
        //       average them, calculate their geometric mean, etc. (this gave worse results during preliminar testing)
        Map<Resource, Pair<Integer, Double>> predicateMetrics = new HashMap<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("pred");
            Resource t = qs.getResource("type");
            Literal metric = qs.getLiteral(metricVariableName);

            if (!isTypeAllowed(t)) {
                continue;
            }

            if (predicateMetrics.containsKey(p)) {
                Pair<Integer, Double> d = predicateMetrics.get(p);
                predicateMetrics.put(p, new Pair<>(d.getKey() + 1, metric.getDouble() + d.getValue()));

            } else {
                predicateMetrics.put(p, new Pair<>(1, metric.getDouble()));
            }
        }

        List<ResourceWrapper> clusterInput = new ArrayList<>(predicateMetrics.size());
        for (var entry : predicateMetrics.entrySet())
            clusterInput.add(new ResourceWrapper(entry.getKey(), entry.getValue().getValue()));

        return new OptionalClusterInputOrTemplate(clusterInput);
    }

    /**
     * Generic clusterInput method for any children (importance metrics-based) aggregator which calculates a weighted
     * geometric mean of the results for each type
     *
     * @param endpoint                 Metrics endpoint
     * @param queryFile                SPARQL query file to be formatted with the typeURI, which returns pairs of "pred", "type" and metricVariableName
     * @param queryFileWithoutSubgraph Same query file, without using subgraphs (for HDT endpoints)
     * @param metricVariableName       metric variable name used in the SPARQL query
     * @param geometricMeanExp         Exponent to be used on each term of the geometric mean (1/3f, 1/2f...)
     * @return The cluster input to be used by the MetricsAggregator internal methods
     */
    protected OptionalClusterInputOrTemplate getClusterInputImportanceMetricsBasedGeometricMean(SPARQLEndpoint endpoint,
                                                                                                String queryFile,
                                                                                                String queryFileWithoutSubgraph,
                                                                                                String metricVariableName,
                                                                                                double geometricMeanExp) throws IOException {
        String query;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            query = MetricsAggregator.getQueryString(queryFile);
        else
            query = MetricsAggregator.getQueryString(queryFileWithoutSubgraph);

        ResultSet rs = endpoint.runSelectQuery(query);

        // Resource -> (count of types associated to the predicate, accum of metrics for the predicate)
        // Note: The count is kept for any future implementations. For now, we simply sum all values, but we could also
        //       average them, calculate their geometric mean, etc. (this gave worse results during preliminar testing)
        Map<Resource, Pair<Integer, Double>> predicateMetrics = new HashMap<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("pred");
            Resource t = qs.getResource("type");
            Literal metric = qs.getLiteral(metricVariableName);

            if (!isTypeAllowed(t)) {
                continue;
            }

            if (predicateMetrics.containsKey(p)) {
                Pair<Integer, Double> d = predicateMetrics.get(p);
                predicateMetrics.put(p, new Pair<>(d.getKey() + 1, Math.pow(metric.getDouble(), geometricMeanExp) + d.getValue()));

            } else {
                predicateMetrics.put(p, new Pair<>(1, Math.pow(metric.getDouble(), geometricMeanExp)));
            }
        }

        List<ResourceWrapper> clusterInput = new ArrayList<>(predicateMetrics.size());
        for (var entry : predicateMetrics.entrySet())
            clusterInput.add(new ResourceWrapper(entry.getKey(), entry.getValue().getValue()));

        return new OptionalClusterInputOrTemplate(clusterInput);
    }
}

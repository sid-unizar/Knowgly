package sid.MetricsAggregation.TypeBasedAggregator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import sid.MetricsAggregation.MetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A type-based metrics aggregator, which generates a VirtualDocumentTemplate based on the type it has been assigned.
 * Changing the type this engine should tweak the vdoc for is allowed, alongside cloning it in order to "share" the
 * instance across multiple threads, with different types
 * <p>
 * Note: In order to keep compatibility with the predicate-based metrics aggregators, which generate clusterings
 * for all the KG's predicates while these don't, combinations are not allowed. All combinations are handled
 * internally via its children constructors
 */
public abstract class TypeBasedMetricsAggregator extends MetricsAggregator {
    public String typeURI;
    protected MetricsAggregator combinationAggregator = null;
    protected double combinationWeight;

    // Basic constructor. Initializes the clustering configuration via its configuration file
    protected TypeBasedMetricsAggregator(String typeURI) throws IOException {
        super();
        this.typeURI = typeURI;
    }

    protected TypeBasedMetricsAggregator(String typeURI, int kMeansClusters, int kMeansIterations) throws IOException {
        super(kMeansClusters, kMeansIterations);
        this.typeURI = typeURI;
    }

    public String getTypeURI() {
        return typeURI;
    }

    public void setTypeURI(String typeURI) {
        this.typeURI = typeURI;
    }

    /**
     * Generic clusterInput method for any children aggregator which does not need to modify the metrics retrieved by its
     * query
     *
     * @param endpoint                 Metrics endpoint
     * @param queryFile                SPARQL query file to be formatted with the typeURI, which returns pairs of "pred" and metricVariableName
     * @param queryFileWithoutSubgraph Same query file, without using subgraphs (for HDT endpoints)
     * @param metricVariableName       metric variable name used in the SPARQL query
     * @return The cluster input to be used by the MetricsAggregator internal methods
     */
    protected OptionalClusterInputOrTemplate getClusterInput(SPARQLEndpoint endpoint,
                                                             String queryFile,
                                                             String queryFileWithoutSubgraph,
                                                             String metricVariableName) throws IOException, ExecutionException, InterruptedException {
        String query;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            query = MetricsAggregator.getQueryString(queryFile).formatted(typeURI);
        else
            query = MetricsAggregator.getQueryString(queryFileWithoutSubgraph).formatted(typeURI);

        ResultSet rs = endpoint.runSelectQuery(query);

        // Map of predicate -> metric value
        Map<Resource, Double> predicateMetrics = new HashMap<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("pred");
            Literal metric = qs.getLiteral(metricVariableName);

            predicateMetrics.put(p, metric.getDouble());
        }

        List<ResourceWrapper> clusterInput = new ArrayList<>(predicateMetrics.size());

        if (combinationAggregator != null) {
            if (combinationAggregator instanceof TypeBasedMetricsAggregator)
                ((TypeBasedMetricsAggregator) combinationAggregator).typeURI = this.typeURI;

            List<ResourceWrapper> combinationClusterInput = combinationAggregator.getOptionalClusterInput(endpoint, true).getClusterInput();
            for (var entry : predicateMetrics.entrySet()) {
                for (var combinationEntry : combinationClusterInput) {
                    if (combinationEntry.getResource().equals(entry.getKey())) {
                        double predicateMetric = entry.getValue();
                        double combinationMetric = combinationEntry.getPoint()[0];
                        double combinedScore = Math.pow(predicateMetric, combinationWeight) * Math.pow(combinationMetric, (1 - combinationWeight));

                        clusterInput.add(new ResourceWrapper(entry.getKey(), combinedScore));
                        break;
                    }
                }
            }
        } else {
            for (var entry : predicateMetrics.entrySet())
                clusterInput.add(new ResourceWrapper(entry.getKey(), entry.getValue()));
        }

        return new OptionalClusterInputOrTemplate(clusterInput);
    }

    public boolean isCombined() {
        return (combinationAggregator != null);
    }

    public MetricsAggregator getCombinationAggregator() {
        return combinationAggregator;
    }

    public double getCombinationWeight() {
        return combinationWeight;
    }
}

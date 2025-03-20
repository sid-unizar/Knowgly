package sid.MetricsAggregation.TypeBasedAggregator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.PredicateBasedAggregator.PredicateInfoRankMetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metrics aggregator equivalent to PredicateGmeansGeometricMeanMetricsAggregator, for a single type
 * <p>
 * Allows its combination with InfoRank via its specific constructors
 */
public class TypeBasedGmeansGeometricMeanMetricsAggregator extends TypeBasedMetricsAggregator {
    public static final String G_MEANS_SPARQL = "configuration/queries/metrics_aggregator_queries/predicate_g_means_mult_for_type.sparql";
    public static final String G_MEANS_SPARQL_NO_SUBGRAPH = "configuration/queries/metrics_aggregator_queries/predicate_g_means_mult_for_type_no_subgraph.sparql";

    /**
     * Full constructor, without InfoRank combination
     *
     * @param typeURI type URI for which the metrics are calculated
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedGmeansGeometricMeanMetricsAggregator(String typeURI, int kMeansClusters, int kMeansIterations) throws IOException {
        super(typeURI, kMeansClusters, kMeansIterations);
    }

    /**
     * Full constructor, with combination
     *
     * @param typeURI           type URI for which the metrics are calculated
     * @param combinationWeight weight to assign to this aggregator vs. inforank's in the weighted geometric mean between both
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedGmeansGeometricMeanMetricsAggregator(String typeURI,
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
    public TypeBasedGmeansGeometricMeanMetricsAggregator(String typeURI) throws IOException {
        super(typeURI);
    }

    /**
     * Empty constructor. Use it only as input to the indexing pipeline, which will clone it and change its typeURI
     *
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedGmeansGeometricMeanMetricsAggregator() throws IOException {
        super("");
    }

    /**
     * Empty constructor with InfoRank combination. Use it only as input to the indexing pipeline, which will clone it and change its typeURI
     *
     * @param combinationWeight weight to assign to this aggregator vs. inforank's in the weighted geometric mean between both
     * @throws IOException If an IO error occurs when reading the configuration file
     */
    public TypeBasedGmeansGeometricMeanMetricsAggregator(MetricsAggregator combinationAggregator, double combinationWeight) throws IOException {
        super("");
        this.combinationAggregator = combinationAggregator;
        this.combinationWeight = combinationWeight;
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        String gmeansQuery;
        if (endpoint instanceof SPARQLEndpointWithNamedGraphs)
            gmeansQuery = MetricsAggregator.getQueryString(G_MEANS_SPARQL).formatted(typeURI);
        else
            gmeansQuery = MetricsAggregator.getQueryString(G_MEANS_SPARQL_NO_SUBGRAPH).formatted(typeURI);

        ResultSet rs = endpoint.runSelectQuery(gmeansQuery);

        Map<Resource, Double> gmeansPerPredicate = new HashMap<>();

        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            Resource p = qs.getResource("pred");
            Literal gmeans = qs.getLiteral("gmeans");

            gmeansPerPredicate.put(p, Math.pow(gmeans.getDouble(), 1 / 3f));
        }

        List<ResourceWrapper> clusterInput = new ArrayList<>(gmeansPerPredicate.size());


        if (combinationAggregator != null) {
            List<ResourceWrapper> combinationClusterInput = new PredicateInfoRankMetricsAggregator().getOptionalClusterInput(endpoint, true).getClusterInput();
            for (var entry : gmeansPerPredicate.entrySet()) {
                for (var combinationEntry : combinationClusterInput) {
                    if (combinationEntry.getResource().equals(entry.getKey())) {
                        double gmeans = entry.getValue();
                        double irP = combinationEntry.getPoint()[0];
                        double combinedScore = Math.pow(gmeans, combinationWeight) * Math.pow(irP, (1 - combinationWeight));

                        clusterInput.add(new ResourceWrapper(entry.getKey(), combinedScore));
                        break;
                    }
                }
            }
        } else {
            for (var entry : gmeansPerPredicate.entrySet())
                clusterInput.add(new ResourceWrapper(entry.getKey(), entry.getValue()));
        }

        return new OptionalClusterInputOrTemplate(clusterInput);
    }
}

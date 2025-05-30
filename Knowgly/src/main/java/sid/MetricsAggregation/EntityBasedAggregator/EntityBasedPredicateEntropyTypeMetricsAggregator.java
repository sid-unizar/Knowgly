package sid.MetricsAggregation.EntityBasedAggregator;

import sid.MetricsAggregation.VirtualDocumentTemplate;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


/**
 * An entity based metrics aggregator, using the predicateEntropyType metric
 */
public class EntityBasedPredicateEntropyTypeMetricsAggregator extends EntityBasedMetricsAggregator {
    /**
     * Basic constructor, with aggregator combination. Initializes the clustering configuration via its configuration file
     */
    public EntityBasedPredicateEntropyTypeMetricsAggregator(String entityURI,
                                                            boolean combineWithGlobalTemplate,
                                                            double combinationWeight) throws IOException {
        super(entityURI, combineWithGlobalTemplate, combinationWeight);
    }

    @Override
    public void cacheAllMetrics(LocalHDTSPARQLEndpoint endpoint, VirtualDocumentTemplate globalTemplate) throws IOException {
        super.cacheAllMetrics(endpoint, "predicateEntropyType", globalTemplate);
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        return super.getOptionalClusterInput(endpoint, forceReturnClusterInput);
    }
}

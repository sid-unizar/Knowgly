package sid.MetricsAggregation;

import org.apache.commons.math3.exception.ConvergenceException;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Dummy metrics aggregator which doesn't run KMeans and always returns the VirtualDocumentTemplate provided during
 * construction. For testing purposes.
 * <p>
 * Note: Do not use the DummyInferenceEngine(int kMeansClusters, int kMeansIterations) constructor!
 */
public class DummyMetricsAggregator extends MetricsAggregator {
    private final VirtualDocumentTemplate vdocToReturn;

    public DummyMetricsAggregator(VirtualDocumentTemplate vdocToReturn) throws IOException {
        this.vdocToReturn = vdocToReturn;
    }

    public DummyMetricsAggregator(int kMeansClusters, int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
        this.vdocToReturn = null;
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException {
        return null;
    }

    @Override
    public VirtualDocumentTemplate createVirtualDocumentTemplate(SPARQLEndpoint endpoint) throws IOException,
            ConvergenceException, ExecutionException, InterruptedException {
        return vdocToReturn;
    }
}

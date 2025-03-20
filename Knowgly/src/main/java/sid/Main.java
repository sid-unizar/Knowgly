package sid;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import sid.Connectors.Galago.GalagoConnector;
import sid.Connectors.IndexConnector;
import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.PredicateBasedAggregator.PredicateEntityTypeImportanceMetricsAggregator;
import sid.MetricsAggregation.PredicateBasedAggregator.PredicateEntropyTypeMetricsAggregator;
import sid.Pipeline.IndexingPipeline;
import sid.Pipeline.MetricsPipeline;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws IOException,
            NotFoundException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            IndexingPipeline.TypeBasedMetricsAggregatorInSPARQLException,
            ExecutionException,
            InterruptedException,
            ParserException,
            URISyntaxException {
        // Ensure a homogeneous formatting  (avoids conflicts of decimal commas when writing results to ntriples, for example)
        Locale.setDefault(Locale.US);

        /*
         * Metrics generation
         */

        MetricsPipeline metricsPipeline = MetricsPipeline.fromConfigurationFile();
        // For convenience, we join the original KG and the metrics KG when using the pipeline. We can do anything we
        // want with the endpoint at this point
        SPARQLEndpoint endpointWithKGAndMetrics = metricsPipeline.run();

        /*
         * Indexing
         */

        // Indexing with predicate-based aggregators
        MetricsAggregator aggr1 = new PredicateEntropyTypeMetricsAggregator();
        MetricsAggregator aggr2 = new PredicateEntityTypeImportanceMetricsAggregator();

        // Index entities with a template calculated using a weighted combination of both aggregator's metrics
        IndexingPipeline indexingPipeline = IndexingPipeline.fromConfigurationFile(
                // First aggregator
                aggr1,
                // Second aggregator
                aggr2,
                // Weight for the first aggregator
                0.75);

        indexingPipeline.run();

        // The index on the goal system is now ready. We can now launch individual queries as below, or
        // perform an automated evaluation using RunEvaluator.java (preferred)
        IndexConnector connector = GalagoConnector.fromConfigurationFile(); // We are using galago for this example
        connector.scoredSearch("michael schumacher",
                // We only need to indicate the number of fields and their names from now onwards
                MetricsAggregator.getEmptyVirtualDocumentTemplate(),
                // k1 (global) value for BM25F
                1.2,
                // b (global) value for BM25F
                0.5);
    }
}

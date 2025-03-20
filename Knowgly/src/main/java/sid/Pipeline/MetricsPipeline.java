package sid.Pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import sid.MetricsGeneration.HDT.ImportanceMetricsGenerator;
import sid.MetricsGeneration.HDT.InfoRankMetricsGenerator;
import sid.MetricsGeneration.MetricsGenerator;
import sid.SPARQLEndpoint.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * Pipeline class for generating metrics according to its configuration, handling internally any kind of conversion
 * and class specialization instantiations
 */
public class MetricsPipeline {
    public static final String CONFIGURATION_FILE = "configuration/metricsPipelineConfiguration.json";
    // Configuration keys
    public static final String SOURCE_CONF = "source";
    public static final String ADD_VIRTUAL_TYPES_CONF = "addVirtualTypes";
    public static final String CALCULATE_INFORANK_METRICS_CONF = "calculateInfoRankMetrics";
    public static final String INFORANK_METRICS_ALGORITHM_CONF = "inforankMetricsAlgorithm";
    public static final String CALCULATE_IMPORTANCE_METRICS_CONF = "calculateImportanceMetrics";
    public static final String IMPORTANCE_METRICS_ALGORITHM_CONF = "importanceMetricsAlgorithm";

    // Options
    public static final String HDT_OPTION = "HDT";
    public static final String RDF_OPTION = "RDF";
    public static final String EMBEDDED_SPARQL_ENDPOINT_OPTION = "EmbeddedSPARQLServerEndpoint";
    public static final String REMOTE_SPARQL_ENDPOINT_OPTION = "RemoteSPARQLEndpoint";

    private SPARQLEndpoint endpoint;
    private final boolean calculateInfoRankMetrics;
    private final boolean useRDFInInforankMetrics;
    private final boolean calculateImportanceMetrics;
    private final boolean useRDFInImportanceMetrics;
    MetricsGenerator inforankMetricsGenerator;
    MetricsGenerator importanceMetricsGenerator;


    public static MetricsPipeline fromConfigurationFile() throws IOException, NotFoundException, ParserException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        String source = rootNode.get(SOURCE_CONF).asText();

        SPARQLEndpoint endpoint = switch (source) {
            case HDT_OPTION -> LocalHDTSPARQLEndpoint.fromConfigurationFile();
            case EMBEDDED_SPARQL_ENDPOINT_OPTION -> EmbeddedSPARQLServerEndpoint.fromConfigurationFile();
            case REMOTE_SPARQL_ENDPOINT_OPTION -> RemoteSPARQLEndpoint.fromConfigurationFile();
            default -> throw new RuntimeException("Unknown option for source: " + source);
        };


        boolean calculateInfoRankMetrics = rootNode.get(CALCULATE_INFORANK_METRICS_CONF).asBoolean();
        boolean useRDFInInforankMetrics = false;
        String inforankAlgorithm = rootNode.get(INFORANK_METRICS_ALGORITHM_CONF).asText();
        MetricsGenerator inforanksMetricsGenerator = null;
        if (calculateInfoRankMetrics) {
            inforanksMetricsGenerator = switch (inforankAlgorithm) {
                case HDT_OPTION -> {
                    if (endpoint instanceof LocalHDTSPARQLEndpoint) {
                        yield InfoRankMetricsGenerator.fromConfigurationFile((LocalHDTSPARQLEndpoint) endpoint);
                    } else {
                        yield InfoRankMetricsGenerator.fromConfigurationFile(LocalHDTSPARQLEndpoint.fromConfigurationFile());
                    }
                }

                case RDF_OPTION -> {
                    useRDFInInforankMetrics = true;
                    yield sid.MetricsGeneration.SPARQL.InfoRankMetricsGenerator.fromConfigurationFile((SPARQLEndpointWithNamedGraphs) endpoint);
                }

                default ->
                        throw new RuntimeException("Unknown Inforank metrics algorithm option: " + inforankAlgorithm);
            };
        }

        boolean calculateImportanceMetrics = rootNode.get(CALCULATE_IMPORTANCE_METRICS_CONF).asBoolean();
        boolean useRDFInImportanceMetrics = false;
        String importanceMetricsAlgorithm = rootNode.get(IMPORTANCE_METRICS_ALGORITHM_CONF).asText();
        MetricsGenerator importanceMetricsGenerator = null;
        if (calculateImportanceMetrics) {
            importanceMetricsGenerator = switch (importanceMetricsAlgorithm) {
                case HDT_OPTION -> {
                    if (endpoint instanceof LocalHDTSPARQLEndpoint) {
                        yield ImportanceMetricsGenerator.fromConfigurationFile((LocalHDTSPARQLEndpoint) endpoint);
                    } else {
                        yield ImportanceMetricsGenerator.fromConfigurationFile(LocalHDTSPARQLEndpoint.fromConfigurationFile());
                    }
                }

                case RDF_OPTION -> {
                    useRDFInImportanceMetrics = true;
                    yield new sid.MetricsGeneration.SPARQL.ImportanceMetricsGenerator((SPARQLEndpointWithNamedGraphs) endpoint);
                }

                default ->
                        throw new RuntimeException("Unknown Importance metrics algorithm option: " + inforankAlgorithm);
            };
        }

        if ((useRDFInInforankMetrics || useRDFInImportanceMetrics) && endpoint instanceof LocalHDTSPARQLEndpoint)
            throw new RuntimeException("Using a local HDT endpoint as a source for RDF metrics engines is not supported.");

        return new MetricsPipeline(endpoint,
                calculateInfoRankMetrics,
                useRDFInInforankMetrics,
                calculateImportanceMetrics,
                useRDFInImportanceMetrics,
                inforanksMetricsGenerator,
                importanceMetricsGenerator,
                rootNode.get(ADD_VIRTUAL_TYPES_CONF).asBoolean());
    }

    public MetricsPipeline(SPARQLEndpoint endpoint,

                           boolean calculateInfoRankMetrics,
                           boolean useRDFInInforankMetrics,

                           boolean calculateImportanceMetrics,
                           boolean useRDFInImportanceMetrics,

                           MetricsGenerator inforankMetricsGenerator,
                           MetricsGenerator importanceMetricsGenerator,

                           boolean addVirtualTypes) throws ParserException, NotFoundException, IOException {
        this.endpoint = endpoint;

        this.calculateInfoRankMetrics = calculateInfoRankMetrics;
        this.useRDFInInforankMetrics = useRDFInInforankMetrics;

        this.calculateImportanceMetrics = calculateImportanceMetrics;
        this.useRDFInImportanceMetrics = useRDFInImportanceMetrics;

        this.inforankMetricsGenerator = inforankMetricsGenerator;
        this.importanceMetricsGenerator = importanceMetricsGenerator;

        if (addVirtualTypes) {
            System.out.println("Adding a virtual type to every subject...");
            this.endpoint.addVirtualTypes();
        }
    }

    /**
     * Run the pipeline
     * <p>
     * Warning: The indexing pipeline will instantiate the same endpoint to return internally, based on its configuration
     * files, and will query to it. Modifying it while executing thi pipeline will result in undefined behavior
     *
     * @return The original endpoint with the metrics contained inside it (In non-HDT endpoints, in a subgraph)
     * @throws IOException If there is any IO error happens when loading a SPARQL query, if used
     */
    public SPARQLEndpoint run() throws IOException {
        if (!useRDFInInforankMetrics || !useRDFInImportanceMetrics) {
            System.out.println("Deleting previous HDT metrics file...");
            // Delete previous metrics HDT files, which will cause invalid index exceptions
            if (!useRDFInInforankMetrics && inforankMetricsGenerator != null) {
                Files.deleteIfExists(Path.of(((InfoRankMetricsGenerator) inforankMetricsGenerator).getDestinationHDTFile()));
                Files.deleteIfExists(Path.of(((InfoRankMetricsGenerator) inforankMetricsGenerator).getDestinationHDTFile() + ".index.v1-1"));
            }

            if (!useRDFInImportanceMetrics && importanceMetricsGenerator != null) {
                Files.deleteIfExists(Path.of(((ImportanceMetricsGenerator) importanceMetricsGenerator).getDestinationHDTFile()));
                Files.deleteIfExists(Path.of(((ImportanceMetricsGenerator) importanceMetricsGenerator).getDestinationHDTFile() + ".index.v1-1"));
            }
        }

        // HDT endpoint, if any, to merge with the source after running all metrics engines
        LocalHDTSPARQLEndpoint HDTEndpointToMerge = null;

        if (calculateInfoRankMetrics) {
            System.out.println("Running infoRank metrics...");
            Instant start = Instant.now();
            inforankMetricsGenerator.run();
            System.out.println("Time for infoRank metrics: " + Duration.between(start, Instant.now()));

            if (!useRDFInInforankMetrics) { // Else: The metrics have been already inserted in a subgraph
                HDTEndpointToMerge = ((InfoRankMetricsGenerator) inforankMetricsGenerator).getMetricsHDT(false);
            }
        }

        if (calculateImportanceMetrics) {
            System.out.println("Running importance metrics...");
            Instant start = Instant.now();
            importanceMetricsGenerator.run();
            System.out.println("Time for importance metrics: " + Duration.between(start, Instant.now()));

            if (!useRDFInImportanceMetrics) { // Else: The metrics have been already inserted in a subgraph
                // If previous metrics generators were also HDT-based, the underlying HDT file will be the same,
                // since the metrics HDT file has been concatenated with the results of previous metrics generators
                // internally. WE only have to replace the previous reference with this one as the previous one
                // is now invalid (the file has changed)
                HDTEndpointToMerge = ((ImportanceMetricsGenerator) importanceMetricsGenerator).getMetricsHDT(false);
            }
        }

        if (HDTEndpointToMerge != null) {
            if (endpoint instanceof SPARQLEndpointWithNamedGraphs) { // RDF endpoint, add it as a subgraph
                System.out.println("Merging metrics with the KG as a subgraph...");
                ((SPARQLEndpointWithNamedGraphs) endpoint).addNamedModel(HDTEndpointToMerge.baseURI, HDTEndpointToMerge.model);
            } else { // HDT endpoint, add it by concatenating them
                System.out.println("Merging metrics with the KG file as an HDT concatenation...");
                LocalHDTSPARQLEndpoint endpointAsHDT = (LocalHDTSPARQLEndpoint) endpoint;
                // Generate an index at this point, since we will perform searches on it later when indexing
                endpoint = endpointAsHDT.concatenate(HDTEndpointToMerge, endpointAsHDT.datasetLocation, true, true);
            }
        }

        return endpoint;
    }
}

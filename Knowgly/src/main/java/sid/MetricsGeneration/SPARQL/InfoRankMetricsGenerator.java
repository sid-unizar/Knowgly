package sid.MetricsGeneration.SPARQL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import sid.MetricsGeneration.MetricsGenerator;
import sid.MetricsGeneration.PageRank.HDT.WeightedPageRankMetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.SPARQLEndpoint.LocalSPARQLEndpoint;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * SPARQL specialization for the InfoRank metrics. Not recommended for efficiency reasons,
 * use InfoRankMetricsGeneratorHDT instead.
 */
public class InfoRankMetricsGenerator implements MetricsGenerator {
    private static final String CONFIGURATION_FILE = "configuration/metricsConfiguration.json";
    public static final String METRICS_HDT_FILE_CONF = "metricsToHDTFile";

    public static final String IW = "configuration/queries/inforank_queries/iw.sparql";
    public static final String ABSC_SPARQL = "configuration/queries/inforank_queries/absC.sparql";
    public static final String ABSP_SPARQL = "configuration/queries/inforank_queries/absP.sparql";
    public static final String ABSD_SPARQL = "configuration/queries/inforank_queries/absD.sparql";
    public static final String IR_C_SPARQL = "configuration/queries/inforank_queries/irC.sparql";
    public static final String IR_P_SPARQL = "configuration/queries/inforank_queries/irP.sparql";
    public static final String IR_D_SPARQL = "configuration/queries/inforank_queries/irD.sparql";
    public static final String W_LEFT_SPARQL = "configuration/queries/inforank_queries/w_left.sparql";
    public static final String W_RIGHT_SPARQL = "configuration/queries/inforank_queries/w_right.sparql";
    public static final String INFORANK_SPARQL = "configuration/queries/inforank_queries/inforank.sparql";

    public static final String IW_URI = "http://purl.org/voc/vrank#iw";
    public static final String ABS_C_URI = "http://purl.org/voc/vrank#absC";
    public static final String ABS_D_URI = "http://purl.org/voc/vrank#absD";
    public static final String ABS_P_URI = "http://purl.org/voc/vrank#absP";
    public static final String IR_C_URI = "http://purl.org/voc/vrank#irC";
    // We make irD and irP non-distinguishable
    public static final String IR_D_URI = "http://purl.org/voc/vrank#ir";
    public static final String IR_P_URI = "http://purl.org/voc/vrank#ir";
    public static final String PAGERANK_URI = "http://purl.org/voc/vrank#pagerank";
    public static final String INFORANK_URI = "http://purl.org/voc/vrank#inforank";
    public static final String TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    private final SPARQLEndpointWithNamedGraphs endpoint;
    private WeightedPageRankMetricsGenerator pagerank;

    // HDT file to which we save the metrics prior to PageRank's computation
    private final String hdtFile;

    public static InfoRankMetricsGenerator fromConfigurationFile(SPARQLEndpointWithNamedGraphs endpoint) throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new InfoRankMetricsGenerator(endpoint,
                // Defer the creation of WeightedPageRankMetricsGeneratorHDT until the HDT
                // file is created, as it should have the same metrics input HDT file
                null,
                rootNode.get(METRICS_HDT_FILE_CONF).asText());
    }

    public InfoRankMetricsGenerator(SPARQLEndpointWithNamedGraphs endpoint,
                                    WeightedPageRankMetricsGenerator pagerank,
                                    String HDTFile) {
        this.endpoint = endpoint;
        this.pagerank = pagerank;
        this.hdtFile = HDTFile;
    }

    @Override
    public void run() {
        try {
            addInfoRankFeatures();

            if (pagerank == null) {
                System.out.println("Saving InfoRank metrics to HDT prior to PageRank's computation (This may take a while...)");
                LocalHDTSPARQLEndpoint hdtEndpoint = LocalHDTSPARQLEndpoint.fromEndpointSubgraph(endpoint,
                        BASE_INFORANK_SUBGRAPH_URI,
                        BASE_INFORANK_SUBGRAPH_URI,
                        hdtFile);

                // Instantiate it now that we have the input metrics HDT file
                pagerank = WeightedPageRankMetricsGenerator.fromConfigurationFile(endpoint);
            }

            System.out.println("Calculating PageRank...");
            pagerank.compute();
            pagerank.writePageRankScoresAsNtriples();
            System.out.println("Adding PageRank results...");
            endpoint.addNamedModel(BASE_INFORANK_SUBGRAPH_URI, new LocalSPARQLEndpoint(pagerank.getRDFOutputFile()).model); // Add PageRank results to the infoRank subgraph

            System.out.println("Calculating InfoRank...");
            endpoint.runUpdate(MetricsGenerator.getQueryString(INFORANK_SPARQL));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addInfoRankFeatures() throws IOException {
        List<Metric> metrics = Arrays.asList(
                new Metric(MetricsGenerator.getQueryString(IW), "IW(r)"),

                new Metric(MetricsGenerator.getQueryString(ABSC_SPARQL), "Absolute informativeness of each class"),
                new Metric(MetricsGenerator.getQueryString(ABSP_SPARQL), "Absolute informativeness of each object property"),
                new Metric(MetricsGenerator.getQueryString(ABSD_SPARQL), "Absolute informativeness of each datatype property"),

                new Metric(MetricsGenerator.getQueryString(IR_C_SPARQL), "IR(c) for each class"),
                new Metric(MetricsGenerator.getQueryString(IR_P_SPARQL), "IR(p) for each object property"),
                new Metric(MetricsGenerator.getQueryString(IR_D_SPARQL), "IR(p) for each data property"),
                new Metric(MetricsGenerator.getQueryString(W_LEFT_SPARQL), "W(r,p) for each data entity r and p associated to it (\"left side\")"),
                new Metric(MetricsGenerator.getQueryString(W_RIGHT_SPARQL), "W(r,p) for each data entity r and p associated to it (\"right side\")")
        );


        for (Metric metric : metrics) {
            System.out.println("Calculating " + metric.name + "...");
            endpoint.runUpdate(metric.query);
        }
    }
}

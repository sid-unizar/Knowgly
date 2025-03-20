package sid.MetricsGeneration.PageRank.SPARQL;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Abstract class all PageRank engines need to implement
 */
public abstract class PageRankMetricsGenerator {
    protected static String CONFIGURATION_FILE = "configuration/metricsConfiguration.json";
    protected static String INPUT_FILE_CONF = "inputFile";
    protected static String OUTPUT_FILE_CONF = "outputFile";
    protected static String OUTPUT_FILE_HDT_CONF = "outputFileHDT";
    protected static String DAMPING_FACTOR_CONF = "dampingFactor";
    protected static String START_VALUE_CONF = "startValue";
    protected static String NUMBER_OF_ITERATIONS_CONF = "numberOfIterations";
    protected static String CONSIDER_LITERALS_CONF = "considerLiterals";
    protected static String PARALLELIZE_CONF = "parallelize";

    public String getRDFOutputFile() {
        return RDFOutputFile;
    }

    public String getHDTOutputFile() {
        return HDTOutputFile;
    }

    protected String RDFOutputFile;
    protected String HDTOutputFile;
    protected double dampingFactor = 0.85D;
    protected double startValue = 0.1D;
    protected int numberOfIterations = 40;
    protected boolean considerLiterals = false;
    protected boolean parallelize = true;

    public PageRankMetricsGenerator(String RDFOutputFile,
                                    String HDTOutputFile) {
        this.RDFOutputFile = RDFOutputFile;
        this.HDTOutputFile = HDTOutputFile;
    }

    public PageRankMetricsGenerator(String RDFOutputFile,
                                    String outputFileHDT,
                                    double dampingFactor,
                                    double startValue,
                                    int numberOfIterations,
                                    boolean considerLiterals,
                                    boolean parallelize) {
        this.RDFOutputFile = RDFOutputFile;
        this.HDTOutputFile = outputFileHDT;
        this.dampingFactor = dampingFactor;
        this.startValue = startValue;
        this.numberOfIterations = numberOfIterations;
        this.considerLiterals = considerLiterals;
        this.parallelize = parallelize;
    }

    class PageRankScore {
        public String node;
        public Double pageRank;
    }

    public abstract void compute() throws ExecutionException, InterruptedException;

    public abstract void writePageRankScoresAsNtriples() throws FileNotFoundException;

    // Replace strange characters in URIs
    protected String sanitizeURI(String uri) {
        return uri
                .replace("<", "")
                .replace(">", "");
    }

    // Returns a LocalHDTSPARQLEndpoint containing the PageRank results
    public abstract LocalHDTSPARQLEndpoint writePageRankScoresHDT() throws IOException, ParserException, NotFoundException;
}
package sid.MetricsGeneration;

import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.TripleID;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * MetricsGenerator interface that all generators must implement
 * <p>
 * Contains several helper method for loading SPARQL queries extracting data from literal strings
 */
public interface MetricsGenerator {

    String BASE_IMPORTANCE_SUBGRAPH_URI = "http://sid-unizar-search.com/importance/facts";
    String BASE_IMPORTANCE_METRIC_URI = "http://sid-unizar-search.com/importance";
    String BASE_INFORANK_SUBGRAPH_URI = "http://sid-unizar-search.com/infoRank";

    class Metric {
        public String query;
        public String name;

        public Metric(String query, String name) {
            this.query = query;
            this.name = name;
        }
    }

    void run();

    static String getQueryString(String queryFile) throws IOException {
        Path factFrequencyQueryPath = Path.of(queryFile);
        return Files.readString(factFrequencyQueryPath);
    }

    static String longToIntLiteral(long l) {
        return "\"" + l + "\"^^<http://www.w3.org/2001/XMLSchema#integer>";
    }

    static String doubleToFloatLiteral(double d) {
        return "\"" + String.format(Locale.US, "%.10f", d) + "\"^^<http://www.w3.org/2001/XMLSchema#float>";
    }

    static long intLiteralToLong(HDT metricsHDT, TripleID iwTriple) {
        String literalString = metricsHDT.getDictionary().idToString(iwTriple.getObject(), TripleComponentRole.OBJECT).toString();
        String splitLiteralString = literalString.split("\\^")[0]; // Remove the literal type suffix
        splitLiteralString = splitLiteralString.substring(1, splitLiteralString.length() - 1); // Remove surrounding '"'s


        return Long.parseLong(splitLiteralString);
    }

    static double floatLiteralToDouble(HDT metricsHDT, TripleID iwTriple) {
        String literalString = metricsHDT.getDictionary().idToString(iwTriple.getObject(), TripleComponentRole.OBJECT).toString();
        String splitLiteralString = literalString.split("\\^")[0]; // Remove the literal type suffix
        splitLiteralString = splitLiteralString.substring(1, splitLiteralString.length() - 1); // Remove surrounding '"'s


        return Double.parseDouble(splitLiteralString);
    }
}

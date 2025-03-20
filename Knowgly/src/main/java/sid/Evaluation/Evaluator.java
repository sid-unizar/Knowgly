package sid.Evaluation;

import sid.Connectors.IndexConnector;
import sid.Connectors.ScoredSearchResult;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Evaluation helper class, which outputs search results to valid TREC files for their evaluation
 * <p>
 * It can allow writing prefixed URIs in the results file (by default, it will do it for DBPedia resources)
 */
public class Evaluator {
    public static final String DBPEDIA_RESOURCE_PREFIX = "http://dbpedia.org/resource/";
    // Query ID -> Query text
    private Map<String, String> queries;
    private final String queriesFile;
    // Map of URI -> prefix substitution to use when writing results to file
    private final Map<String, String> prefixes;
    private final IndexConnector connector;

    public Evaluator(String queriesFile, IndexConnector connector) {
        queries = new HashMap<>();
        this.queriesFile = queriesFile;
        this.connector = connector;

        this.prefixes = new HashMap<>();
        this.prefixes.put(DBPEDIA_RESOURCE_PREFIX, "dbpedia:");
    }

    /**
     * Writes the results for each query as a TREC-compliant results file, so that an external evaluator (trec_eval...)
     * can be used.
     * <p>
     * From trec_eval's documentation:
     * <p>
     * -----------------------
     * Results_file format: Standard 'trec_results'
     * <p>
     * Lines of results_file are of the form
     * 030  Q0  ZF08-175-870  0   4238   prise1
     * qid iter   docno      rank  sim   run_id
     * giving TREC document numbers (a string) retrieved by query qid
     * (a string) with similarity sim (a float).  The other fields are ignored,
     * with the exception that the run_id field of the last line is kept and
     * output.  In particular, note that the rank field is ignored here;
     * internally ranks are assigned by sorting by the sim field with ties
     * broken deterministicly (using docno).
     * Sim is assumed to be higher for the docs to be retrieved first.
     * File may contain no NULL characters.
     * Lines may contain fields after the run_id; they are ignored.
     * <p>
     * -----------------------
     * Rel_info_file format: Standard 'qrels'
     * <p>
     * Relevance for each docno to qid is determined from rel_info_file, which
     * consists of text tuples of the form
     * qid  iter  docno  rel
     * giving TREC document numbers (docno, a string) and their relevance (rel,
     * a non-negative integer less than 128, or -1 (unjudged))
     * to query qid (a string).  iter string field is ignored.
     * Fields are separated by whitespace, string fields can contain no whitespace.
     * File may contain no NULL characters.
     *
     * @param fileName File to which the TREC results will be written
     * @param truncate Whether to truncate the file or not
     * @param template A VirtualDocumentTemplate containing references to this index's fields. Can be empty and easily generated
     *                 from a previously saved JSON file or from the inference configuration with
     * @param runID    TREC runID to use
     * @throws IOException          If any IO errors happen when loading queries or writing the results
     * @throws ExecutionException   If any exception occurs during the (threaded) query execution
     * @throws InterruptedException If any interruption occurs during the (threaded) query execution
     */
    public void runQueriesAndSaveAsTRECResultsFile(String fileName,
                                                   boolean truncate,
                                                   VirtualDocumentTemplate template,
                                                   String runID,
                                                   double k1,
                                                   double b) throws IOException, ExecutionException, InterruptedException {

        System.out.println("Loading queries...");
        loadQueries(queriesFile);

        BufferedWriter output = new BufferedWriter(new FileWriter(fileName, !truncate));
        String iter = "Q0";
        if (runID == null || runID.isEmpty()) runID = "Knowgly";

        Map<String, List<ScoredSearchResult>> resultsList = connector.scoredSearch(queries, template, k1, b);

        writeResultsToFile(runID, output, iter, resultsList);
    }

    private void loadQueries(String file) {
        queries.clear();

        try {
            FileInputStream stream = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                queries.put(parts[0], parts[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String replacePrefixesInURI(String URI) {
        for (var entry : prefixes.entrySet()) {
            if (URI.contains(entry.getKey()))
                return URI.replace(entry.getKey(), entry.getValue());
        }

        return URI;
    }

    private void writeResultsToFile(String runID, BufferedWriter output, String iter, Map<String, List<ScoredSearchResult>> resultsList) throws IOException {
        for (var results : resultsList.entrySet()) {
            int rank = 0;
            for (ScoredSearchResult result : results.getValue()) {
                output.append(results.getKey() + " " + iter + " <" + replacePrefixesInURI(result.URI) + "> " + rank + " " + result.score + " " + runID);
                output.newLine();

                rank = rank + 1;
            }
        }

        output.close();
    }
}

package sid.Connectors.Galago;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import sid.Connectors.EntityDocument;
import sid.Connectors.IndexConnector;
import sid.Connectors.ScoredSearchResult;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static sid.MetricsAggregation.MetricsAggregator.*;

/**
 * Connector for Galago.
 * Limitations:
 * <p>
 * - In comparison to other connectors, indexing with galago is a 2-step process.
 *   This java executable will create trectexts for every entity during its own "indexing" phase. Afterwards, the actual
 *   galago index can be created with build_galago_index.sh in the main Knowgly's folder
 * <p>
 * - Searching is also a 2-step process, although it has been integrated in this java executable, and will automatically
 *   call the run_queries_galago.sh script. As it will already create a .run file, it skips the EntitySearcher phase
 *   entirely
 */
public class GalagoConnector extends IndexConnector {
    private static final String GALAGO_INDEX_PATH = "./galago_index";
    private static final String GALAGO_DOCUMENTS_PATH = GALAGO_INDEX_PATH + "/trectexts";
    public static final String GALAGO_INDEX_CONFIG_FILE = GALAGO_INDEX_PATH + "/params.json";
    public static final String GALAGO_SEARCHER_CONFIG_FILE = "evaluation/queries_galago.json";
    public static final String GALAGO_SEARCH_SCRIPT = "./run_queries_galago.sh";

    private static final String PARAMS_JSON = """
            {
              "inputPath" : "galago_index/trectexts",
              "indexPath" : "galago_index/idx",
              "tokenizer" : {
                "fields" : [%s]
              },
              "nonStemmedPostings" : true
            }
            """;
    private static final String FIELD_IN_TRECTEXT = """
            <%s>%s</%s>
            """;

    private static final String SINGLE_DOC_TRECTEXT = """
            <DOC>
            <DOCNO>%s</DOCNO>
            <TEXT>
            %s
            </TEXT>
            </DOC>
            """;

    private static final String SEARCH_CONFIG_BM25_JSON = """
            {
                "verbose": true,
                "casefold": true,
                "requested": 1000,
                "index": "galago_index/idx",
                "K": %s,
                "b": %s,
                "queries": [
                    %s
                ]
            }
            """;

    // Gotchas:
    //  - It allows us to set per-field smoothing (b) parameters (default b value in Galago: 0.5, in elasticsearch: 0.75)
    //       - We set the same smoothing value for all fields, we only want to adjust the weights
    //  - The general b parameter is ignored, it always applies per-field smoothing (as above)
    //  - The general k1 parameter in Galago has a default value of 0.5, we set it to 1.2 as in elasticserach
    private static final String SEARCH_CONFIG_BM25F_JSON = """
            {
                "verbose": true,
                "casefold": true,
                "requested": 1000,
                "index": "galago_index/idx",
                "traversals": [
                    {
                        "name": "org.lemurproject.galago.contrib.retrieval.traversal.BM25FTraversal",
                        "order": "before"
                    }
                ],
                "fields": [
                    %s
                ],
                "bm25f": {
                    "K": %s,
                    "b": %s,
                    "weights": {
                        %s
                    },
                    "smoothing": {
                        %s
                    }
                },
                "queries": [
                    %s
                ]
            } 
            """;

    private static final String SEARCH_CONFIG_FSDM_JSON = """
            {
                "verbose": true,
                "casefold": true,
                "requested": 1000,
                "index": "galago_index/idx",
                "traversals": [
                    {
                        "name": "org.lemurproject.galago.contrib.retrieval.traversal.FieldedSequentialDependenceTraversal",
                        "order": "before"
                    }
                ],
                "fields": [
                    %s
                ],
                "queries": [
                    %s
                ],
                %s
            } 
            """;
    private static final String FSDM_UNI_WEIGHT = "\"uni-%s\": %s";
    private static final String FSDM_OD_WEIGHT = "\"od-%s\": %s";
    private static final String FSDM_UWW_WEIGHT = "\"uww-%s\": %s";

    private static final String SINGLE_QUERY_BM25_JSON = """
            {
                "number": "%s",
                "text": "#bm25(%s)"
            }
            """;

    private static final String SINGLE_QUERY_BM25F_JSON = """
            {
                "number": "%s",
                "text": "#combine(%s)"
            }
            """;

    private static final String SINGLE_QUERY_TERM_BM25F_JSON = "#bm25f(%s)";
    private static final String SINGLE_QUERY_TERM_BM25_JSON = "#bm25(%s)";
    // Using #od inside #bm25f completely breaks it, returning nothing for most queries are spurious results for the rest
    // We instead to phrase queries, which seem to work
    private static final String SINGLE_QUERY_TERM_BM25F_BIGRAM_JSON = "#bm25f(%s)";
    // In this case, #od inside #bm25 works correctly. Using phrase queries returns worse results
    private static final String SINGLE_QUERY_TERM_BM25_BIGRAM_JSON = "#bm25(#od(%s))";

    private static final String SINGLE_QUERY_FSDM_JSON = """
            {
                "number": "%s",
                "text": "#fieldedsdm(%s)"
            }
            """;

    private static final String ENDPOINT_CONFIGURATION_FILE = "configuration/galagoEndpointConfiguration.json";
    private static final String CREATE_INDEX_CONF = "createIndex";

    private final BufferedWriter docWriter;

    // Entity indexing
    private final Deque<EntityDocument> documentsToIndex;
    private final ReentrantLock lock;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static GalagoConnector fromConfigurationFile() throws IOException, URISyntaxException {
        byte[] mapDataEndpoint = Files.readAllBytes(Paths.get(ENDPOINT_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNodeEndpoint = objectMapper.readTree(mapDataEndpoint);

        return new GalagoConnector(rootNodeEndpoint.get(CREATE_INDEX_CONF).asBoolean());
    }

    /**
     * Create a GalagoConnector using the configuration files.
     *
     * @param createIndex   Whether to attempt to create the index or not (will fail silently if it exists)
     * @throws IOException                 If there is any IO errors when reading the configuration files when creating the index
     * @throws java.net.URISyntaxException If the server's address URL cannot be parsed
     */
    private GalagoConnector(boolean createIndex) throws IOException, URISyntaxException {
        super();

        this.docWriter = new BufferedWriter(new FileWriter(GALAGO_DOCUMENTS_PATH + "/documents.trectext"));

        if (createIndex) {
            createIndex(fieldName, numberOfFields, createTypesOverrideField, createRelationsFields);
        }

        this.documentsToIndex = new ConcurrentLinkedDeque<>();
        this.lock = new ReentrantLock();
    }

    public void createIndex(String fieldName, int numberOfFields, boolean createTypesOverrideField, boolean createRelationsFields) throws IOException {
        FileUtils.cleanDirectory(new File(GALAGO_INDEX_PATH));
        new File(GALAGO_DOCUMENTS_PATH).mkdirs(); // Recreate the trectexts folder

        // Format and add each field to a temporary buffer
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < numberOfFields; i++) {
            if (divideDataTypeAndObjectProperties) {
                fields.add(fieldName + i + DATATYPE_PROPERTIES_SUFFIX);
                fields.add(fieldName + i + OBJECT_PROPERTIES_SUFFIX);
            } else
                fields.add(fieldName + i);

            if (createRelationsFields) {
                fields.add(RELATIONS_FIELD_NAME + i);
            }
        }

        if (createTypesOverrideField) {
            fields.add(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_NAME);
        }

        StringBuilder fieldsString = new StringBuilder();
        var fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            fieldsString.append("\"").append(fieldsIterator.next()).append("\"");
            if (fieldsIterator.hasNext())
                fieldsString.append(",");
        }

        String params = PARAMS_JSON.formatted(fieldsString.toString());

        Files.writeString(Path.of(GALAGO_INDEX_CONFIG_FILE), params);
    }

    /**
     * Delete every document inside the index
     *
     * @throws IOException If there is any IO errors when reading the configuration files for the index
     */
    public void clearIndex() throws IOException {
        createIndex(fieldName, numberOfFields, createTypesOverrideField, createRelationsFields);
    }

    public void indexDocuments(Deque<EntityDocument> documentsToIndex) throws IOException {
        for (EntityDocument doc : documentsToIndex) {
            indexDocument(doc);
        }
    }

    /**
     * Note: Assumes mutual exclusion when writing to the docs file (meaning, it has to be called from a
     * GalagoEntityIndexer instance)
     */
    public void indexDocument(EntityDocument doc) throws IOException {
        StringBuilder fieldsText = new StringBuilder();
        for (var entry : doc.fields.entrySet()) {
            if (entry.getKey().equals(TYPES_FIELD_NAME)) {
                StringBuilder typesFieldContent = new StringBuilder();

                for (String s : doc.types) {
                    typesFieldContent.append(s).append(' ');
                }

                if (!typesFieldContent.isEmpty())
                    typesFieldContent.deleteCharAt(typesFieldContent.length() - 1);

                fieldsText.append(FIELD_IN_TRECTEXT.formatted(TYPES_FIELD_NAME, typesFieldContent.toString(), TYPES_FIELD_NAME));
            } else {
                StringBuilder fieldContents = new StringBuilder();

                for (String s : entry.getValue()) {
                    fieldContents.append(s).append(' ');
                }

                if (!fieldContents.isEmpty())
                    fieldContents.deleteCharAt(fieldContents.length() - 1);

                fieldsText.append(FIELD_IN_TRECTEXT.formatted(entry.getKey(), fieldContents.toString(), entry.getKey()));
            }
        }

        docWriter.write(SINGLE_DOC_TRECTEXT.formatted(doc.getEntityName(), fieldsText.toString()));
    }

    public void closeIndex() throws IOException {
        docWriter.close();
    }

    /**
     * Search with galago's default BM25 query. This already creates a .run file, so it's not integrated into an
     * EntitySearcher
     */
    public void searchWithBM25(Map<String, String> queries, String fileName) throws IOException, InterruptedException {
        generateBM25SearchConfig(queries,
                // Default k1 value in elasticsearch (galago also assigns it 0.5, which is strange)
                1.2,
                // Default b value in galago
                0.5);
        callSearchScript(fileName);
    }

    /**
     * Search with galago's default BM25 query. This already creates a .run file, so it's not integrated into an
     * EntitySearcher
     * <p>
     * Allows modifying the k1 and b parameters
     */
    public void searchWithBM25(Map<String, String> queries,
                               double k1,
                               double b,
                               String fileName) throws IOException, InterruptedException {
        generateBM25SearchConfig(queries, k1, b);
        callSearchScript(fileName);
    }

    /**
     * Search with galago's contrib BM25F query. This already creates a .run file, so it's not integrated into an
     * EntitySearcher
     */
    public void searchWithBM25F(Map<String, String> queries,
                                VirtualDocumentTemplate t,
                                String fileName) throws IOException, InterruptedException {
        generateBM25FSearchConfig(queries, t,
                // Default k1 value in elasticsearch (galago also assigns it 0.5, which is strange)
                1.2,
                // Default b value in galago
                0.5);
        callSearchScript(fileName);
    }

    /**
     * Search with galago's contrib BM25F query. This already creates a .run file, so it's not integrated into an
     * EntitySearcher
     * <p>
     * Allows modifying the k1 and b parameters
     */
    public void searchWithBM25F(Map<String, String> queries,
                                VirtualDocumentTemplate t,
                                double k1,
                                double b,
                                String fileName) throws IOException, InterruptedException {
        generateBM25FSearchConfig(queries, t, k1, b);
        callSearchScript(fileName);
    }

    public void searchWithFSDM(Map<String, String> queries,
                               VirtualDocumentTemplate t,
                               String fileName) throws IOException, InterruptedException {
        Map<String, Double> uniWeights = new HashMap<>();
        Map<String, Double> odWeights = new HashMap<>();
        Map<String, Double> uwwWeights = new HashMap<>();

        for (Field f : t.fields) {
            uniWeights.put(f.name, 0.8 * f.weight);
            odWeights.put(f.name, 0.2 * f.weight);
            uwwWeights.put(f.name, 0.2 * f.weight);
        }


        generateFSDMSearchConfig(queries, t, uniWeights, odWeights, uwwWeights);
        callSearchScript(fileName);
    }

    public void searchWithFSDM(Map<String, String> queries,
                               VirtualDocumentTemplate t,
                               double uni,
                               double od,
                               double uww, String fileName) throws IOException, InterruptedException {
        Map<String, Double> uniWeights = new HashMap<>();
        Map<String, Double> odWeights = new HashMap<>();
        Map<String, Double> uwwWeights = new HashMap<>();

        for (Field f : t.fields) {
            uniWeights.put(f.name, uni * f.weight);
            odWeights.put(f.name, od * f.weight);
            uwwWeights.put(f.name, uww * f.weight);
        }


        generateFSDMSearchConfig(queries, t, uniWeights, odWeights, uwwWeights);
        callSearchScript(fileName);
    }

    public void searchWithFSDM(Map<String, String> queries, VirtualDocumentTemplate t,
                               Map<String, Double> uniWeights,
                               Map<String, Double> odWeights,
                               Map<String, Double> uwwWeights,
                               String fileName) throws IOException, InterruptedException {
        generateFSDMSearchConfig(queries, t, uniWeights, odWeights, uwwWeights);
        callSearchScript(fileName);
    }

    private void generateBM25SearchConfig(Map<String, String> queries,
                                          double k1,
                                          double b) throws IOException {
        String k1String = String.format("%.4f", k1);
        String bString = String.format("%.4f", b);
        StringBuilder queriesString = new StringBuilder();

        var queriesIterator = queries.entrySet().iterator();
        while (queriesIterator.hasNext()) {
            var q = queriesIterator.next();

            StringBuilder combineQuery = new StringBuilder();

            String[] queryTerms = q.getValue().split(" ");
            for (String queryTerm : queryTerms)
                if (!queryTerm.isEmpty())
                    combineQuery.append(SINGLE_QUERY_TERM_BM25_JSON.formatted(queryTerm)).append(" ");

            queriesString.append(SINGLE_QUERY_BM25F_JSON.formatted(q.getKey(), combineQuery));

            if (queriesIterator.hasNext())
                queriesString.append(",");
        }


        String searchConfig = SEARCH_CONFIG_BM25_JSON.formatted(k1String, bString, queriesString);

        BufferedWriter searchConfigWriter = new BufferedWriter(new FileWriter(GALAGO_SEARCHER_CONFIG_FILE));
        searchConfigWriter.write(searchConfig);
        searchConfigWriter.close();
    }

    private void generateBM25BigramsSearchConfig(Map<String, String> queries, double k1, double b) throws IOException {
        String k1String = String.format("%.4f", k1);
        String bString = String.format("%.4f", b);
        StringBuilder queriesString = new StringBuilder();

        var queriesIterator = queries.entrySet().iterator();
        while (queriesIterator.hasNext()) {
            var q = queriesIterator.next();

            StringBuilder combineQuery = new StringBuilder();

            String[] queryTerms = q.getValue().split(" ");
            for (String queryTerm : queryTerms)
                if (!queryTerm.isEmpty())
                    combineQuery.append(SINGLE_QUERY_TERM_BM25_JSON.formatted(queryTerm)).append(" ");

            for (String bigram : getBigrams(q.getValue())) {
                combineQuery.append(SINGLE_QUERY_TERM_BM25_BIGRAM_JSON.formatted(bigram)).append(" ");
            }

            queriesString.append(SINGLE_QUERY_BM25F_JSON.formatted(q.getKey(), combineQuery));

            if (queriesIterator.hasNext())
                queriesString.append(",");
        }


        String searchConfig = SEARCH_CONFIG_BM25_JSON.formatted(k1String, bString, queriesString);

        BufferedWriter searchConfigWriter = new BufferedWriter(new FileWriter(GALAGO_SEARCHER_CONFIG_FILE));
        searchConfigWriter.write(searchConfig);
        searchConfigWriter.close();
    }

    /**
     * Generates a params file for galago's searcher, indicating the fields and BM25F parameters (weights, k1, b...)
     */
    private void generateBM25FSearchConfig(Map<String, String> queries, VirtualDocumentTemplate t, double k1, double b) throws IOException {
        String k1String = String.format("%.4f", k1);
        String bString = String.format("%.4f", b);
        StringBuilder fieldsString = new StringBuilder();
        StringBuilder fieldWeightsString = new StringBuilder();
        StringBuilder fieldSmoothingsString = new StringBuilder();
        StringBuilder queriesString = new StringBuilder();

        var fieldsIterator = t.fields.iterator();
        while (fieldsIterator.hasNext()) {
            Field f = fieldsIterator.next();

            fieldsString.append("\"").append(f.name).append("\"");
            if (fieldsIterator.hasNext())
                fieldsString.append(",");

            fieldWeightsString.append("\"").append(f.name).append("\":").append(String.format("%.4f", f.weight));
            if (fieldsIterator.hasNext())
                fieldWeightsString.append(",");

            fieldSmoothingsString.append("\"").append(f.name).append("\":").append(String.format("%.4f", b));
            if (fieldsIterator.hasNext())
                fieldSmoothingsString.append(",");
        }

        var queriesIterator = queries.entrySet().iterator();
        while (queriesIterator.hasNext()) {
            var q = queriesIterator.next();
            StringBuilder combineQuery = new StringBuilder();

            String[] queryTerms = q.getValue().split(" ");
            for (String queryTerm : queryTerms)
                if (!queryTerm.isEmpty())
                    combineQuery.append(SINGLE_QUERY_TERM_BM25F_JSON.formatted(queryTerm)).append(" ");

            queriesString.append(SINGLE_QUERY_BM25F_JSON.formatted(q.getKey(), combineQuery));

            if (queriesIterator.hasNext())
                queriesString.append(",");
        }


        String searchConfig = SEARCH_CONFIG_BM25F_JSON.formatted(fieldsString, k1String, bString, fieldWeightsString, fieldSmoothingsString, queriesString);

        BufferedWriter searchConfigWriter = new BufferedWriter(new FileWriter(GALAGO_SEARCHER_CONFIG_FILE));
        searchConfigWriter.write(searchConfig);
        searchConfigWriter.close();
    }

    private void generateBM25FBigramsSearchConfig(Map<String, String> queries, VirtualDocumentTemplate t, double k1, double b) throws IOException {
        String k1String = String.format("%.4f", k1);
        String bString = String.format("%.4f", b);
        StringBuilder fieldsString = new StringBuilder();
        StringBuilder fieldWeightsString = new StringBuilder();
        StringBuilder fieldSmoothingsString = new StringBuilder();
        StringBuilder queriesString = new StringBuilder();

        var fieldsIterator = t.fields.iterator();
        while (fieldsIterator.hasNext()) {
            Field f = fieldsIterator.next();

            fieldsString.append("\"").append(f.name).append("\"");
            if (fieldsIterator.hasNext())
                fieldsString.append(",");

            fieldWeightsString.append("\"").append(f.name).append("\":").append(String.format("%.4f", f.weight));
            if (fieldsIterator.hasNext())
                fieldWeightsString.append(",");

            fieldSmoothingsString.append("\"").append(f.name).append("\":").append(String.format("%.4f", b));
            if (fieldsIterator.hasNext())
                fieldSmoothingsString.append(",");
        }

        var queriesIterator = queries.entrySet().iterator();
        while (queriesIterator.hasNext()) {
            var q = queriesIterator.next();
            StringBuilder combineQuery = new StringBuilder();

            String[] queryTerms = q.getValue().split(" ");
            for (String queryTerm : queryTerms)
                if (!queryTerm.isEmpty())
                    combineQuery.append(SINGLE_QUERY_TERM_BM25F_JSON.formatted(queryTerm)).append(" ");

            for (String bigram : getBigrams(q.getValue())) {
                combineQuery.append(SINGLE_QUERY_TERM_BM25F_BIGRAM_JSON.formatted(bigram)).append(" ");
            }

            queriesString.append(SINGLE_QUERY_BM25F_JSON.formatted(q.getKey(), combineQuery));

            if (queriesIterator.hasNext())
                queriesString.append(",");
        }


        String searchConfig = SEARCH_CONFIG_BM25F_JSON.formatted(fieldsString, k1String, bString, fieldWeightsString, fieldSmoothingsString, queriesString);

        BufferedWriter searchConfigWriter = new BufferedWriter(new FileWriter(GALAGO_SEARCHER_CONFIG_FILE));
        searchConfigWriter.write(searchConfig);
        searchConfigWriter.close();
    }

    private void generateFSDMSearchConfig(Map<String, String> queries,
                                          VirtualDocumentTemplate t,
                                          Map<String, Double> uniWeights,
                                          Map<String, Double> odWeights,
                                          Map<String, Double> uwwWeights) throws IOException {
        StringBuilder fieldsString = new StringBuilder();
        StringBuilder queriesString = new StringBuilder();
        StringBuilder weightsString = new StringBuilder();

        var fieldsIterator = t.fields.iterator();
        while (fieldsIterator.hasNext()) {
            Field f = fieldsIterator.next();

            fieldsString.append("\"").append(f.name).append("\"");
            if (fieldsIterator.hasNext())
                fieldsString.append(",");
        }

        var queriesIterator = queries.entrySet().iterator();
        while (queriesIterator.hasNext()) {
            var q = queriesIterator.next();

            queriesString.append(SINGLE_QUERY_FSDM_JSON.formatted(q.getKey(), q.getValue()));

            if (queriesIterator.hasNext())
                queriesString.append(",");
        }


        for (Iterator<Map.Entry<String, Double>> it = uniWeights.entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            weightsString.append(FSDM_UNI_WEIGHT.formatted(entry.getKey(), String.format("%.4f", entry.getValue())));
            weightsString.append(",\n");
        }

        for (Iterator<Map.Entry<String, Double>> it = odWeights.entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            weightsString.append(FSDM_OD_WEIGHT.formatted(entry.getKey(), String.format("%.4f", entry.getValue())));
            weightsString.append(",\n");
        }

        for (Iterator<Map.Entry<String, Double>> it = uwwWeights.entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            weightsString.append(FSDM_UWW_WEIGHT.formatted(entry.getKey(), String.format("%.4f", entry.getValue())));

            if (it.hasNext())
                weightsString.append(",\n");
            else
                weightsString.append("\n");
        }

        String searchConfig = SEARCH_CONFIG_FSDM_JSON.formatted(fieldsString, queriesString, weightsString);

        BufferedWriter searchConfigWriter = new BufferedWriter(new FileWriter(GALAGO_SEARCHER_CONFIG_FILE));
        searchConfigWriter.write(searchConfig);
        searchConfigWriter.close();
    }

    private static void callSearchScript(String outputFilename) throws IOException, InterruptedException {
        ProcessBuilder searchScriptProcessBuilder = new ProcessBuilder("bash", GALAGO_SEARCH_SCRIPT, outputFilename);
        searchScriptProcessBuilder.redirectErrorStream(true);
        Process searchScriptProcess = searchScriptProcessBuilder.start();

        InputStream inputStream = searchScriptProcess.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        searchScriptProcess.waitFor();
    }

    private static List<String> getBigrams(String query) {
        String[] words = query.split("\\s+");
        List<String> bigrams = new ArrayList<>();

        for (int i = 0; i < words.length - 1; i++) {
            String pair = words[i] + " " + words[i + 1];
            bigrams.add(pair);
        }

        return bigrams;
    }

    @Override
    public void addDocumentToIndex(EntityDocument d) throws IOException {
        lock.lock();

        try {
            indexDocument(d);

        } finally {
            lock.unlock();
        }
    }

    private void doBulkUpdate() throws IOException {
        lock.lock();

        try {
            if (!documentsToIndex.isEmpty()) {
                indexDocuments(documentsToIndex);
                documentsToIndex.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void finishIndexing() throws IOException, InterruptedException {
        doBulkUpdate();
        closeIndex();
    }

    @Override
    public List<ScoredSearchResult> scoredSearch(String query,
                                                 VirtualDocumentTemplate template,
                                                 double k1,
                                                 double b) {
        return null;
    }

    @Override
    public Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                              VirtualDocumentTemplate template,
                                                              double k1,
                                                              double b) {
        try {
            searchWithBM25F(queries, template, k1, b, "run_galago.run");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

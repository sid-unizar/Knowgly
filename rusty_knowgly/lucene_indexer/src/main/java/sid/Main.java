package sid;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;


/**
 * Connector for Lucene
 * <p>
 * Limitations: BM25F field weights must be > 1.0
 */
public class Main {
    public static final String LUCENE_INDEX_PATH = "./lucene_index";
    public static final String FIELD_BASE_NAME = "field_";

    private static IndexWriter indexWriter;
    private static DirectoryReader ireader; // Shared reader for all (parallel) searchers

    private final ObjectMapper mapper = new ObjectMapper();

    public static final String DBPEDIA_RESOURCE_PREFIX = "http://dbpedia.org/resource/";
    private final Map<String, String> prefixes;

    /**
     * Abstraction of a retrieval result from any system, composed of an URI and a numerical score
     * Allows ordering via its score
     */
    public static class ScoredSearchResult implements Comparable<ScoredSearchResult> {
        public String URI;
        public double score;

        public ScoredSearchResult(String URI, double score) {
            this.URI = URI;
            this.score = score;
        }

        @Override
        public int compareTo(ScoredSearchResult other) {
            return Double.compare(score, other.score);
        }
    }

    /**
     * Private constructor
     *
     * @throws IOException If there are any IO errors when reading/writing to Lucene's index
     */
    private Main() throws IOException {
        super();

        // Only created if there is an index inside
        createDirectoryReader();

        // Always created even if we are not going to index, will be
        // closed once any query is launched
        createIndexWriter();

        this.prefixes = new HashMap<>();
        this.prefixes.put(DBPEDIA_RESOURCE_PREFIX, "dbpedia:");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }

        try {
            Main app = new Main();
            String command = args[0].toLowerCase();

            switch (command) {
                case "clear-index" -> {
                    app.clearIndex();
                }
                case "index" -> {
                    if (args.length < 2) {
                        System.out.println("Please provide a .jsonl file to index.");
                    } else {
                        java.util.concurrent.atomic.LongAdder count = new java.util.concurrent.atomic.LongAdder();

                        try (java.util.stream.Stream<String> lines = java.nio.file.Files.lines(java.nio.file.Paths.get(args[1]))) {
                            lines.parallel()
                                    .filter(line -> !line.isBlank())
                                    .forEach(line -> {
                                        try {
                                            app.indexJsonLine(line);
                                            count.increment();

                                            long currentCount = count.sum();
                                            if (currentCount % 1000 == 0) {
                                                System.out.println("Indexed " + currentCount + " entities...");
                                            }
                                        } catch (Exception e) {
                                            System.err.println("Failed to index line: " + e.getMessage());
                                        }
                                    });
                            System.out.println("Indexing finished. Total entities indexed: " + count.sum());
                        } catch (java.io.IOException e) {
                            System.err.println("Error: Error when indexing file " + args[1] + ": " + e.getMessage());
                        }
                    }
                }
                case "search" -> {
                    if (args.length < 7) {
                        System.out.println("Error: Provide a search query.");
                    } else {
                        Map<String, String> queries = app.loadQueries(args[1]);
                        double baseWeight = Double.parseDouble(args[2]);
                        int n_fields = Integer.parseInt(args[3]);
                        double k1 = Double.parseDouble(args[4]);
                        double b = Double.parseDouble(args[5]);
                        int n_results = Integer.parseInt(args[6]);
                        String output_file = args[7];

                        var resultsList = app.scoredSearch(queries, baseWeight, n_fields, k1, b, n_results);
                        BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_file));
                        app.writeResultsToFile(writer, resultsList);
                    }
                }
                case "clear" -> {
                    app.clearIndex();
                    System.out.println("Index cleared.");
                }
                default -> printUsage();
            }
            app.closeIndex();
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

    private void writeResultsToFile(BufferedWriter output,
                                    Map<String, List<ScoredSearchResult>> resultsList) throws IOException {
        for (var results : resultsList.entrySet()) {
            int rank = 0;
            for (ScoredSearchResult result : results.getValue()) {
                output.append(results.getKey() + " Q0 <" + replacePrefixesInURI(result.URI) + "> " + rank + " " + result.score + " Knowgly_2.0");
                output.newLine();

                rank = rank + 1;
            }
        }

        output.close();
    }

    /**
     * Loads queries from a .tsv file
     */
    private Map<String, String> loadQueries(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundException("Queries .tsv file not found at: " + path);
        }

        Map<String, String> queries = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length >= 2) {
                    queries.put(columns[0], columns[1]);
                }
            }
        }
        return queries;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  index \"<json_file>\" - Adds the VDocs contained in the .jsonl file to the index");
        System.out.println("  search \"<queries_file>\" \"<base_weight>\" \"<n_fields>\" \"<k1>\" \"<b>\" \"<n_results>\" \"<output_file>\" - Searches the index");
        System.out.println("  clear - Deletes the entire index");
    }

    private static void createDirectoryReader() throws IOException {
        Path indexPath = Paths.get(LUCENE_INDEX_PATH);
        if (Files.list(indexPath).findAny().isPresent()) {
            ireader = DirectoryReader.open(FSDirectory.open(indexPath));
        }
    }

    private void createIndexWriter() throws IOException {
        Path indexPath = Paths.get(LUCENE_INDEX_PATH);
        if (!Files.exists(indexPath))
            indexPath = Files.createDirectory(indexPath);

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        sid.Main.indexWriter = new IndexWriter(FSDirectory.open(indexPath), config);
    }

    /**
     * Delete every document inside the index
     *
     * @throws IOException If there are any IO errors when reading/writing to Lucene's index
     */
    public void clearIndex() throws IOException {
        if (sid.Main.indexWriter != null)
            sid.Main.indexWriter.close();

        if (sid.Main.ireader != null)
            sid.Main.ireader.close();

        IOUtils.rm(Paths.get(LUCENE_INDEX_PATH));
        createIndexWriter();
    }

    protected void closeIndex() throws IOException {
        sid.Main.indexWriter.close();

        if (sid.Main.ireader != null)
            sid.Main.ireader.close();
    }

    /**
     * Index an entity's document. Thread-safe and buffered by Lucene itself.
     *
     * @param jsonLine Entity's VDoc, in JSON format
     * @throws IOException IOException If there are any IO errors when reading/writing to Lucene's index
     */
     protected void indexJsonLine(String jsonLine) throws IOException {
         JsonNode root = mapper.readTree(jsonLine);
         Document luceneDoc = new Document();

         String entityIri = root.get("entity_iri").asString();
         luceneDoc.add(new StoredField("URI", entityIri));

         JsonNode fields = root.get("fields");
         if (fields != null && fields.isArray()) {
             for (int i = 0; i < fields.size(); i++) {
                 JsonNode cluster = fields.get(i);
                 JsonNode predicateTexts = cluster.get("predicate_texts");

                 StringBuilder clusterContent = new StringBuilder();

                 for (JsonNode predicateGroup : predicateTexts) {
                     // Predicate's label or lexicalized IRI
                     JsonNode predicateInfo = predicateGroup.get(0);
                     if (predicateInfo != null && predicateInfo.size() > 1) {
                         String predIRI = predicateInfo.get(1).asText();
                         clusterContent.append(predIRI).append(' ');
                     }

                     // Object labels or lexicalized IRIs
                     JsonNode objects = predicateGroup.get(1);
                     if (objects != null && objects.isArray()) {
                         for (JsonNode obj : objects) {
                             String objText = obj.asText();
                             clusterContent.append(objText).append(' ');
                         }
                     }
                 }

                 // Index the entire cluster as a single Lucene field
                 if (!clusterContent.isEmpty()) {
                     String fieldName = FIELD_BASE_NAME + i;
                     luceneDoc.add(new Field(fieldName, clusterContent.toString().trim(), TextField.TYPE_NOT_STORED));
                 }
             }
         }

         indexWriter.addDocument(luceneDoc);
     }

    public void finishIndexing() throws IOException {
        closeIndex();
        createDirectoryReader(); // Allow querying it now
    }

    private Query buildBM25FQuery(String query,
                                  Map<String, Double> fieldWeights) throws IOException {
        var bm25fQueryBuilder = new CombinedFieldQuery.Builder();

        // Apply the analyzer to the query (split, lowercase filter...)
        TokenStream tokenStream = new StandardAnalyzer().tokenStream(null, new StringReader(query));

        // OR (sum of scores) of BM25F queries
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        try (tokenStream) {
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            // For each term, build a BM25F query and add it to the booleanQuery
            while (tokenStream.incrementToken()) {
                bm25fQueryBuilder.addTerm(new BytesRef(charTermAttribute.toString()));

                for (Map.Entry<String, Double> entry : fieldWeights.entrySet()) {
                    bm25fQueryBuilder.addField(entry.getKey(), entry.getValue().floatValue());
                }

                booleanQueryBuilder.add(bm25fQueryBuilder.build(), BooleanClause.Occur.SHOULD);

                bm25fQueryBuilder = new CombinedFieldQuery.Builder();
            }

            tokenStream.end();
        }

        return booleanQueryBuilder.build();
    }

    private List<ScoredSearchResult> runLuceneQuery(Query q,
                                                    double k1,
                                                    double b,
                                                    int n_results) throws IOException {
        List<ScoredSearchResult> results = new ArrayList<>();

        //DirectoryReader ireader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(ireader); //Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));

        // Set k1 and b for this query
        searcher.setSimilarity(new BM25Similarity((float) k1, (float) b));

        TopDocs topDocs = searcher.search(q, n_results);
        StoredFields storedFields = searcher.storedFields();

        for (ScoreDoc hit : topDocs.scoreDocs) {
            Document doc = storedFields.document(hit.doc);
            results.add(new ScoredSearchResult(doc.get("URI"), hit.score));
        }

        return results;
    }

    public List<ScoredSearchResult> scoredSearch(String query,
                                                 double baseWeight,
                                                 int n_fields,
                                                 double k1,
                                                 double b,
                                                 int n_results) throws IOException {
        sid.Main.indexWriter.close(); // We cannot write to the index anymore

        Map<String, Double> fieldWeights = new HashMap<>();
        for (int i = 0; i < n_fields; i++) {
            // Example for 3 fields:
            // field_0 -> 3.0
            // field_1 -> 2.0
            // field_2 -> 1.0
            fieldWeights.put("field_"+i, baseWeight*(n_fields - i));
        }
        var booleanQuery = buildBM25FQuery(query, fieldWeights);
        return runLuceneQuery(booleanQuery, k1, b, n_results);
    }

    public Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                              double baseWeight,
                                                              int n_fields,
                                                              double k1,
                                                              double b,
                                                              int n_results) throws IOException, ExecutionException, InterruptedException {
        sid.Main.indexWriter.close(); // We cannot write to the index anymore

        Map<String, Double> fieldWeights = new HashMap<>();
        for (int i = 0; i < n_fields; i++) {
            // Example for 3 fields:
            // field_0 -> 3.0
            // field_1 -> 2.0
            // field_2 -> 1.0
            fieldWeights.put("field_"+i, baseWeight*(n_fields - i));
        }

        ConcurrentHashMap<String, List<ScoredSearchResult>> results = new ConcurrentHashMap<>();

        List<Future<Object>> futures;
        try (ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            futures = queries.entrySet().stream()
                    .map(entry -> executor.submit(() -> {
                        String qid = entry.getKey();
                        String q = entry.getValue();

                        try {
                            Query booleanQuery = buildBM25FQuery(q, fieldWeights);
                            results.put(qid, runLuceneQuery(booleanQuery, k1, b, n_results));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return null;
                    }))
                    .toList();
        }

        java.util.concurrent.atomic.LongAdder completedCount = new java.util.concurrent.atomic.LongAdder();
        int totalTasks = futures.size();

        for (Future<Object> future : futures) {
            try {
                future.get();
                completedCount.increment();

                long current = completedCount.sum();
                System.out.print("\rProgress: " + current + "/" + totalTasks + " queries run...");
            } catch (ExecutionException e) {
                System.err.println("Query failed: " + e.getCause().getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return results;
    }
}

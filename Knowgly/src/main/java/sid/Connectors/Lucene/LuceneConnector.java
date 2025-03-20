package sid.Connectors.Lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import sid.Connectors.EntityDocument;
import sid.Connectors.IndexConnector;
import sid.Connectors.ScoredSearchResult;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Connector for Lucene
 * <p>
 * Limitations: BM25F field weights must be > 1.0
 */
public class LuceneConnector extends IndexConnector {
    public static final String LUCENE_INDEX_PATH = "./lucene_index";

    private static final String ENDPOINT_CONFIGURATION_FILE = "configuration/luceneEndpointConfiguration.json";

    private static final String CREATE_INDEX_CONF = "createIndex";

    private static IndexWriter indexWriter;
    private static DirectoryReader ireader; // Shared reader for all (parallel) searchers

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static LuceneConnector fromConfigurationFile() throws IOException, URISyntaxException {
        byte[] mapDataEndpoint = Files.readAllBytes(Paths.get(ENDPOINT_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNodeEndpoint = objectMapper.readTree(mapDataEndpoint);

        return new LuceneConnector(rootNodeEndpoint.get(CREATE_INDEX_CONF).asBoolean());
    }


    /**
     * Create a LuceneConnector using the configuration files.
     *
     * @param createIndex Whether to attempt to create the index or not (will fail silently if it exists)
     * @throws IOException If there is any IO errors when reading the configuration files when creating the index
     */
    private LuceneConnector(boolean createIndex) throws IOException, URISyntaxException {
        super();

        // Only created if there is an index inside
        createDirectoryReader();
        // Always created even if we are not going to index, will be
        // closed once any query is launched
        createIndexWriter();
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

        LuceneConnector.indexWriter = new IndexWriter(FSDirectory.open(indexPath), config);
    }

    /**
     * Delete every document inside the index
     *
     * @throws IOException If there is any IO errors when reading the configuration files for the index
     */
    public void clearIndex() throws IOException {
        if (LuceneConnector.indexWriter != null)
            LuceneConnector.indexWriter.close();

        if (LuceneConnector.ireader != null)
            LuceneConnector.ireader.close();

        IOUtils.rm(Paths.get(LUCENE_INDEX_PATH));
        createIndexWriter();
    }

    protected void closeIndex() throws IOException {
        LuceneConnector.indexWriter.close();

        if (LuceneConnector.ireader != null)
            LuceneConnector.ireader.close();
    }

    /**
     * Index an entity's document. Thread-safe and buffered by Lucene itself.
     *
     * @param entityDoc Entity's document
     * @throws IOException If there is an error writing the document from Lucene's side
     */
    protected void indexDocument(EntityDocument entityDoc) throws IOException {
        Document luceneDoc = new Document();
        // StoredField is used for storing metadata for summary results, we use the
        // entity's URI in this case
        luceneDoc.add(new StoredField("URI", entityDoc.getEntityName()));

        for (var entry : entityDoc.fields.entrySet()) {
            StringBuilder fieldContents = new StringBuilder();

            for (String s : entry.getValue()) {
                fieldContents.append(s).append(' ');
            }

            if (!fieldContents.isEmpty())
                fieldContents.deleteCharAt(fieldContents.length() - 1);

            luceneDoc.add(new Field(entry.getKey(), fieldContents, TextField.TYPE_NOT_STORED));
        }

        indexWriter.addDocument(luceneDoc);
    }

    @Override
    public void addDocumentToIndex(EntityDocument d) throws IOException {
        indexDocument(d);
    }

    @Override
    public void finishIndexing() throws IOException {
        closeIndex();
        createDirectoryReader(); // Allow querying it now
    }

    private Query buildBM25FQuery(String query,
                                  VirtualDocumentTemplate template) throws IOException {
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
                for (sid.MetricsAggregation.Field f : template.fields) {
                    bm25fQueryBuilder.addField(f.name, (float) f.weight);
                }

                booleanQueryBuilder.add(bm25fQueryBuilder.build(), BooleanClause.Occur.SHOULD);

                bm25fQueryBuilder = new CombinedFieldQuery.Builder();
            }

            tokenStream.end();
        }

        return booleanQueryBuilder.build();
    }

    private List<ScoredSearchResult> runLuceneQuery(Query q, double k1, double b) throws IOException {
        List<ScoredSearchResult> results = new ArrayList<>();

        //DirectoryReader ireader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(ireader); //Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));

        // Set k1 and b for this query
        searcher.setSimilarity(new BM25Similarity((float) k1, (float) b));

        TopDocs topDocs = searcher.search(q, this.maxNumberOfResults);
        StoredFields storedFields = searcher.storedFields();

        for (ScoreDoc hit : topDocs.scoreDocs) {
            Document doc = storedFields.document(hit.doc);
            results.add(new ScoredSearchResult(doc.get("URI"), hit.score));
        }

        return results;
    }

    @Override
    public List<ScoredSearchResult> scoredSearch(String query,
                                                 VirtualDocumentTemplate template,
                                                 double k1,
                                                 double b) throws IOException {
        LuceneConnector.indexWriter.close(); // We cannot write to the index anymore

        var booleanQuery = buildBM25FQuery(query, template);
        return runLuceneQuery(booleanQuery, k1, b);
    }

    @Override
    public Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                              VirtualDocumentTemplate template,
                                                              double k1,
                                                              double b) throws IOException, ExecutionException, InterruptedException {
        LuceneConnector.indexWriter.close(); // We cannot write to the index anymore

        ConcurrentHashMap<String, List<ScoredSearchResult>> results = new ConcurrentHashMap<>();

        List<Future<Object>> futures;
        try (ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            futures = queries.entrySet().stream()
                    .map(entry -> executor.submit(() -> {
                        String qid = entry.getKey();
                        String q = entry.getValue();
                        try {
                            Query booleanQuery = buildBM25FQuery(q, template);
                            results.put(qid, runLuceneQuery(booleanQuery, k1, b));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return null;
                    }))
                    .toList();
        }

        for (Future<Object> future : futures) {
            future.get();
        }

        return results;
    }
}

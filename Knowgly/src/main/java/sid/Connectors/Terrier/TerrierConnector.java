package sid.Connectors.Terrier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import sid.Connectors.EntityDocument;
import sid.Connectors.IndexConnector;
import sid.Connectors.ScoredSearchResult;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

import static sid.MetricsAggregation.MetricsAggregator.*;

/**
 * Connector for pyTerrier. Depending on the task, it requires indexer.py or searcher.py to be running and accepting
 * requests. The java executable will send requests in a similar way to elastic's connector
 * <p>
 * Limitations: k1 and b, although exposed, are ignored due to pyTerrier not properly allowing to tune them
 */
public class TerrierConnector extends IndexConnector {
    private static final String INDEX_DOCS_MESSAGE = """
            {
                "action": "index_docs",
                "docs": %s
            }""";

    private static final String SINGLE_DOC_JSON = """
            {"docno": %s, %s}""";

    private static final String CLEAR_INDEX_MESSAGE = """
            {
                "action": "clear_index"
            }""";

    private static final String CREATE_INDEX_MESSAGE = """
            {
                "action": "create_index",
                "fields": %s
            }""";

    private static final String FINISH_INDEXING_MESSAGE = """
            {
                "action": "finish_indexing"
            }""";

    private static final String SEARCH_MESSAGE = """
            {
                "action": %s,
                "query": %s,
                "fields": %s,
                "weights": %s
            }""";

    private static final String SEARCH_MESSAGE_BULK = """
            {
                "action": %s,
                "queries": %s,
                "fields": %s,
                "weights": %s
            }""";

    private static final String ENDPOINT_CONFIGURATION_FILE = "configuration/terrierEndpointConfiguration.json";
    private static final String SERVER_ADDRESS_CONF = "serverAddress";
    private static final String CREATE_INDEX_CONF = "createIndex";

    private final URL serverAddress;
    private final ObjectMapper objectMapper;

    // Entity indexing
    final Deque<EntityDocument> documentsToIndex;
    private final static int BULK_UPDATE_SIZE = 1000;
    private final ReentrantLock lock;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static TerrierConnector fromConfigurationFile() throws IOException, URISyntaxException {
        byte[] mapDataEndpoint = Files.readAllBytes(Paths.get(ENDPOINT_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNodeEndpoint = objectMapper.readTree(mapDataEndpoint);

        return new TerrierConnector(rootNodeEndpoint.get(SERVER_ADDRESS_CONF).asText(),
                rootNodeEndpoint.get(CREATE_INDEX_CONF).asBoolean());
    }

    /**
     * Create a TerrierConnector  using the configuration files.
     *
     * @param serverAddress URL for pyTerrier's server
     * @param createIndex   Whether to attempt to create the index or not (will fail silently if it exists)
     * @throws IOException If there is any IO errors when reading the configuration files when creating the index
     */
    // If createIndex == true, it will create an index based on the indexDefinition (index general configuration,
    // catchAll fields, etc.) and indexFieldDefinition (definition of a single field).
    //
    // The first one needs to have a %s formatting specifier inside mappings.properties.fields.properties, where it will
    // create as many fields as specified following the indexFieldDefinition template, which needs to have its name as
    // an %s formatting specifier, enclosed in double quotation marks. See the provided example files for a working example.
    private TerrierConnector(String serverAddress,
                             boolean createIndex) throws IOException, URISyntaxException {
        super();

        this.serverAddress = new URI(serverAddress).toURL();
        this.objectMapper = new ObjectMapper();

        if (createIndex) {
            createIndex(fieldName, numberOfFields, createTypesOverrideField, createRelationsFields);
        }

        this.documentsToIndex = new ConcurrentLinkedDeque<>();
        this.lock = new ReentrantLock();
    }

    public void createIndex(String fieldName, int numberOfFields, boolean createTypesOverrideField, boolean createRelationsFields) throws IOException {
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

        // fields.add("catchAll"); // Not needed

        sendJSON(CREATE_INDEX_MESSAGE.formatted(objectMapper.writeValueAsString(fields)));
    }

    /**
     * Delete every document inside the index, by dropping and recreating it
     *
     * @throws IOException If there is any IO errors when reading the configuration files for the index
     */
    public void clearIndex() throws IOException {
        sendJSON(CLEAR_INDEX_MESSAGE);

        createIndex(fieldName, numberOfFields, createTypesOverrideField, createRelationsFields);
    }

    public void indexDocuments(Deque<EntityDocument> documentsToIndex) throws IOException {
        String docsJSON = "[";

        var docsToIndexIter = documentsToIndex.iterator();
        while (docsToIndexIter.hasNext()) {
            var doc = docsToIndexIter.next();
            String docJSON = objectMapper.writeValueAsString(doc);
            docJSON = docJSON.substring(1, docJSON.length() - 1);

            docsJSON = docsJSON.concat(SINGLE_DOC_JSON.formatted("\"" + doc.getEntityName() + "\"", docJSON));
            if (docsToIndexIter.hasNext()) docsJSON = docsJSON.concat(",");
            else docsJSON = docsJSON.concat("]");
        }

        sendJSON(INDEX_DOCS_MESSAGE.formatted(docsJSON));
    }

    public List<ScoredSearchResult> searchWithBM25(String query) throws IOException {
        String resp = sendJSON(SEARCH_MESSAGE.formatted("\"bm25\"", "\"" + query + "\"", "\"\"", "\"\""));
        return getResultsResponse(resp);
    }

    public Map<String, List<ScoredSearchResult>> searchWithBM25Bulk(Map<String, String> queries) throws IOException {
        String queriesString = "[";

        var queriesIter = queries.entrySet().iterator();
        while (queriesIter.hasNext()) {
            var query = queriesIter.next();

            String queryString = "[\"" + query.getKey() + "\",\"" + query.getValue() + "\"]";

            if (queriesIter.hasNext()) queriesString = queriesString.concat(queryString + ",");
            else queriesString = queriesString.concat(queryString + "]");
        }


        String resp = sendJSON(SEARCH_MESSAGE_BULK.formatted("\"bm25_bulk\"", queriesString, "\"\"", "\"\""));
        return getBulkResultsResponse(resp);
    }

    public List<ScoredSearchResult> searchWithBM25F(String query, List<Field> fields) throws IOException {
        String fieldsString = "[";

        var fieldsIter = fields.iterator();
        while (fieldsIter.hasNext()) {
            String fieldStr = fieldsIter.next().name;
            fieldsString = fieldsString.concat("\"" + fieldStr + "\"");

            if (fieldsIter.hasNext()) fieldsString = fieldsString.concat(",");
            else fieldsString = fieldsString.concat("]");
        }

        String weightsString = "[";

        var weightsIter = fields.iterator();
        while (weightsIter.hasNext()) {
            double w = weightsIter.next().weight;
            weightsString = weightsString.concat(String.format("%.4f", w));

            if (weightsIter.hasNext()) weightsString = weightsString.concat(",");
            else weightsString = weightsString.concat("]");
        }


        String resp = sendJSON(SEARCH_MESSAGE.formatted("\"bm25f\"", "\"" + query + "\"", fieldsString, weightsString));
        return getResultsResponse(resp);
    }

    public Map<String, List<ScoredSearchResult>> searchWithBM25FBulk(Map<String, String> queries, List<Field> fields) throws IOException {
        String fieldsString = "[";

        var fieldsIter = fields.iterator();
        while (fieldsIter.hasNext()) {
            String fieldStr = fieldsIter.next().name;
            fieldsString = fieldsString.concat("\"" + fieldStr + "\"");

            if (fieldsIter.hasNext()) fieldsString = fieldsString.concat(",");
            else fieldsString = fieldsString.concat("]");
        }

        String weightsString = "[";

        var weightsIter = fields.iterator();
        while (weightsIter.hasNext()) {
            double w = weightsIter.next().weight;
            weightsString = weightsString.concat(String.format("%.4f", w));

            if (weightsIter.hasNext()) weightsString = weightsString.concat(",");
            else weightsString = weightsString.concat("]");
        }

        String queriesString = "[";

        var queriesIter = queries.entrySet().iterator();
        while (queriesIter.hasNext()) {
            var query = queriesIter.next();

            String queryString = "[\"" + query.getKey() + "\",\"" + query.getValue() + "\"]";

            if (queriesIter.hasNext()) queriesString = queriesString.concat(queryString + ",");
            else queriesString = queriesString.concat(queryString + "]");
        }


        String resp = sendJSON(SEARCH_MESSAGE_BULK.formatted("\"bm25f_bulk\"", queriesString, fieldsString, weightsString));
        return getBulkResultsResponse(resp);
    }

    private String sendJSON(String json) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");


        DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());
        outputStream.write(json.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        outputStream.close();

        if (connection.getResponseCode() != 200)
            throw new IOException("Received a non-200 response code when sending the following request: \n" + json);

        InputStream inputStream = connection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder responseBuffer = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            responseBuffer.append(line);
        }
        reader.close();
        inputStream.close();

        connection.disconnect();

        return responseBuffer.toString();
    }

    public void closeIndex() throws IOException {
        sendJSON(FINISH_INDEXING_MESSAGE);
    }

    private static List<ScoredSearchResult> getResultsResponse(String resp) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(resp);

        List<String> uris = new ObjectMapper().convertValue(rootNode.get("results"), new TypeReference<List<String>>() {
        });
        List<Double> scores = new ObjectMapper().convertValue(rootNode.get("scores"), new TypeReference<List<Double>>() {
        });

        List<ScoredSearchResult> results = new ArrayList<>();
        for (int i = 0; i < uris.size(); i++) {
            results.add(new ScoredSearchResult(uris.get(i), scores.get(i)));
        }

        return results;
    }

    private static Map<String, List<ScoredSearchResult>> getBulkResultsResponse(String resp) throws JsonProcessingException {
        Map<String, List<ScoredSearchResult>> resultsMap = new HashMap<>();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode results = objectMapper.readTree(resp).get("results");

        if (results != null && results.isObject()) {
            results.fields().forEachRemaining(resultsForQID -> {
                String QID = resultsForQID.getKey();

                List<ScoredSearchResult> scoredSearchResults = new ArrayList<>();

                for (JsonNode result : resultsForQID.getValue()) {
                    scoredSearchResults.add(new ScoredSearchResult(result.get(0).asText(), result.get(1).asDouble()));
                }

                resultsMap.put(QID, scoredSearchResults);
            });
        }

        return resultsMap;
    }

    @Override
    public void addDocumentToIndex(EntityDocument d) throws IOException {
        lock.lock();

        try {
            documentsToIndex.add(d);

            if (documentsToIndex.size() >= BULK_UPDATE_SIZE)
                doBulkUpdate();

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
    public void finishIndexing() throws IOException {
        doBulkUpdate();
        closeIndex();
    }

    @Override
    public List<ScoredSearchResult> scoredSearch(String query,
                                                 VirtualDocumentTemplate template,
                                                 double k1,
                                                 double b) throws IOException {
        return searchWithBM25F(query, template.fields);
    }

    @Override
    public Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                              VirtualDocumentTemplate t,
                                                              double k1,
                                                              double b) throws IOException {
        return searchWithBM25Bulk(queries);
    }
}

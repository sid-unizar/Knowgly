package sid.Connectors.Elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.CombinedFieldsOperator;
import co.elastic.clients.elasticsearch._types.query_dsl.CombinedFieldsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import sid.Connectors.EntityDocument;
import sid.Connectors.IndexConnector;
import sid.Connectors.ScoredSearchResult;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

import static sid.MetricsAggregation.MetricsAggregator.*;

/**
 * Connector implementation for elastic
 * <p>
 * Requires a user, password and the server's key fingerprint
 * for authentication, alongside the host and port of the server
 * and the index the documents will be stored in
 * <p>
 * Limitations:
 * <p>
 * - BM25F field weights must be > 1.0
 * <p>
 * - k1 and b, although available, are ignored due to elastic requiring to close the index, so it requires manual
 * intervention for now.
 * //TODO look for a way to automate this. We can close -> change the default similarity -> open,
 * //TODO but then we should assume nobody else is using the index
 */
public class ElasticConnector extends IndexConnector {
    private static final String ENDPOINT_CONFIGURATION_FILE = "configuration/elasticEndpointConfiguration.json";
    private static final String PASSWORD_CONF = "password";
    private static final String ENDPOINT_CONF = "endpoint";
    private static final String CERTIFICATE_FINGERPRINT_CONF = "certificateFingerprint";
    private static final String ENDPOINT_PORT_CONF = "endpointPort";
    private static final String ELASTIC_INDEX_NAME_CONF = "elasticIndexName";
    private static final String USER_CONF = "user";
    private static final String CREATE_INDEX_CONF = "createIndex";

    private static final String INDEX_DEFINITION_FILE = "configuration/indexDefinition.json";
    private static final String INDEX_FIELD_DEFINITION_FILE = "configuration/indexFieldDefinition.json";
    private static final String ENTITY_LINKING_FIELD_DEFINITION_FILE = "configuration/entityLinkingFieldDefinition.json";

    private final String indexName;

    private static ElasticsearchClient client = null;

    // Entity indexing
    final Deque<EntityDocument> documentsToIndex;
    final static int BULK_UPDATE_SIZE = 1000; // Elastic bulk requests should be smaller than 5 MiB
    private final ReentrantLock lock;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static ElasticConnector fromConfigurationFile() throws IOException {
        byte[] mapDataEndpoint = Files.readAllBytes(Paths.get(ENDPOINT_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNodeEndpoint = objectMapper.readTree(mapDataEndpoint);

        return new ElasticConnector(rootNodeEndpoint.get(USER_CONF).asText(),
                rootNodeEndpoint.get(PASSWORD_CONF).asText(),
                rootNodeEndpoint.get(ENDPOINT_CONF).asText(),
                rootNodeEndpoint.get(CERTIFICATE_FINGERPRINT_CONF).asText(),
                rootNodeEndpoint.get(ENDPOINT_PORT_CONF).asInt(),
                rootNodeEndpoint.get(ELASTIC_INDEX_NAME_CONF).asText(),
                rootNodeEndpoint.get(CREATE_INDEX_CONF).asBoolean());
    }

    /**
     * Create an ElasticConnector using the configuration files.
     *
     * @param user        The elastic user
     * @param password    The elastic user's password
     * @param host        The elastic server's address (without the port)
     * @param fingerprint The elastic server's certificate fingerprint
     * @param port        The elastic server's port
     * @param indexName   The elastic index where all operations will be done
     * @param createIndex Whether to attempt to create the index or not (will fail silently if it exists)
     * @throws IOException If there is any IO errors when reading the configuration files when creating the index
     */
    // If createIndex == true, it will create an index based on the indexDefinition (index general configuration,
    // catchAll fields, etc.) and indexFieldDefinition (definition of a single field).
    //
    // The first one needs to have a %s formatting specifier inside mappings.properties.fields.properties, where it will
    // create as many fields as specified following the indexFieldDefinition template, which needs to have its name as
    // an %s formatting specifier, enclosed in double quotation marks. See the provided example files for a working example.
    private ElasticConnector(String user,
                             String password,
                             String host,
                             String fingerprint,
                             int port,
                             String indexName,
                             boolean createIndex) throws IOException {
        super();
        this.indexName = indexName;

        SSLContext sslContext = TransportUtils.sslContextFromCaFingerprint(fingerprint);

        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(user, password)
        );

        RestClient restClient = RestClient
                .builder(new HttpHost(host, port, "https"))
                .setHttpClientConfigCallback(hc -> hc
                                .setSSLContext(sslContext)
                                .setDefaultCredentialsProvider(credsProv)
                        // To avoid possible timeout problems (not happening right now)
                        // https://github.com/elastic/elasticsearch/issues/65213
                        //.setKeepAliveStrategy((response, context) -> 300000/* 5 minutes*/)
                        //.setDefaultIOReactorConfig(IOReactorConfig.custom().setSoKeepAlive(true).build())
                )
                .build();

        // Create the transport and the API client
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        // Refresh the client's connector, ignoring whether it's already initialized or not
        client = new ElasticsearchClient(transport);

        if (createIndex) {
            createIndex(indexName);
        }

        this.documentsToIndex = new ConcurrentLinkedDeque<>();
        this.lock = new ReentrantLock();
    }

    private void createIndex(String indexName) throws IOException {
        // Elastic's dynamic mapping can automagically decide that something that passes a date format test is a Date
        // instead of a String, which will make indexing fail when a field both contains a String and a Date
        //
        // As a workaround, we forcefully disable date detection. All numeric values will be treated as Strings
        String indexCreationJSON = Files.readString(Path.of(INDEX_DEFINITION_FILE));
        String indexFieldJSON = Files.readString(Path.of(INDEX_FIELD_DEFINITION_FILE));
        String entityLinkingFieldJSON = Files.readString(Path.of(ENTITY_LINKING_FIELD_DEFINITION_FILE));

        // Format and add each field to a temporary buffer
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < numberOfFields; i++) {
            if (divideDataTypeAndObjectProperties) {
                fields.append(indexFieldJSON.formatted(fieldName + i + DATATYPE_PROPERTIES_SUFFIX));
                fields.append(",");
                fields.append(indexFieldJSON.formatted(fieldName + i + OBJECT_PROPERTIES_SUFFIX));
            } else
                fields.append(indexFieldJSON.formatted(fieldName + i));

            if (createRelationsFields) {
                fields.append(",");
                fields.append(entityLinkingFieldJSON.formatted(RELATIONS_FIELD_NAME + i));
            }

            if (i + 1 < numberOfFields) fields.append(",");
        }

        if (createTypesOverrideField) {
            fields.append(",");
            fields.append(indexFieldJSON.formatted(TYPE_PREDICATES_OVERRIDE_FIELD_WEIGHT_NAME));
        }

        CreateIndexRequest req = CreateIndexRequest.of(b -> b
                .index(indexName)
                // Create the index formatting the index template with the fields
                .withJson(new StringReader(indexCreationJSON.formatted(fields)))
        );

        // It will fail silently later if it was already created and didn't have it disabled
        try {
            client.indices().create(req);
        } catch (Exception e) {
            System.err.println("Warning: index creation failed (does it already exist?) \n" +
                    "Warning: If date detection was not disabled for dynamic mapping, it may fail when indexing documents");
        }
    }

    /**
     * Delete every document inside the index, by dropping and recreating it
     *
     * @throws IOException If there is any IO errors when reading the configuration files for the index
     */
    public void clearIndex() throws IOException {
        System.out.println("Deleting index...");

        // Drop it
        DeleteIndexRequest req = DeleteIndexRequest.of(b -> b
                .index(indexName)
        );

        try {
            client.indices().delete(req);
        } catch (Exception e) {
            System.err.println("Warning: failed to delete index! (does it exist?)");
        }

        createIndex(indexName);
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
                BulkRequest.Builder br = new BulkRequest.Builder();

                while (!documentsToIndex.isEmpty()) {
                    EntityDocument d = documentsToIndex.pop();
                    br.operations(op -> op
                            .index(idx -> idx
                                    .index(indexName)
                                    .id(d.getEntityName())
                                    .document(d)
                            )
                    );
                }

                /*
                 * Posible failures:
                 *   - Date detection is enabled: It will attempt to create fields both containing Strings and Dates and fail (see above)
                 *   - The entity name is empty: It will attempt to do an update (PUT) instead of an insertion (POST), and fail
                 * */
                var result = client.bulk(br.build());

                // Log errors, if any
                if (result.errors()) {
                    String errors = "";
                    for (BulkResponseItem item : result.items()) {
                        if (item.error() != null) {
                            errors = errors.concat(" " + item.error().reason());
                            System.err.println("Error when indexing entityDocument: " + item.error().reason());
                        }
                    }

                    throw new RuntimeException("Error when doing bulk indexing! \n" + errors);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void finishIndexing() throws IOException, InterruptedException {
        doBulkUpdate();
        // No need for anything else
    }

    @Override
    public List<ScoredSearchResult> scoredSearch(String query,
                                                 VirtualDocumentTemplate template,
                                                 double k1,
                                                 double b) throws IOException {
        return getBM25FScoredElasticResults(query, template, k1, b);
    }

    @Override
    public Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                              VirtualDocumentTemplate template,
                                                              double k1,
                                                              double b) throws IOException {
        Map<String, List<ScoredSearchResult>> results = new HashMap<>();

        for (var entry : queries.entrySet()) {
            results.put(entry.getKey(), scoredSearch(entry.getValue(), template, k1, b));
        }

        return results;
    }

    /**
     * Run a Lucene BM25F query (CombinedFieldsQuery in elastic)
     */
    private List<Hit<EntityDocument>> executeElasticBM25FQuery(String query,
                                                               VirtualDocumentTemplate template,
                                                               double k1,
                                                               double b) throws IOException {
        // Do a query over all main fields, without using any subfield
        List<String> fields = new ArrayList<>();

        for (Field f : template.fields) {
            if (f.weight != 0.0 && !f.isForEntityLinking) // Else: Omit the field
                fields.add("fields." + f.name + "^" + f.weight);
        }


        Query BM25FQuery = new CombinedFieldsQuery.Builder()
                .query(query)
                .fields(fields)
                .operator(CombinedFieldsOperator.Or)
                .build()._toQuery();

        // To visualize the JSON queries and requests:
        //System.out.println(BM25FQuery);
        System.out.println(new SearchRequest.Builder().index(indexName)
                .query(BM25FQuery).build().toString());

        SearchResponse<EntityDocument> response = client.search(s -> s
                        .index(indexName)
                        .query(BM25FQuery)
                        .size(maxNumberOfResults)
                        .timeout("10000ms"),
                EntityDocument.class
        );

        return response.hits().hits();
    }

    private List<ScoredSearchResult> getBM25FScoredElasticResults(String query,
                                                                  VirtualDocumentTemplate template,
                                                                  double k1,
                                                                  double b) {
        List<ScoredSearchResult> results = new ArrayList<>();

        try {
            for (Hit<EntityDocument> hit : executeElasticBM25FQuery(query, template, k1, b)) { // Using inference
                results.add(new ScoredSearchResult(hit.id(), hit.score()));
            }
        } catch (Exception e) {
            System.err.println("Warning: ElasticSearch query failed (is the endpoint online?)");
            e.printStackTrace();
        }

        return results;
    }
}

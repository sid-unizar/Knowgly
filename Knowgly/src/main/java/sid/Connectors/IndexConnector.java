package sid.Connectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import sid.EntityExtractor.EntityExtractorConfiguration;
import sid.EntityExtractor.ExtractedEntity;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.VirtualDocumentTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static sid.MetricsAggregation.MetricsAggregator.*;

/**
 * A connector for indexing and retrieval on a specific system, to be instanced via:
 * - ElasticConnector
 * - GalagoConnector
 * - TerrierConnector
 * - LuceneConnector
 */
public abstract class IndexConnector {
    // Fields data
    private static final String METRICS_AGGREGATOR_CONFIGURATION_FILE = "configuration/metricsAggregatorConfiguration.json";
    private static final String CLUSTERS_CONF = "buckets";

    private static final String INDEX_CONFIGURATION_FILE = "configuration/indexConfiguration.json";
    private static final String FIELD_NAME_CONF = "fieldName";
    private static final String SUBFIELDS_CONF = "subfields";

    protected static final String TYPES_FIELD_NAME = "typesField";
    protected final String fieldName;
    protected final int numberOfFields;
    protected final boolean divideDataTypeAndObjectProperties;
    protected final boolean createTypesOverrideField;
    protected final boolean createRelationsFields;
    public final List<String> subfields;

    // Entity indexer
    protected static final String ENTITY_INDEXER_CONFIGURATION_FILE = "configuration/entityIndexerConfiguration.json";
    protected static final String ADD_PREDICATE_NAMES_CONF = "addPredicateNames";
    protected static final String PREDICATE_NAME_PREFIXES_CONF = "predicateNamePrefixes";

    protected final boolean addPredicateNames;
    protected final List<String> predicateNamePrefixes;

    // Entity searcher
    protected static final String ENTITY_SEARCHER_CONFIGURATION_FILE = "configuration/entitySearcherConfiguration.json";
    protected static final String FORBIDDEN_SUBFIELDS_CONF = "forbiddenSubfields";
    protected static final String MAX_NUMBER_OF_RESULTS_CONF = "maxNumberOfResults";

    protected final List<String> forbiddenSubfields;
    protected final int maxNumberOfResults;

    protected IndexConnector() throws IOException {
        byte[] mapDataIndex = Files.readAllBytes(Paths.get(INDEX_CONFIGURATION_FILE));
        byte[] mapDataAggregator = Files.readAllBytes(Paths.get(METRICS_AGGREGATOR_CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode rootNodeIndex = objectMapper.readTree(mapDataIndex);
        JsonNode rootNodeAggregator = objectMapper.readTree(mapDataAggregator);

        Set<String> typePredicatesOverride = objectMapper.convertValue(rootNodeAggregator.get(TYPE_PREDICATES_OVERRIDE_CONF),
                new TypeReference<HashSet<String>>() {
                });

        // Fields data
        this.fieldName = rootNodeIndex.get(FIELD_NAME_CONF).asText();
        this.numberOfFields = rootNodeAggregator.get(CLUSTERS_CONF).asInt();
        this.divideDataTypeAndObjectProperties = rootNodeAggregator.get(DIVIDE_DATATYPE_AND_OBJECT_PROPERTIES_CONF).asBoolean();
        this.createTypesOverrideField = !typePredicatesOverride.isEmpty();
        this.createRelationsFields = rootNodeAggregator.get(CREATE_RELATIONS_FIELDS_CONF).asBoolean();
        this.subfields = objectMapper.convertValue(rootNodeIndex.get(SUBFIELDS_CONF), new TypeReference<>() {
        });

        // Entity indexer
        byte[] mapData = Files.readAllBytes(Paths.get(ENTITY_INDEXER_CONFIGURATION_FILE));
        JsonNode rootNode = objectMapper.readTree(mapData);

        this.addPredicateNames = rootNode.get(ADD_PREDICATE_NAMES_CONF).asBoolean();
        this.predicateNamePrefixes = objectMapper.convertValue(rootNode.get(PREDICATE_NAME_PREFIXES_CONF),
                new TypeReference<List<String>>() {
                });

        // Entity searcher
        mapData = Files.readAllBytes(Paths.get(ENTITY_SEARCHER_CONFIGURATION_FILE));

        objectMapper = new ObjectMapper();
        rootNode = objectMapper.readTree(mapData);

        this.forbiddenSubfields = objectMapper.convertValue(rootNode.get(FORBIDDEN_SUBFIELDS_CONF),
                new TypeReference<List<String>>() {
                });
        this.maxNumberOfResults = rootNode.get(MAX_NUMBER_OF_RESULTS_CONF).asInt();
    }

    /**
     * Helper method for creating the needed EntityDocuments
     *
     * @param e        An extracted entity
     * @param t        A VirtualDocumentTemplate containing references to this index's fields and predicates associates to it.
     *                 Must not be empty, and can come from a previously saved JSON file or from an metrics aggregator
     * @param strategy The URI renaming strategy to follow. Can be generated from
     *                 EntityExtractorConfiguration.fromConfigurationFile().uriRenamingStrategy
     * @return An EntityDocument ready to be indexed
     */
    public EntityDocument createEntityDocument(ExtractedEntity e, VirtualDocumentTemplate t, EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        boolean addToCatchall = false; //TODO //this instanceof ElasticEntityIndexer;

        Map<String, String> predicateNames = e.getAllPredicateNamesMap(strategy);

        EntityDocument d = new EntityDocument(e.name);

        for (Field f : t.fields) {
            d.addField(f.name);

            // A field can be empty, and their names should match with the full predicate URIs (which can be obtained from
            // e.getAllPredicateURIs())
            for (var fieldElement : f.predicates) {
                if (f.isForEntityLinking) { // Do not add predicate names, only objects as-is (URIs)
                    List<String> terms = e.getObjectsOfPredicate(fieldElement.getPredicateURI(), EntityExtractorConfiguration.URIRenamingStrategy.None);

                    for (String term : terms) d.addTermToField(f.name, term, addToCatchall);
                } else {
                    List<String> terms = e.getObjectsOfPredicate(fieldElement.getPredicateURI(), strategy);

                    for (int i = 0; i < fieldElement.getRepetitions(); i++) { // For as many times as the predicate (and its objects) should appear
                        if (!terms.isEmpty()) {
                            if (addPredicateNames) {
                                if (predicateNamePrefixes.isEmpty()) { // Add all predicates
                                    // Add the predicate's name once, before its objects
                                    // In case it is a camelCase URI converted to text by extracting only its local name,
                                    // we separate it into chunks
                                    d.addTermToField(f.name, predicateNames.get(fieldElement.getPredicateURI()), addToCatchall);
                                } else { // Add only those predicates which contains any of the required prefixes
                                    for (String predicateNamePrefix : predicateNamePrefixes) {
                                        if (fieldElement.getPredicateURI().contains(predicateNamePrefix)) {
                                            d.addTermToField(f.name, predicateNames.get(fieldElement.getPredicateURI()), addToCatchall);
                                        }
                                    }
                                }
                            }

                            for (String term : terms) d.addTermToField(f.name, term, addToCatchall);
                        }
                    }
                }
            }
        }

        // TODO: For now, only on elastic
        /*if (this instanceof ElasticEntityIndexer) {
            // Add all relations
            for (var relation : e.relations.entrySet()) {
                for (var relationURI : relation.getValue()) d.addRelation(relationURI.getURI());
            }
        }*/

        // Add all types. In this case we don't distinguish between different type predicates, these will go to a generic
        // types field (if indicated in the configuration)
        for (var typeList : e.types.values()) {
            for (var type : typeList)
                d.addType(type.getURI());
        }

        return d;
    }

    public abstract void addDocumentToIndex(EntityDocument d) throws IOException;

    public abstract void finishIndexing() throws IOException, InterruptedException;

    /**
     * Executes the query on a given system, returning a list of well-formed URIs
     * alongside their scores, ordered by descending score, making use of the
     * provided template to search in and weigh accordingly multiple fields
     *
     * @param template A VirtualDocumentTemplate containing references to this index's fields. Can be empty and easily generated
     *                 from a previously saved JSON file or from the inference configuration with
     * @return A list of scored search results, in descending order
     */
    public abstract List<ScoredSearchResult> scoredSearch(String query,
                                                          VirtualDocumentTemplate template,
                                                          double k1,
                                                          double b) throws IOException;

    /**
     * Executes a map of query ID -> query on a given system, returning a map of
     * query ID -> List of results ordered by descending score, making use of the
     * provided template to search in and weigh accordingly multiple fields
     *
     * @param queries  A map of query ID -> query
     * @param template A VirtualDocumentTemplate containing references to this index's fields. Can be empty and easily generated
     *                 from a previously saved JSON file or from the inference configuration with
     * @return A list of scored search results, in descending order
     */
    public abstract Map<String, List<ScoredSearchResult>> scoredSearch(Map<String, String> queries,
                                                                       VirtualDocumentTemplate template,
                                                                       double k1,
                                                                       double b) throws IOException, ExecutionException, InterruptedException;
}

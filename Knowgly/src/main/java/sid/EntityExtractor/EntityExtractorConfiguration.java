package sid.EntityExtractor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Internal configuration class used by the entity extractor, which dictates which predicates are allowed and dictates
 * how to dereference URIs (see URIRenamingStrategy)
 */
public class EntityExtractorConfiguration {
    public enum URIRenamingStrategy {
        /**
         * Get base name, replace '_' with ' ' (http://dbpedia.org/resource/As_We_May_Think  → “as we may think”)
         */
        FromURI,
        /**
         * Try to get the label of said URI, apply FromURI otherwise
         */
        FromLabel,
        /**
         * Keep the URI as-is
         */
        None
    }

    private static final String CONFIGURATION_FILE = "configuration/entityExtractorConfiguration.json";
    private static final String EXTRACT_ATTRIBUTES_CONF = "extractAttributes";
    private static final String EXTRACT_RELATIONS_CONF = "extractRelations";
    private static final String EXTRACT_ANONYMOUS_NODES_CONF = "extractAnonymousNodes";
    private static final String FLATTEN_ENTITY_CONF = "flattenEntity";
    private static final String EXTRACT_TYPES_CONF = "extractTypes";
    private static final String URI_RENAMING_STRATEGY_CONF = "uriRenamingStrategy";
    private static final String TYPE_PREDICATES_CONF = "typePredicates";
    private static final String ALLOWED_PREDICATES_CONF = "allowedPredicates";
    private static final String FORBIDDEN_PREDICATES_CONF = "forbiddenPredicates";

    // Allowed or forbidden predicates. Only one of them can be set at the same time. If both are empty, all predicates
    // will be allowed
    public Set<Resource> allowedPredicates = new HashSet<>();
    public Set<Resource> forbiddenPredicates = new HashSet<>();

    // Predicate URIs to be considered as "indicators" of types (for example, http://purl.org/dc/terms/subject)
    // To be read by EntityExtractor instances
    public static Set<Resource> typePredicates = new HashSet<>();

    // General extraction options
    public boolean extractTypes = false;
    public boolean extractAttributes = false;
    public boolean extractRelations = false;

    // Whether to consider an anonymous node as a valid attribute (with an empty object)
    public boolean extractAnonymousNodes = false;

    // Whether to keep information about the domain of each property and range of each type (false), or not (true)
    public boolean flattenEntity = false;

    public URIRenamingStrategy uriRenamingStrategy;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static EntityExtractorConfiguration fromConfigurationFile() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        EntityExtractorConfiguration conf = new EntityExtractorConfiguration(rootNode.get(EXTRACT_TYPES_CONF).asBoolean(),
                rootNode.get(EXTRACT_ATTRIBUTES_CONF).asBoolean(),
                rootNode.get(EXTRACT_RELATIONS_CONF).asBoolean(),
                rootNode.get(EXTRACT_ANONYMOUS_NODES_CONF).asBoolean(),
                rootNode.get(FLATTEN_ENTITY_CONF).asBoolean(),
                URIRenamingStrategy.values()[rootNode.get(URI_RENAMING_STRATEGY_CONF).asInt()]);

        List<String> typePredicates =
                objectMapper.convertValue(rootNode.get(TYPE_PREDICATES_CONF), new TypeReference<List<String>>() {
                });
        Set<Resource> typePredicatesResources = new HashSet<>();
        for (String pred : typePredicates) typePredicatesResources.add(ResourceFactory.createResource(pred));
        if (!typePredicates.isEmpty()) conf.setTypePredicates(typePredicatesResources);

        List<String> allowedPredicates =
                objectMapper.convertValue(rootNode.get(ALLOWED_PREDICATES_CONF), new TypeReference<List<String>>() {
                });
        Set<Resource> allowedPredicatesResources = new HashSet<>();
        for (String pred : allowedPredicates) allowedPredicatesResources.add(ResourceFactory.createResource(pred));
        if (!allowedPredicates.isEmpty()) conf.setAllowedPredicates(allowedPredicatesResources);

        List<String> forbiddenPredicates =
                objectMapper.convertValue(rootNode.get(FORBIDDEN_PREDICATES_CONF), new TypeReference<List<String>>() {
                });
        Set<Resource> forbiddenPredicatesResources = new HashSet<>();
        for (String pred : forbiddenPredicates) forbiddenPredicatesResources.add(ResourceFactory.createResource(pred));
        if (!forbiddenPredicates.isEmpty()) conf.setForbiddenPredicates(forbiddenPredicatesResources);

        return conf;
    }

    public EntityExtractorConfiguration(boolean extractTypes,
                                        boolean extractAttributes,
                                        boolean extractRelations,
                                        boolean extractAnonymousNodes,
                                        boolean flattenEntity,
                                        URIRenamingStrategy strategy) {
        this.extractTypes = extractTypes;
        this.extractAttributes = extractAttributes;
        this.extractRelations = extractRelations;
        this.extractAnonymousNodes = extractAnonymousNodes;
        this.flattenEntity = flattenEntity;
        this.uriRenamingStrategy = strategy;
    }

    public void setAllowedPredicates(Set<Resource> allowedPredicates) {
        if (!forbiddenPredicates.isEmpty())
            throw new RuntimeException("Setting both allowed and forbidden predicates lists is not allowed");
        this.allowedPredicates = allowedPredicates;
    }

    public void setForbiddenPredicates(Set<Resource> forbiddenPredicates) {
        if (!allowedPredicates.isEmpty())
            throw new RuntimeException("Setting both allowed and forbidden predicates lists is not allowed");
        this.forbiddenPredicates = forbiddenPredicates;
    }

    public void setTypePredicates(Set<Resource> typePredicates) {
        EntityExtractorConfiguration.typePredicates = typePredicates;
    }

    /**
     * @param pred Jena resource containing a reference to the predicate
     * @return Whether the given predicate is allowed as per this configuration
     */
    public boolean isAllowed(Resource pred) {
        if ((allowedPredicates.isEmpty() && forbiddenPredicates.isEmpty())) return true;

        if (!forbiddenPredicates.isEmpty()) {
            return !(forbiddenPredicates.contains(pred));
        } else {
            return allowedPredicates.contains(pred);
        }
    }

    public boolean isTypePredicate(Resource pred) {
        return typePredicates.contains(pred);
    }
}

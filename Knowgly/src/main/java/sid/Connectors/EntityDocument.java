package sid.Connectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Virtual document for an entity, with its fields being its direct counterparts to the inverted index being used
 * <p>
 * The predicate contents are represented as lists of strings associated to it.
 * <p>
 * Following the scheme in the "Representations from Structured Knowledge Bases" chapter of Entity Oriented Search,
 * they can either be lists of objects associated to the predicates assigned to each field is, or contain a string
 * representation of the predicate right before all its associated object's representations. This behaviour is
 * controlled from the IndexConnector.
 * <p>
 * All contents are string representations of literals or URIs
 */
// Values to NOT be serialized (useful for ElasticConnector, which simply serializes everything)
@JsonIgnoreProperties(value = {
        "entity",
})
public class EntityDocument {
    // Name of the entity which this document represents
    private String entity;

    /**
     * Types this entity has (controlled from EntityExtractor)
     */
    public HashSet<String> types;

    /**
     * Valid URIs to which this entity is linked to, in no particular order
     */
    public HashSet<String> relations;

    /**
     * Map of field name <-> contents associated to it
     * Warning: If the variable name is changed, you will need to change it too in the ElasticEntitySearcher methods
     * See executeElasticQuery(String query, VirtualDocumentTemplate template)
     */
    public HashMap<String, List<String>> fields;

    // List of every term added to any of the predicates
    public List<String> catchAll;

    public EntityDocument() {
        this.entity = "";
        this.fields = new HashMap<>();
        this.catchAll = new ArrayList<>();
        this.types = new HashSet<>();
        this.relations = new HashSet<>();
    }

    public EntityDocument(String entity) {
        this.entity = entity;
        this.fields = new HashMap<>();
        this.catchAll = new ArrayList<>();
        this.types = new HashSet<>();
        this.relations = new HashSet<>();
    }

    /**
     * Add a type to this entity. Guaranteed to not add duplicates
     */
    public void addType(String URI) {
        types.add(URI);
    }

    /**
     * Add a relation to this entity. Guaranteed to not add duplicates
     */
    public void addRelation(String URI) {
        relations.add(URI);
    }

    /**
     * Add a term to the given field
     *
     * @return false if it was not inserted due to the predicate/field not having been inserted previously,
     * true otherwise
     */
    public boolean addTermToField(String field, String term, boolean addToCatchall) {
        if (fields.containsKey(field)) {
            addTermToField(fields, field, term, addToCatchall);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Add a field
     *
     * @return false if it hasn't been inserted due to it already being present, true otherwise
     */
    public boolean addField(String field) {
        if (!this.fields.containsKey(field)) {
            this.fields.put(field, new ArrayList<>());
            return true;
        } else {
            return false;
        }
    }

    @JsonIgnore
    public String getEntityName() {
        return entity;
    }

    public void setEntityName(String entity) {
        this.entity = entity;
    }

    private void addTermToField(HashMap<String, List<String>> fields, String fieldName, String term, boolean addToCatchall) {
        List<String> field = fields.get(fieldName);
        field.add(term);
        fields.replace(fieldName, field);

        if (addToCatchall) catchAll.add(term);
    }
}


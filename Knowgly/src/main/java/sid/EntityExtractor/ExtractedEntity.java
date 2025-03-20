package sid.EntityExtractor;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Intermediate representation of an Entity, filled by an EntityExtractor, which
 * doesn't have any kind of logical clusterization of its predicates
 * <p>
 * Inserted predicates are checked for duplicates and not inserted if so,
 * but object values inserted in the object lists of a given predicate
 * are not checked
 * <p>
 * Extra methods are provided to extract all predicates and objects of a given predicate
 * (from both relations and attributes) as Strings, given a URI renaming strategy. These
 * are used for indexing.
 */
public class ExtractedEntity {
    public String name;

    /**
     * Relations with other entities, as predicate->object(s) pairs (the objects need to be URIs)
     */
    public Map<Resource, List<Resource>> relations;

    /**
     * Types map, can be considered a separate map from relations
     */
    public Map<Resource, List<Resource>> types;

    /**
     * Attributes of this entity, as predicate->object(s) pairs (the objects need to be literals)
     */
    public Map<Resource, List<Literal>> attributes;

    /**
     * Domains for every predicate associated to this entity (if they exist)
     */
    public Map<Resource, List<Resource>> predicateDomains;

    /**
     * Ranges for every predicate associated to this entity (if they exist)
     */
    public Map<Resource, List<Resource>> predicateRanges;

    /**
     * Labels for every predicate found, filled only if needed (URIRenamingStrategy set to FromLabel...)
     */
    public Map<Resource, Literal> URILabels;

    public ExtractedEntity(String name,
                           Map<Resource, List<Resource>> types,
                           Map<Resource, List<Resource>> relations,
                           Map<Resource, List<Literal>> attributes,
                           Map<Resource, List<Resource>> predicateDomains,
                           Map<Resource, List<Resource>> predicateRanges,
                           Map<Resource, Literal> URILabels) {
        this.name = name;
        this.types = types;
        this.relations = relations;
        this.attributes = attributes;
        this.predicateDomains = predicateDomains;
        this.predicateRanges = predicateRanges;
        this.URILabels = URILabels;
    }

    public ExtractedEntity(String name) {
        this.name = name;
        this.types = new HashMap<>();
        this.relations = new HashMap<>();
        this.attributes = new HashMap<>();
        this.predicateDomains = new HashMap<>();
        this.predicateRanges = new HashMap<>();
        this.URILabels = new HashMap<>();
    }

    public void addType(Resource typePredicate, Resource typeObject) {
        List<Resource> typeObjects;
        if (types.containsKey(typePredicate)) {
            typeObjects = types.get(typePredicate);
        } else {
            typeObjects = new ArrayList<>();
        }
        typeObjects.add(typeObject);

        types.put(typePredicate, typeObjects);
    }

    /**
     * Register a new relation for the entity
     */
    public void addRelation(Resource pred, Resource obj) {
        List<Resource> objs;
        if (relations.containsKey(pred)) {
            objs = relations.get(pred);
        } else {
            objs = new ArrayList<>();
        }

        objs.add(obj);
        relations.put(pred, objs);
    }

    /**
     * Register a new attribute for the entity
     */
    public void addAttribute(Resource pred, Literal lit) {
        List<Literal> literals;
        if (attributes.containsKey(pred)) {
            literals = attributes.get(pred);
        } else {
            literals = new ArrayList<>();
        }

        literals.add(lit);
        attributes.put(pred, literals);
    }

    /**
     * Register a domain for the predicate
     */
    public void addDomainToPredicate(Resource pred, Resource domain) {
        List<Resource> domains;
        if (predicateDomains.containsKey(pred)) {
            domains = predicateDomains.get(pred);
        } else {
            domains = new ArrayList<>();
        }

        if (!domains.contains(domain)) domains.add(domain);
        predicateDomains.put(pred, domains);
    }

    /**
     * Register a range for the predicate
     */
    public void addRangeToPredicate(Resource pred, Resource range) {
        List<Resource> ranges;
        if (predicateRanges.containsKey(pred)) {
            ranges = predicateRanges.get(pred);
        } else {
            ranges = new ArrayList<>();
        }

        if (!ranges.contains(range)) ranges.add(range);
        predicateRanges.put(pred, ranges);
    }

    /**
     * Register a label for a predicate or URI object associated to this entity
     */
    public void addURILabel(Resource uri, Literal label) {
        URILabels.put(uri, label);
    }

    /**
     * Get a map of predicate URI -> predicate name, using the renaming strategy specified
     * Guarantees to contain all (allowed) predicates associated with this entity
     */
    public Map<String, String> getAllPredicateNamesMap(EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        Map<String, String> predicateNames = new HashMap<>();

        for (Resource rel : relations.keySet()) {
            predicateNames.put(rel.getURI(), extractResourceName(rel, strategy));
        }

        for (Resource attr : attributes.keySet()) {
            if (!predicateNames.containsKey(attr.getURI()))
                predicateNames.put(attr.getURI(), extractRDFNodeName(attr, strategy));
        }

        for (Resource type : EntityExtractorConfiguration.typePredicates) {
            predicateNames.put(type.getURI(), extractResourceName(type, strategy));
        }

        return predicateNames;
    }

    /**
     * Get every predicate name found for this entity, without duplicates, using the renaming strategy specified
     * Guarantees to contain all predicates associated with this entity
     */
    public List<String> getAllPredicateNames(EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        List<String> predicateNames = new ArrayList<>();

        for (Resource rel : relations.keySet()) {
            predicateNames.add(extractResourceName(rel, strategy));
        }

        for (RDFNode rel : attributes.keySet()) {
            if (!predicateNames.contains(rel.toString()))
                predicateNames.add(extractRDFNodeName(rel, strategy));
        }

        return predicateNames;
    }

    /**
     * Get every predicate URI found for this entity, without duplicates, as Strings
     */
    public List<String> getAllPredicateURIs() {
        return getAllPredicateNames(EntityExtractorConfiguration.URIRenamingStrategy.None);
    }

    /**
     * Get every object name found for this entity for the given predicate, including duplicates, using the renaming
     * strategy specified. The provided predicate name should be a complete URI (for example, "http://schema.org/sameAs")
     * <p>
     * Literals will be represented as the content enclosed inside its enclosed quotes, without its quotes and datatype,
     * and without its additional enclosing quotes and language indicator if applicable
     */
    public List<String> getObjectsOfPredicate(String predicate, EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        List<String> objects = new ArrayList<>();

        Resource predicateResource = ResourceFactory.createResource(predicate);

        if (relations.containsKey(predicateResource)) {
            for (Resource object : relations.get(predicateResource)) {
                objects.add(extractResourceName(object, strategy));
            }
        }

        if (attributes.containsKey(predicateResource)) {
            for (Literal lit : attributes.get(predicateResource)) {
                objects.add(extractStringFromLiteral(lit));
            }
        }

        if (EntityExtractorConfiguration.typePredicates.contains(predicateResource) && types.containsKey(predicateResource)) {
            for (Resource object : types.get(predicateResource)) {
                objects.add(extractResourceName(object, strategy));
            }
        }

        return objects;
    }

    // Return a String representing the literal's value, without its enclosing quotes and datatype
    private static String extractStringFromLiteral(Literal lit) {
        String litString = lit.getString();

        if (litString.length() > 0) {
            // It can also be enclosed by a language indicator, which
            // we also want to remove
            while (litString.length() > 0 && litString.charAt(0) == '"') {
                String subString = getSubStringAroundQuotes(litString);
                // If for some reason it starts with a '"', but does not contain more quotes, stop
                if (litString.equals(subString)) break;
                litString = subString;
            }

        }

        return litString;
    }

    // Returns the same string if no enclosing quotes are found
    private static String getSubStringAroundQuotes(String litString) {
        int startIndex = litString.indexOf('"'); // Find the first quote
        if (startIndex != -1) {
            int endIndex = litString.indexOf('"', startIndex + 1); // For some reason, lastIndexOf doesn't reliably work
            if (endIndex != -1) {
                int tempEndIndex = endIndex;

                while (tempEndIndex != -1) { // Try to look for any further quotes (the one we found may be an intermediate one)
                    tempEndIndex = litString.indexOf('"', endIndex + 1);
                    if (tempEndIndex != -1) endIndex = tempEndIndex;
                }

                litString = litString.substring(startIndex + 1, endIndex); // Remove everything around and including the quotes
            }
        }
        return litString;
    }

    // Get the given resource's name following the renaming strategy specified
    private String extractResourceName(Resource rel, EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        switch (strategy) {
            case FromURI -> {
                return sanitizeURI(getLocalName(rel));
            }
            case FromLabel -> {
                if (URILabels.containsKey(rel)) {
                    return extractStringFromLiteral(URILabels.get(rel));
                } else {
                    return sanitizeURI(getLocalName(rel));
                }
            }
            case None -> {
                return rel.getURI();
            }
        }

        return rel.getURI();
    }

    private static String getLocalName(Resource rel) {
        // Jena's method is unreliable (http://dbpedia.org/resource/Category:Formula_One_World_Drivers'_Champions -> Champions)
        //return rel.getLocalName();

        String path = rel.getURI();
        String[] URIPaths = path.split("/");
        String lastURIPath = URIPaths[URIPaths.length - 1];

        if (lastURIPath.contains(":")) {
            String[] subparts = lastURIPath.split(":");
            if (subparts.length > 0)
                return subparts[subparts.length - 1];
            else
                return lastURIPath;
        } else {
            return lastURIPath;
        }
    }

    public static String sanitizeURI(String uriString) {
        int index = uriString.lastIndexOf('#');
        if (index != -1) {
            uriString = uriString.substring(index + 1); // #label, #comment...
        }

        uriString = uriString.replace('_', ' ');
        return String.join(" ", StringUtils.splitByCharacterTypeCamelCase(uriString)); // Useful for types
    }


    // Get the predicate's name following the renaming strategy specified
    private String extractRDFNodeName(RDFNode rel, EntityExtractorConfiguration.URIRenamingStrategy strategy) {
        return extractResourceName(rel.asResource(), strategy);
    }
}

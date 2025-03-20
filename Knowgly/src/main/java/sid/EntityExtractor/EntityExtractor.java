package sid.EntityExtractor;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import java.util.List;

/**
 * Abstract class which all entity extractors need to implement
 */
public abstract class EntityExtractor {
    public EntityExtractorConfiguration config;

    public EntityExtractor(EntityExtractorConfiguration config) {
        this.config = config;
    }

    /**
     * @return A list of all the entities that can be extracted
     */
    public abstract List<Resource> getAllEntityURIs();

    /**
     * @param entity Jena resource referencing the entity
     * @return An extracted entity
     */
    public abstract ExtractedEntity extractEntity(Resource entity);

    /**
     * @param URI A valid URI referencing the entity
     * @return An extracted entity
     */
    public abstract ExtractedEntity extractEntity(String URI);

    /**
     * Add (or not) the given predicate and object to the given entity, according to the extractor's configuration
     * This should be called by the EntityExtractor implementations, so that the configuration is only handled here
     *
     * @param entity An extracted entity
     * @param pred   Predicate to which the object is associated
     * @param obj    Object associated to the predicate
     * @return An extracted entity with the object and predicate references added to it, depending on the extractor's
     * configuration
     */
    protected ExtractedEntity addPOToEntity(ExtractedEntity entity, Resource pred, RDFNode obj) {
        if (config.isAllowed(pred)) {
            if (obj.isLiteral()) {
                if (config.extractAttributes) entity.addAttribute(pred, obj.asLiteral());
            } else if (obj.isURIResource()) {
                if (config.extractTypes && config.isTypePredicate(pred)) entity.addType(pred, obj.asResource());
                else if (config.extractRelations) {
                    entity.addRelation(pred, obj.asResource());
                }
            } else { // Anonymous node
                if (config.extractAnonymousNodes) entity.addAttribute(pred, obj.asLiteral());
            }
        }

        return entity;
    }
}

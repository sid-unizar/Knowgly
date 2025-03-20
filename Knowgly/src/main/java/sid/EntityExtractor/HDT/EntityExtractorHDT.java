package sid.EntityExtractor.HDT;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import sid.EntityExtractor.EntityExtractor;
import sid.EntityExtractor.EntityExtractorConfiguration;
import sid.EntityExtractor.ExtractedEntity;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;


/**
 * EntityExtractor specialization for HDT. Recommended over the SPARQL specialization for efficiency reasons.
 * <p>
 * Note: The EntityExtractorHDT keeps internal cache for String representations of frequently looked-up elements
 * such as predicate URIs and labels, which will be initialized statically upon being created by its constructor or
 * via the fromConfigurationFile() method. Initialization in a threaded context is not supported.
 */
public class EntityExtractorHDT extends EntityExtractor {
    private static final String DOMAIN_URI = "http://www.w3.org/2000/01/rdf-schema#domain";
    private static final String RANGE_URI = "http://www.w3.org/2000/01/rdf-schema#range";
    private static final String LABEL_URI = "http://www.w3.org/2000/01/rdf-schema#label";

    private final long domainID;
    private final long rangeID;
    private final long labelID;
    private final LocalHDTSPARQLEndpoint endpoint;

    // Cache of predicate ID -> URI String
    private static ConcurrentHashMap<Long, String> predicateNamesCache = null;
    // Cache of predicate ID -> Label String
    private static ConcurrentHashMap<Long, Literal> predicateLabelsCache = null;
    // Cache of object ID -> Label String
    private static ConcurrentHashMap<Long, Literal> objectLabelsCache = null;
    // Cache of predicate ID -> Domains
    private static ConcurrentHashMap<Long, List<Resource>> predicateDomainsCache = null;
    // Cache of predicate ID -> Ranges
    private static ConcurrentHashMap<Long, List<Resource>> predicateRangesCache = null;
    // Only used for subsuming predicates
    //private Map<String, String> subsumedPredicatesMap;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file
     */
    public static EntityExtractorHDT fromConfigurationFile(LocalHDTSPARQLEndpoint endpoint) throws IOException, ExecutionException, InterruptedException {
        return new EntityExtractorHDT(EntityExtractorConfiguration.fromConfigurationFile(), endpoint);
    }

    public EntityExtractorHDT(EntityExtractorConfiguration config, LocalHDTSPARQLEndpoint endpoint) {
        super(config);
        this.endpoint = endpoint;
        this.domainID = endpoint.hdt.getDictionary().stringToId(DOMAIN_URI, TripleComponentRole.PREDICATE);
        this.rangeID = endpoint.hdt.getDictionary().stringToId(RANGE_URI, TripleComponentRole.PREDICATE);
        this.labelID = endpoint.hdt.getDictionary().stringToId(LABEL_URI, TripleComponentRole.PREDICATE);

        if (predicateNamesCache == null)
            fillCaches();
    }

    protected void fillCaches() {
        predicateNamesCache = new ConcurrentHashMap<>();
        predicateLabelsCache = new ConcurrentHashMap<>();
        objectLabelsCache = new ConcurrentHashMap<>();
        predicateDomainsCache = new ConcurrentHashMap<>();
        predicateRangesCache = new ConcurrentHashMap<>();

        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1);
        predicatesStream.forEach(p -> {
            // Names
            String pString = endpoint.hdt.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();
            predicateNamesCache.put(p, pString);

            // The predicate and subject dicts are separate, so we have to do a string to ID lookup
            long idOfPredicateActingAsSubject = endpoint.hdt.getDictionary().stringToId(pString, TripleComponentRole.SUBJECT);

            // Labels
            if (config.uriRenamingStrategy == EntityExtractorConfiguration.URIRenamingStrategy.FromLabel) {
                var labelQuery = endpoint.hdt.getTriples().search(new TripleID(idOfPredicateActingAsSubject, this.labelID, 0));
                if (labelQuery.hasNext()) {
                    predicateLabelsCache.put(p, endpoint.model.createTypedLiteral(endpoint.hdt.getDictionary().idToString(
                            labelQuery.next().getObject(), TripleComponentRole.OBJECT).toString()));
                }
            }

            // Domains and ranges
            List<Resource> predicateDomains = new ArrayList<>();
            List<Resource> predicateRanges = new ArrayList<>();

            var domainQuery = endpoint.hdt.getTriples().search(new TripleID(idOfPredicateActingAsSubject, this.domainID, 0));
            while (domainQuery.hasNext()) {
                long domain = domainQuery.next().getObject();

                predicateDomains.add(ResourceFactory.createResource(
                        endpoint.hdt.getDictionary().idToString(domain, TripleComponentRole.OBJECT).toString()));
            }

            // Extract the predicate's range
            var rangeQuery = endpoint.hdt.getTriples().search(new TripleID(idOfPredicateActingAsSubject, this.rangeID, 0));
            while (rangeQuery.hasNext()) {
                long range = rangeQuery.next().getObject();

                predicateRanges.add(ResourceFactory.createResource(
                        endpoint.hdt.getDictionary().idToString(range, TripleComponentRole.OBJECT).toString()));
            }

            predicateDomainsCache.put(p, predicateDomains);
            predicateRangesCache.put(p, predicateRanges);
        });

        // Object labels
        if (config.uriRenamingStrategy == EntityExtractorConfiguration.URIRenamingStrategy.FromLabel) {
            // Only look them up for shared subjects!
            LongStream sharedSubjectsStream = LongStream.range(1, endpoint.hdt.getDictionary().getNshared() + 1);
            sharedSubjectsStream.forEach(s -> {
                var labelQuery = endpoint.hdt.getTriples().search(new TripleID(s, this.labelID, 0));
                if (labelQuery.hasNext()) {
                    long labelID = labelQuery.next().getObject();

                    objectLabelsCache.put(s, endpoint.model.createTypedLiteral(
                            endpoint.hdt.getDictionary().idToString(
                                    labelID,
                                    TripleComponentRole.OBJECT).toString()));

                }
            });
        }

        // Only used for subsuming predicates
        /*var pair = ImportanceMetricsGeneratorHDT.getSubsumedPredicates(endpoint);
        var subsumedPredicatesIDsMap = pair.getKey();

        this.subsumedPredicatesMap = new HashMap<>();
        for (var entry : subsumedPredicatesIDsMap.entrySet()) {
            for (var subsumed : entry.getValue())
                subsumedPredicatesMap.put(endpoint.hdt.getDictionary().idToString(subsumed, TripleComponentRole.PREDICATE).toString(),
                        endpoint.hdt.getDictionary().idToString(entry.getKey(), TripleComponentRole.PREDICATE).toString());
        }*/
    }

    @Override
    public List<Resource> getAllEntityURIs() {
        List<Resource> entities = new ArrayList<>();

        for (PrimitiveIterator.OfLong it = LongStream.range(1, endpoint.hdt.getDictionary().getNsubjects() + 1).iterator(); it.hasNext(); ) {
            long entityID = it.next();
            entities.add(ResourceFactory.createResource(endpoint.hdt.getDictionary().idToString(entityID, TripleComponentRole.SUBJECT).toString()));
        }

        return entities;
    }

    /**
     * Note: Use of the HDT-specialized extractEntity(long entityID) is preferred, to avoid a lookup of its ID in the
     * dictionary
     */
    public ExtractedEntity extractEntity(Resource entity) {
        if (!entity.isURIResource()) {
            throw new RuntimeException("Attempted to extract an entity which is not an URI: " + entity);
        }

        return extractEntity(entity.getURI());
    }


    /**
     * Note: Use of the HDT-specialized extractEntity(long entityID) is preferred, to avoid a lookup of its ID in the
     * dictionary
     */
    @Override
    public ExtractedEntity extractEntity(String entityURI) {
        return extractEntity(endpoint.hdt.getDictionary().stringToId(entityURI, TripleComponentRole.SUBJECT));
    }

    public ExtractedEntity extractEntity(long entityID) {
        ExtractedEntity entity = new ExtractedEntity(
                endpoint.hdt.getDictionary().idToString(entityID, TripleComponentRole.SUBJECT).toString());

        var getEverythingFromEntityQuery = endpoint.hdt.getTriples().search(new TripleID(entityID, 0, 0));


        while (getEverythingFromEntityQuery.hasNext()) {
            TripleID triple = getEverythingFromEntityQuery.next();
            String pred = predicateNamesCache.get(triple.getPredicate());
            // Only used for subsuming predicates
            //if (subsumedPredicatesMap.containsKey(pred))
            //    pred = subsumedPredicatesMap.get(pred);

            Resource predResource = ResourceFactory.createResource(pred);
            // Check if the predicate is allowed, skip it if it isn't
            if (!config.isAllowed(predResource)) continue;

            String obj = endpoint.hdt.getDictionary().idToString(triple.getObject(), TripleComponentRole.OBJECT).toString();
            RDFNode objResource = switch (obj.charAt(0)) { // Is it a literal or a URI?
                case '"' -> ResourceFactory.createTypedLiteral(obj);
                default -> ResourceFactory.createResource(obj);
            };

            entity = addPOToEntity(entity, predResource, objResource);

            if (config.uriRenamingStrategy == EntityExtractorConfiguration.URIRenamingStrategy.FromLabel) {
                if (!entity.URILabels.containsKey(predResource)) {
                    if (predicateLabelsCache.containsKey(triple.getPredicate())) {
                        entity.addURILabel(predResource, predicateLabelsCache.get(triple.getPredicate()));
                    }
                }

                // Get the label for o
                if (objResource.isURIResource()) { // If it's a literal, we don't even need to query anything
                    if (!entity.URILabels.containsKey(objResource.asResource())) {
                        if (objectLabelsCache.containsKey(triple.getObject())) {
                            entity.addURILabel(objResource.asResource(), objectLabelsCache.get(triple.getObject()));
                        }
                    }
                }
            }


            if (!config.flattenEntity) {
                if (predicateDomainsCache.containsKey(triple.getPredicate())) {
                    for (Resource domain : predicateDomainsCache.get(triple.getPredicate())) {
                        entity.addDomainToPredicate(predResource, domain);
                    }
                }

                if (predicateRangesCache.containsKey(triple.getPredicate())) {
                    for (Resource range : predicateDomainsCache.get(triple.getPredicate())) {
                        entity.addRangeToPredicate(predResource, range);
                    }
                }
            }
        }

        return entity;
    }

}

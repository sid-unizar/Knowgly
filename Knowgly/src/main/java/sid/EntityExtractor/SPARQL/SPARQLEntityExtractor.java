package sid.EntityExtractor.SPARQL;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Query;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import sid.EntityExtractor.EntityExtractor;
import sid.EntityExtractor.EntityExtractorConfiguration;
import sid.EntityExtractor.ExtractedEntity;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * EntityExtractor specialization for local or remote SPARQL Endpoints. Not recommended for efficiency reasons
 */
public class SPARQLEntityExtractor extends EntityExtractor {
    public static final String ALL_ENTITIES_QUERY_FILE = "configuration/queries/all_entities_query.sparql";
    public static final String DOMAIN_QUERY_FILE = "configuration/queries/domain_query.sparql";
    public static final String RANGE_QUERY_FILE = "configuration/queries/range_query.sparql";
    public static final String PREDICATE_LABELS_QUERY_FILE = "configuration/queries/predicate_labels_query.sparql";
    public static final String OBJECT_LABELS_QUERY_FILE = "configuration/queries/object_labels_query.sparql";

    private String allEntitiesQuery;
    private String domainQuery;
    private String rangeQuery;
    private String predicateLabelsQuery;
    private String objectLabelsQuery;
    private final SPARQLEndpoint sparqlEndpoint;

    /**
     * Constructor from configuration files
     *
     * @throws IOException If there is any IO error with the configuration file, or when loading the SPARQL queries
     */
    public static SPARQLEntityExtractor fromConfigurationFile(SPARQLEndpoint sparqlEndpoint) throws IOException {
        return new SPARQLEntityExtractor(EntityExtractorConfiguration.fromConfigurationFile(), sparqlEndpoint);

    }

    /**
     * @throws IOException If there is any IO error when loading the SPARQL queries
     */
    public SPARQLEntityExtractor(EntityExtractorConfiguration config, SPARQLEndpoint sparqlEndpoint) throws IOException {
        super(config);
        this.sparqlEndpoint = sparqlEndpoint;
        loadQueries();
    }

    @Override
    public List<Resource> getAllEntityURIs() {
        List<Resource> entities = new ArrayList<>();

        ResultSet rs = sparqlEndpoint.runSelectQuery(allEntitiesQuery);
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();

            entities.add(qs.getResource("s"));
        }

        return entities;
    }

    public ExtractedEntity extractEntity(Resource entity) {
        if (!entity.isURIResource()) {
            throw new RuntimeException("Attempted to extract an entity which is not an URI: " + entity);
        }

        return extractEntity(entity.getURI());
    }

    @Override
    public ExtractedEntity extractEntity(String entityURI) {
        try {
            SelectBuilder sb = new SelectBuilder()
                    .addVar("?pred")
                    .addVar("?obj")
                    .addWhere("<" + entityURI + ">", "?pred", "?obj");

            if (!config.allowedPredicates.isEmpty()) {
                StringBuilder filterStr = new StringBuilder();
                Iterator<Resource> it = config.allowedPredicates.iterator();
                while (it.hasNext()) {
                    filterStr.append("?pred = <" + it.next() + ">");

                    if (it.hasNext() || !config.typePredicates.isEmpty()) filterStr.append(" || ");
                }

                // Añadir también predicados de tipo como permitidos
                it = config.typePredicates.iterator();
                while (it.hasNext()) {
                    filterStr.append("?pred = <" + it.next() + ">");

                    if (it.hasNext()) filterStr.append(" || ");
                }

                sb.addFilter(filterStr.toString());

            } else if (!config.forbiddenPredicates.isEmpty()) {
                StringBuilder filterStr = new StringBuilder();
                Iterator<Resource> it = config.forbiddenPredicates.iterator();
                while (it.hasNext()) {
                    filterStr.append("?pred != <" + it.next() + ">");

                    if (it.hasNext()) filterStr.append(" && ");
                }

                sb.addFilter(filterStr.toString());
            }

            Query getEverythingFromEntity = sb.build();

            ExtractedEntity entity = new ExtractedEntity(entityURI);

            ResultSet rs = sparqlEndpoint.runSelectQuery(getEverythingFromEntity);
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();

                Resource pred = qs.getResource("pred");
                RDFNode obj = qs.get("obj");

                entity = addPOToEntity(entity, pred, obj);

                if (!config.flattenEntity) {
                    // Extract the predicate's domain
                    ResultSet resultSetDom = sparqlEndpoint.runSelectQuery(domainQuery.formatted("<" + pred + ">"));
                    while (resultSetDom.hasNext()) {
                        QuerySolution qsDom = resultSetDom.next();

                        RDFNode dom = qsDom.getResource("obj");

                        entity.addDomainToPredicate(pred, dom.asResource());
                    }

                    // Extract the predicate's range
                    ResultSet resultSetRange = sparqlEndpoint.runSelectQuery(rangeQuery.formatted("<" + pred + ">"));
                    while (resultSetRange.hasNext()) {
                        QuerySolution qsRange = resultSetRange.next();

                        RDFNode range = qsRange.getResource("obj");

                        entity.addRangeToPredicate(pred, range.asResource());
                    }
                }
            }

            if (config.uriRenamingStrategy == EntityExtractorConfiguration.URIRenamingStrategy.FromLabel) {
                ResultSet resultSetPredLabels = sparqlEndpoint.runSelectQuery(predicateLabelsQuery.formatted("<" + entityURI + ">"));
                while (resultSetPredLabels.hasNext()) {
                    QuerySolution qs = resultSetPredLabels.next();

                    Resource pred = qs.getResource("p");
                    Literal label = qs.getLiteral("label");

                    entity.addURILabel(pred, label);
                }

                ResultSet resultSetObjLabels = sparqlEndpoint.runSelectQuery(objectLabelsQuery.formatted("<" + entityURI + ">"));
                while (resultSetObjLabels.hasNext()) {
                    QuerySolution qs = resultSetObjLabels.next();

                    Resource obj = qs.getResource("o");
                    Literal label = qs.getLiteral("label");

                    entity.addURILabel(obj, label);
                }
            }

            return entity;
        } catch (ParseException e) {
            System.err.println("Error when parsing a query");
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private void loadQueries() throws IOException {
        Path allEntitiesQueryPath = Path.of(ALL_ENTITIES_QUERY_FILE);
        allEntitiesQuery = Files.readString(allEntitiesQueryPath);

        Path domainQueryPath = Path.of(DOMAIN_QUERY_FILE);
        domainQuery = Files.readString(domainQueryPath);

        Path rangeQueryPath = Path.of(RANGE_QUERY_FILE);
        rangeQuery = Files.readString(rangeQueryPath);

        Path predicateLabelsQueryPath = Path.of(PREDICATE_LABELS_QUERY_FILE);
        predicateLabelsQuery = Files.readString(predicateLabelsQueryPath);

        Path objectLabelsQueryPath = Path.of(OBJECT_LABELS_QUERY_FILE);
        objectLabelsQuery = Files.readString(objectLabelsQueryPath);
    }

}

package sid.SPARQLEndpoint;

import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.Update;
import org.rdfhdt.hdt.exceptions.ParserException;

import java.io.IOException;

/**
 * Generic interface for all endpoints
 */
public interface SPARQLEndpoint {
    String VIRTUAL_TYPE = "http://sid-unizar-search.com/virtualType";
    String RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    void runUpdate(String update);

    ResultSet runSelectQuery(String query);

    boolean runAskQuery(String query);

    Model runDescribeQuery(String query);

    Model runConstructQuery(String query);

    void runUpdate(Update update);

    ResultSet runSelectQuery(Query query);

    boolean runAskQuery(Query query);

    Model runDescribeQuery(Query query);

    Model runConstructQuery(Query query);

    void addModel(Model model);

    void close();

    /**
     * Add a virtual type to every entity (unique subject and object URIs) in the endpoint, in order to improve the
     * exhaustiveness of the metrics, which assume that all entities are typed
     *
     * @throws IOException     If any IO error occurs while writing results to temporary files
     * @throws ParserException If any error occurs during HDT parsing (only on LocalHDTSPARQLEndpoint)
     */
    void addVirtualTypes() throws IOException, ParserException;
}

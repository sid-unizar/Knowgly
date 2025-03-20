package sid.SPARQLEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.system.Txn;
import org.apache.jena.update.Update;
import org.rdfhdt.hdt.exceptions.ParserException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * A connection to a remote SPARQL endpoint
 */
public class RemoteSPARQLEndpoint implements SPARQLEndpointWithNamedGraphs {
    public static final String CONFIGURATION_FILE = "configuration/RemoteSPARQLEndpointConfiguration.json";
    public static final String SPARQL_REMOTE_URL_CONF = "RemoteEndpointURL";
    private static final String ADD_VIRTUAL_TYPES_QUERY = "configuration/queries/addVirtualTypes.sparql";
    private final String remoteURL;

    public static RemoteSPARQLEndpoint fromConfigurationFile() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new RemoteSPARQLEndpoint(rootNode.get(SPARQL_REMOTE_URL_CONF).asText());
    }

    public RemoteSPARQLEndpoint(String remoteURL) {
        this.remoteURL = remoteURL;
    }

    @Override
    public void runUpdate(String update) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            Txn.executeWrite(conn, () -> {
                conn.update(update);
            });
        }
    }

    @Override
    public ResultSet runSelectQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execSelect();
        }
    }

    @Override
    public boolean runAskQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execAsk();
        }
    }

    @Override
    public Model runDescribeQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execDescribe();
        }
    }

    @Override
    public Model runConstructQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execConstruct();
        }
    }

    @Override
    public void runUpdate(Update update) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            Txn.executeWrite(conn, () -> {
                conn.update(update);
            });
        }
    }

    @Override
    public ResultSet runSelectQuery(Query query) {
        return runSelectQuery(query.serialize());
    }

    @Override
    public boolean runAskQuery(Query query) {
        return runAskQuery(query.serialize());
    }

    @Override
    public Model runDescribeQuery(Query query) {
        return runDescribeQuery(query.serialize());
    }

    @Override
    public Model runConstructQuery(Query query) {
        return runConstructQuery(query.serialize());
    }

    @Override
    public void addModel(Model model) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            conn.load(model);
        }
    }

    @Override
    public void addNamedModel(String URI, Model model) {
        Dataset temp = DatasetFactory.createTxnMem();
        temp.addNamedModel(URI, model);

        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {
            conn.loadDataset(temp);
        }
    }

    @Override
    public ResultSet getIteratorOverSubgraph(String graphURI) {
        try (RDFConnection conn = RDFConnectionRemote.service(remoteURL).build()) {

            String subgraphQuery = """
                    SELECT ?s ?p ?o
                    WHERE {
                        GRAPH <%s> {
                            ?s ?p ?o .
                        }
                    }
                    """.formatted(graphURI);

            QueryExecution qExec = conn.query(subgraphQuery);
            return qExec.execSelect();
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void addVirtualTypes() throws IOException, ParserException {
        runUpdate(Files.readString(Path.of(ADD_VIRTUAL_TYPES_QUERY)));
    }
}

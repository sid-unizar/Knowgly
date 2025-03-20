package sid.SPARQLEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.update.Update;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Embedded SPARQL server using Jena-Fuseki
 * <p>
 * Needs to be provided a Jena dataset location, alongside the name it's going to publish it under. The server will
 * start during initialization.
 */
public class EmbeddedSPARQLServerEndpoint implements SPARQLEndpointWithNamedGraphs {
    public static final String CONFIGURATION_FILE = "configuration/EmbeddedSPARQLServerEndpointConfiguration.json";
    public static final String SPARQL_DATASET_LOCATION_CONF = "SPARQLDatasetLocation";
    public static final String SPARQL_DATASET_NAME_CONF = "SPARQLDatasetName";
    public static final String SPARQL_DATASET_PORT_CONF = "SPARQLDatasetPort";
    private static final String ADD_VIRTUAL_TYPES_QUERY = "configuration/queries/addVirtualTypes.sparql";
    private final String datasetName;
    private FusekiServer server;
    private final int port;
    private final Dataset dataset;

    public static EmbeddedSPARQLServerEndpoint fromConfigurationFile() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new EmbeddedSPARQLServerEndpoint(rootNode.get(SPARQL_DATASET_LOCATION_CONF).asText(),
                rootNode.get(SPARQL_DATASET_NAME_CONF).asText(),
                rootNode.get(SPARQL_DATASET_PORT_CONF).asInt());
    }

    public EmbeddedSPARQLServerEndpoint(String datasetLocation, String datasetName, int port) {
        this.datasetName = datasetName;
        this.port = port;

        dataset = TDB2Factory.connectDataset(datasetLocation);
        startServer();
    }

    /**
     * Starts a Fuseki server using a TDB2 dataset, listening on the port 3030. Default setup:
     * <p>
     * Service         Endpoint 1                  Endpoint 2
     * SPARQL Query 	http://host:3330/ds/query 	http://host:3330/ds
     * SPARQL Query 	http://host:3330/ds/sparql 	http://host:3330/ds
     * SPARQL Update 	http://host:3330/ds/update 	http://host:3330/ds
     * GSP read-write 	http://host:3330/ds/data 	http://host:3330/ds
     */
    private void startServer() {
        System.out.println("Attaching dataset to Fuseki...");

        // https://jena.apache.org/documentation/fuseki2/fuseki-embedded.html#logging
        // Make its logger shut up
        FusekiLogging.setLogging();
        LogCtl.setLevel(Fuseki.serverLogName, "WARN");
        LogCtl.setLevel(Fuseki.actionLogName, "WARN");
        LogCtl.setLevel(Fuseki.requestLogName, "WARN");
        LogCtl.setLevel(Fuseki.adminLogName, "WARN");
        LogCtl.setLevel("org.eclipse.jetty", "WARN");

        server = FusekiServer.create()
                .add(datasetName, dataset)
                .port(port)
                .build();
        server.start();

        System.out.println("Started Fuseki server at " + server.datasetURL(datasetName));
    }

    @Override
    public void runUpdate(String update) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            Txn.executeWrite(conn, () -> {
                conn.update(update);
            });
        }
    }

    @Override
    public ResultSet runSelectQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execSelect();
        }
    }

    @Override
    public boolean runAskQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execAsk();
        }
    }

    @Override
    public Model runDescribeQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execDescribe();
        }
    }

    @Override
    public Model runConstructQuery(String query) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            QueryExecution qExec = conn.query(query);
            return qExec.execConstruct();
        }
    }

    @Override
    public void runUpdate(Update update) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
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
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            conn.load(model);
        }
    }

    @Override
    public void addNamedModel(String URI, Model model) {
        Dataset temp = DatasetFactory.createTxnMem();
        temp.addNamedModel(URI, model);

        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {
            conn.loadDataset(temp);
        }
    }

    @Override
    public ResultSet getIteratorOverSubgraph(String graphURI) {
        try (RDFConnection conn = RDFConnectionRemote.service("http://localhost:" + port + "/" + datasetName).build()) {

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
        server.stop();
        System.out.println("Stopped Fuseki server");
    }

    @Override
    public void addVirtualTypes() throws IOException {
        runUpdate(Files.readString(Path.of(ADD_VIRTUAL_TYPES_QUERY)));
    }
}

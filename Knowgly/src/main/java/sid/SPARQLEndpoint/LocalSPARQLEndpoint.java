package sid.SPARQLEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateAction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Wrapper for a file-backed Jena model, without using TDB2 datasets
 */
public class LocalSPARQLEndpoint implements SPARQLEndpoint {
    private static String CONFIGURATION_FILE = "configuration/LocalSPARQLEndpointConfiguration.json";
    private static String DATASET_LOCATION_CONF = "datasetLocation";
    private static final String ADD_VIRTUAL_TYPES_QUERY = "configuration/queries/addVirtualTypes.sparql";
    public final Model model;
    public final String datasetLocation;

    public static LocalSPARQLEndpoint fromConfigurationFile() throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new LocalSPARQLEndpoint(rootNode.get(DATASET_LOCATION_CONF).asText());
    }

    public LocalSPARQLEndpoint(String datasetLocation) {
        System.out.println("Loading Jena model...");
        this.model = RDFDataMgr.loadModel(datasetLocation);
        this.datasetLocation = datasetLocation;
    }

    @Override
    public void runUpdate(String update) {
        UpdateAction.parseExecute(update, model);
    }

    @Override
    public ResultSet runSelectQuery(String query) {
        return runSelectQuery(QueryFactory.create(query));
    }

    @Override
    public boolean runAskQuery(String query) {
        return runAskQuery(QueryFactory.create(query));
    }

    @Override
    public Model runDescribeQuery(String query) {
        return runDescribeQuery(QueryFactory.create(query));
    }

    @Override
    public Model runConstructQuery(String query) {
        return runConstructQuery(QueryFactory.create(query));
    }

    @Override
    public void runUpdate(Update update) {
        UpdateAction.execute(update, model);
    }

    @Override
    public ResultSet runSelectQuery(Query query) {
        QueryExecution qExec = QueryExecutionFactory.create(query, model);
        return qExec.execSelect();
    }

    @Override
    public boolean runAskQuery(Query query) {
        QueryExecution qExec = QueryExecutionFactory.create(query, model);
        return qExec.execAsk();
    }

    @Override
    public Model runDescribeQuery(Query query) {
        QueryExecution qExec = QueryExecutionFactory.create(query, model);
        return qExec.execDescribe();
    }

    @Override
    public Model runConstructQuery(Query query) {
        QueryExecution qExec = QueryExecutionFactory.create(query, model);
        return qExec.execConstruct();
    }

    @Override
    public void addModel(Model model) {
        this.model.add(model);
    }

    @Override
    public void close() {
        try {
            model.write(new BufferedWriter(new FileWriter(this.datasetLocation)));
        } catch (Exception e) {
            System.err.println("Couldn't write back changes to the local SPARQL endpoint:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void addVirtualTypes() throws IOException {
        runUpdate(Files.readString(Path.of(ADD_VIRTUAL_TYPES_QUERY)));
    }
}

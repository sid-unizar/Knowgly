package sid.SPARQLEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.shaded.com.google.common.collect.Iterators;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateAction;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.hdt.HDTSupplier;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFFluxStop;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;

/**
 * Wrapper for a HDT-backed Jena model, to be considered as read-only. Can be created from an existing RDF file.
 * <p>
 * Update queries will be run and saved on a separate Jena model, since the model on top of HDT is read-only.
 * If it is constructed without providing updatesDatasetLocation, update queries will not be executed at all.
 * Needs to be explicitly closed in order to concatenate any update to the original HDT file
 * <p>
 * If updates are really needed, it is recommended to instead create a new HDT model and concatenate them. This is
 * what we do for incorporating calculated metrics.
 * <p>
 * Provides additional constructors and helper methods on top of SPARQLEndpoint
 * <p>
 * All constructors provide an indexed parameter, which dictates if the memory-mapped HDT file is to be indexed or not
 * If the endpoint is not going to be queried, it is faster to avoid it. For any other use case, it's necessary (queries
 * will be VERY slow otherwise)
 */
public class LocalHDTSPARQLEndpoint implements SPARQLEndpoint {
    private final static String CONFIGURATION_FILE = "configuration/LocalHDTSPARQLEndpointConfiguration.json";
    private final static String DATASET_LOCATION_CONF = "datasetLocation";
    private final static String UPDATES_DATASET_LOCATION_CONF = "updatesDatasetLocation";

    public HDT hdt; // The HDT file this endpoint will serve
    public Model model; // Its HDT-backed Jena model
    public String datasetLocation;

    private final Model updatesModel;

    // Every time an update is run against the model it will be written to the updates dataset file,
    // since Jena's HDT-backed Model is read-only
    private final BufferedWriter updatesDatasetFileWriter;
    private String updatesDatasetLocation;
    public final String baseURI;

    public static LocalHDTSPARQLEndpoint fromConfigurationFile() throws IOException, NotFoundException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new LocalHDTSPARQLEndpoint(rootNode.get(DATASET_LOCATION_CONF).asText(),
                rootNode.get(UPDATES_DATASET_LOCATION_CONF).asText(), true);
    }


    /**
     * Convert a RDF file to HDT and return a LocalHDTSPARQLEndpoint wrapping it. Use fromRDFCatTree if the input file
     * is expected to be considerably big
     *
     * @param inputFile       RDF file to convert
     * @param outputFile      location of the resulting HDT file
     * @param baseURI         Base URI for the HDT dataset, existing ones will not be changed.
     *                        You can set the value to any non-empty String if no prefixes are used (as is the case in Knowgly)
     * @param inputType       HDT's RDFNotation String (ntriples, nt, n3, nq, nquad, rdf-xml, owl, turtle, tbz2...)
     * @param deleteInputFile Delete the original input file after the conversion to HDT (Note: this will invalidate any endpoint which used it)
     * @param indexed         Whether to create an index for the HDT or not
     * @return a LocalHDTSPARQLEndpoint wrapping the resulting HDT file
     * @throws IOException     Ifany  IO any error occurs during the HDT file creation
     * @throws ParserException If parsing error occurs during the HDT file creation
     */
    public static LocalHDTSPARQLEndpoint fromRDF(String inputFile,
                                                 String outputFile,
                                                 String baseURI,
                                                 String inputType,
                                                 boolean deleteInputFile,
                                                 boolean indexed
    ) throws IOException, ParserException {
        // Create HDT from RDF file
        try (HDT hdt = HDTManager.generateHDT(
                inputFile,         // Input RDF File
                baseURI,          // Base URI
                RDFNotation.parse(inputType), // Input Type
                new HDTSpecification(),   // HDT Options
                null              // Progress Listener
        )) {
            // Save generated HDT to a file
            hdt.saveToHDT(outputFile, null);
            if (deleteInputFile && !new File(inputFile).delete()) {
                System.err.println("Failed to delete temporary RDF file!");
            }

            return new LocalHDTSPARQLEndpoint(outputFile, baseURI, indexed);
        }
    }

    /**
     * Convert a RDF file to HDT and return a LocalHDTSPARQLEndpoint wrapping it. Specialized version for big HDT files
     * <p>
     * Adapted from https://github.com/rdfhdt/hdt-java/blob/master/hdt-java-cli/src/main/java/org/rdfhdt/hdt/tools/RDF2HDT.java
     *
     * @param inputFile       RDF file to convert
     * @param outputFile      location of the resulting HDT file
     * @param baseURI         Base URI for the HDT dataset, existing ones will not be changed.
     *                        You can set the value to any non-empty String if no prefixes are used (as is the case in Knowgly)
     * @param inputType       HDT's RDFNotation String (ntriples, nt, n3, nq, nquad, rdf-xml, owl, turtle, tbz2...)
     * @param deleteInputFile Delete the original input file after the conversion to HDT (Note: this will invalidate any endpoint which used it)
     * @param indexed         Whether to create an index for the HDT or not
     * @return a LocalHDTSPARQLEndpoint wrapping the resulting HDT file
     * @throws IOException     Ifany  IO any error occurs during the HDT file creation
     * @throws ParserException If parsing error occurs during the HDT file creation
     */
    public static LocalHDTSPARQLEndpoint fromRDFCatTree(String inputFile,
                                                        String outputFile,
                                                        String baseURI,
                                                        String inputType,
                                                        boolean deleteInputFile,
                                                        boolean indexed
    ) throws IOException, ParserException {
        // Theoretical maximum amount of memory the JVM will attempt to use
        Runtime runtime = Runtime.getRuntime();
        long maxTreeCatChunkSize = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) / (0.85 * 5));

        try (HDT hdt = HDTManager.catTree(
                RDFFluxStop.sizeLimit(maxTreeCatChunkSize),
                HDTSupplier.disk(),
                inputFile,         // Input RDF File
                baseURI,          // Base URI
                RDFNotation.parse(inputType), // Input Type
                new HDTSpecification(),   // HDT Options
                null              // Progress Listener
        )) {
            // Save generated HDT to a file
            hdt.saveToHDT(outputFile, null);
            if (deleteInputFile && !new File(inputFile).delete()) {
                System.err.println("Failed to delete temporary RDF file!");
            }

            return new LocalHDTSPARQLEndpoint(outputFile, baseURI, indexed);
        }
    }

    /**
     * Create an HDT file in the given location from all the triples in the endpoint's default graph,
     * and return a LocalHDTSPARQLEndpoint around it. Note: Slower than fromRDF
     */
    public static LocalHDTSPARQLEndpoint fromEndpoint(SPARQLEndpoint endpoint,
                                                      String baseURI,
                                                      String hdtFileLocation) throws ParserException, IOException {
        ResultSet triplesInSubgraph = endpoint.runSelectQuery("SELECT ?s ?p ?o WHERE {?s ?p ?o.}");

        // Convert Jena's iterator to an HDT's TripleString iterator on the fly
        Iterator<TripleString> tripleStringIterator = Iterators.transform(triplesInSubgraph, (querySolution) -> {
            String s = querySolution.get("s").toString();
            String p = querySolution.get("p").toString();
            String o = querySolution.get("o").toString();

            return new TripleString(s, p, o);
        });

        try (HDT hdt = HDTManager.generateHDT(
                tripleStringIterator,
                baseURI,
                new HDTSpecification(),   // HDT Options
                null              // Progress Listener
        )) {
            hdt.saveToHDT(hdtFileLocation, null);
            return new LocalHDTSPARQLEndpoint(hdt, hdtFileLocation, baseURI);
        }
    }

    /**
     * Create an HDT file in the given location from all the triples in the endpoint's specific graph,
     * and return a LocalHDTSPARQLEndpoint around it
     */
    public static LocalHDTSPARQLEndpoint fromEndpointSubgraph(SPARQLEndpointWithNamedGraphs endpoint,
                                                              String baseURI,
                                                              String subgraph,
                                                              String hdtFileLocation) throws ParserException, IOException {
        ResultSet triplesInSubgraph = endpoint.getIteratorOverSubgraph(subgraph);

        // Convert Jena's iterator to an HDT's TripleString iterator on the fly
        Iterator<TripleString> tripleStringIterator = Iterators.transform(triplesInSubgraph, (querySolution) -> {
            String s = querySolution.get("s").toString();
            String p = querySolution.get("p").toString();
            String o = querySolution.get("o").toString();

            return new TripleString(s, p, o);
        });


        try (HDT hdt = HDTManager.generateHDT(
                tripleStringIterator,
                baseURI,
                new HDTSpecification(),   // HDT Options
                null              // Progress Listener
        )) {
            hdt.saveToHDT(hdtFileLocation, null);
            return new LocalHDTSPARQLEndpoint(hdt, hdtFileLocation, baseURI);
        }
    }

    /**
     * Updates-enabled constructor (note: use of concatenations is recommended over this)
     */
    public LocalHDTSPARQLEndpoint(String datasetLocation,
                                  String updatesDatasetLocation,
                                  String baseURI,
                                  boolean indexed) throws IOException {
        this.datasetLocation = datasetLocation;
        this.baseURI = baseURI;

        // Load HDT file using the hdt-java library
        HDT hdt;
        if (indexed) {
            hdt = HDTManager.mapIndexedHDT(datasetLocation);
        } else {
            hdt = HDTManager.mapHDT(datasetLocation);
        }
        this.hdt = hdt;

        // Create Jena Model on top of HDT.
        HDTGraph graph = new HDTGraph(hdt, true);

        this.model = ModelFactory.createModelForGraph(graph);

        this.updatesModel = ModelFactory.createDefaultModel();
        this.updatesDatasetFileWriter = new BufferedWriter(new FileWriter(updatesDatasetLocation));
        this.updatesDatasetLocation = updatesDatasetLocation;
    }

    /**
     * Basic constructor, with disabled updates
     */
    public LocalHDTSPARQLEndpoint(String datasetLocation,
                                  String baseURI,
                                  boolean indexed) throws IOException {
        this.datasetLocation = datasetLocation;
        this.baseURI = baseURI;

        // Load HDT file using the hdt-java library
        HDT hdt;
        if (indexed) {
            hdt = HDTManager.mapIndexedHDT(datasetLocation);
        } else {
            hdt = HDTManager.mapHDT(datasetLocation);
        }
        this.hdt = hdt;

        // Create Jena Model on top of HDT.
        HDTGraph graph = new HDTGraph(hdt, true);
        this.model = ModelFactory.createModelForGraph(graph);

        this.updatesModel = ModelFactory.createDefaultModel();
        this.updatesDatasetFileWriter = null;
    }

    /**
     * Constructor from a HDT file. WIll inherit its indexing setting
     */
    public LocalHDTSPARQLEndpoint(HDT hdt, String datasetLocation, String baseURI) {
        this.datasetLocation = datasetLocation;
        ;
        this.hdt = hdt;
        this.baseURI = baseURI;

        // Create Jena Model on top of HDT.
        HDTGraph graph = new HDTGraph(hdt, true);
        this.model = ModelFactory.createModelForGraph(graph);

        this.updatesModel = ModelFactory.createDefaultModel();
        this.updatesDatasetFileWriter = null;
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
    public void runUpdate(String update) {
        if (updatesDatasetFileWriter != null) {
            UpdateAction.parseExecute(update, model);
            updatesModel.write(updatesDatasetFileWriter);
        }
    }

    @Override
    public void runUpdate(Update update) {
        if (updatesDatasetFileWriter != null) {
            UpdateAction.execute(update, updatesModel);
            updatesModel.write(updatesDatasetFileWriter);
        }
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
        if (updatesDatasetFileWriter != null) {
            updatesModel.add(model);
        }
    }

    @Override
    public void close() {
        try {
            if (updatesDatasetFileWriter != null) {
                updatesModel.write(updatesDatasetFileWriter);

                // Do a self-concatenation of the HDT file and the updates model
                this.concatenate(LocalHDTSPARQLEndpoint.fromRDF(updatesDatasetLocation,
                                datasetLocation + "_updates_HDT_concat_temp",
                                "unused",
                                "ntriples",
                                true,
                                false), // Delete the input file too
                        datasetLocation,
                        true,
                        true); // And delete the temp HDT updates file
            }
        } catch (Exception e) {
            System.err.println("Couldn't write back changes to the local HDT SPARQL endpoint:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Concatenate this HDT file with another, based on the way the HDTCat tool is implemented:
     * https:github.com/rdfhdt/hdt-java/blob/master/hdt-java-cli/src/main/java/org/rdfhdt/hdt/tools/HDTCat.java
     * <p>
     * Self-concatenations are allowed and handled internally, by deleting the existing index to avoid issues
     *
     * @param other       The other HDT endpoint
     * @param loc         Location to where the new HDT file will be saved. Can be any of the two endpoint locations, or a new one
     * @param deleteAfter If true, both original HDT files will be deleted (or only the second endpoint if it was a
     *                    self-concatenation). WARNING: Attempting to use it after that will be undefined behavior
     * @param indexed     If true, an index will be generated for the new endpoint. This is needed if the endpoint is to be queried
     * @return The new LocalHDTSPARQLEndpoint, containing the concatenation of both endpoints at the specified location
     * @throws IOException If any IO error occurs during the concatenation
     */
    public LocalHDTSPARQLEndpoint concatenate(LocalHDTSPARQLEndpoint other, String loc, boolean deleteAfter, boolean indexed) throws IOException {
        //System.out.println("Concatenating "+ this.datasetLocation + " and " + other.datasetLocation + " on " + loc);

        HDTOptions spec = HDTOptions.of();
        spec.set(HDTOptionsKeys.HDTCAT_LOCATION, new File(loc).getAbsolutePath() + "_tmp");

        boolean selfConcat = loc.equals(this.datasetLocation); // Check if it's a concatenation upon itself

        try (HDT hdt = HDTManager.catHDT(loc, this.datasetLocation, other.datasetLocation, spec, null)) {
            hdt.saveToHDT(loc, null);
            // Delete leftover temp files
            Files.deleteIfExists(Path.of(loc + "dictionary"));
            Files.deleteIfExists(Path.of(loc + "triples"));

            if (deleteAfter) {
                Files.deleteIfExists(Path.of(other.datasetLocation));
                Files.deleteIfExists(Path.of(other.datasetLocation + ".index.v1-1"));
                if (!selfConcat) { // Delete this dataset too
                    Files.deleteIfExists(Path.of(this.datasetLocation));
                    Files.deleteIfExists(Path.of(this.datasetLocation + ".index.v1-1"));
                }
            }

            // Delete the existing index too, it will regenerate it anyway since it will be invalid
            if (selfConcat) Files.deleteIfExists(Path.of(loc + ".index.v1-1"));
            return new LocalHDTSPARQLEndpoint(loc, baseURI, indexed);
        }
    }


    /**
     * Concatenate this HDT file with another, based on the way the HDTCat tool is implemented:
     * https:github.com/rdfhdt/hdt-java/blob/master/hdt-java-cli/src/main/java/org/rdfhdt/hdt/tools/HDTCat.java
     * <p>
     * THis version supports concatenating multiple endpoints via HDT's KCat algorithm
     * <p>
     * Self-concatenations are allowed and handled internally, by deleting the existing index to avoid issues
     *
     * @param endpoints   List of endpoints to concatenate, alongside this one
     * @param loc         Location to where the new HDT file will be saved. Can be any of the two endpoint locations, or a new one
     * @param deleteAfter If true, all original HDT files will be deleted (or only the other endpoints if it was a
     *                    self-concatenation). WARNING: Attempting to use it after that will be undefined behavior
     * @param indexed     If true, an index will be generated for the new endpoint. This is needed if the endpoint is to be queried
     * @return The new LocalHDTSPARQLEndpoint, containing the concatenation of both endpoints at the specified location
     * @throws IOException If any IO error occurs during the concatenation
     */
    public LocalHDTSPARQLEndpoint concatenate(List<LocalHDTSPARQLEndpoint> endpoints, String loc, boolean deleteAfter, boolean indexed) throws IOException {
        List<String> endpointLocations = new ArrayList<>();
        for (var endpoint : endpoints)
            endpointLocations.add(endpoint.datasetLocation);

        //System.out.println("Concatenating "+ this.datasetLocation + " and " + other.datasetLocation + " on " + loc);

        HDTOptions spec = HDTOptions.of();
        spec.set(HDTOptionsKeys.HDTCAT_LOCATION, new File(loc).getAbsolutePath() + "_tmp");

        boolean selfConcat = loc.equals(this.datasetLocation); // Check if it's a concatenation upon itself

        try (HDT hdt = HDTManager.catHDT(endpointLocations, spec, null)) {
            hdt.saveToHDT(loc, null);
            // Delete leftover temp files
            Files.deleteIfExists(Path.of(loc + "dictionary"));
            Files.deleteIfExists(Path.of(loc + "triples"));

            if (deleteAfter) {
                // For some reason, catHDT consumes the whole String list
                for (var endpoint : endpoints) {
                    Files.deleteIfExists(Path.of(endpoint.datasetLocation));
                    Files.deleteIfExists(Path.of(endpoint.datasetLocation + ".index.v1-1"));
                }

                if (!selfConcat) { // Delete this dataset too
                    Files.deleteIfExists(Path.of(this.datasetLocation));
                    Files.deleteIfExists(Path.of(this.datasetLocation + ".index.v1-1"));
                }
            }

            // Delete the existing index too, it will regenerate it anyway since it will be invalid
            if (selfConcat) Files.deleteIfExists(Path.of(loc + ".index.v1-1"));
            return new LocalHDTSPARQLEndpoint(loc, baseURI, indexed);
        }
    }

    @Override
    public void addVirtualTypes() throws IOException, ParserException {
        LongStream subjectsStream = LongStream.range(1, hdt.getDictionary().getNsubjects() + 1);
        PrintWriter tempFile = new PrintWriter(datasetLocation + "_virtual_types");

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        for (PrimitiveIterator.OfLong it = subjectsStream.iterator(); it.hasNext(); ) {
            long subjectID = it.next();
            CharSequence subject = hdt.getDictionary().idToString(subjectID, TripleComponentRole.SUBJECT);

            tempFile.println("<" + subject + ">" + " <" + RDF_TYPE_URI + "> <" + VIRTUAL_TYPE + "> .");
        }
        workers.close();

        // For all remaining URI objects (those outside shared)
        LongStream objectsStream = LongStream.range(hdt.getDictionary().getNshared() + 1, hdt.getDictionary().getNobjects() + 1);
        workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        for (PrimitiveIterator.OfLong it = objectsStream.iterator(); it.hasNext(); ) {
            long objectID = it.next();
            CharSequence object = hdt.getDictionary().idToString(objectID, TripleComponentRole.OBJECT);
            if (object.charAt(0) != '"') // It's not a literal
                tempFile.println("<" + object + ">" + " <" + RDF_TYPE_URI + "> <" + VIRTUAL_TYPE + "> .");
        }
        workers.close();

        tempFile.flush();
        tempFile.close();

        LocalHDTSPARQLEndpoint temp = this.concatenate(
                LocalHDTSPARQLEndpoint.fromRDF(
                        datasetLocation + "_virtual_types",
                        datasetLocation + "_virtual_types.hdt",
                        "unused",
                        "ntriples",
                        true,
                        false),
                this.datasetLocation,
                true,
                true);

        this.hdt = temp.hdt;
        this.datasetLocation = temp.datasetLocation;
        this.model = temp.model;
    }

    /** Utils **/

    /**
     * Concatenate a Deque of LocalHDTSPARQLEndpoints into a single one, deleting and invalidating the individual endpoints
     *
     * @param endpointsToConcatenate Deque of LocalHDTSPARQLEndpoints to concatenate
     * @return A LocalHDTSPARQLEndpoints which contains the contents of all endpoints
     * @throws IOException if an error occurs during HDT file handling
     *                     <p>
     *                     WARNING: All endpoints inside the queue will be invalidated, as it will delete their original files
     * @deprecated This is not needed anymore, but kept just in case. If any memory constraints appear in the future,
     * using LocalHDTSPARQLEndpoint.fromRDFCatTree should be preferable
     */
    @Deprecated
    public static LocalHDTSPARQLEndpoint concatenateEndpoints(Deque<LocalHDTSPARQLEndpoint> endpointsToConcatenate) throws IOException {
        if (endpointsToConcatenate.size() == 1) {
            return endpointsToConcatenate.pop();
        } else {
            LocalHDTSPARQLEndpoint first = endpointsToConcatenate.pop();
            if (!endpointsToConcatenate.isEmpty())
                // Do a KCat of all endpoints
                return first.concatenate(new ArrayList<>(endpointsToConcatenate), first.datasetLocation, true, false);
            else
                return first;
        }
    }

    /***
     * Build a LocalHDTSPARQLEndpoint from a TripleString iterator, doing all operations on disk
     *
     * @deprecated This is not needed anymore, but kept just in case. If any memory constraints appear in the future,
     *             using LocalHDTSPARQLEndpoint.fromRDFCatTree should be preferable
     */
    @Deprecated
    public static LocalHDTSPARQLEndpoint iteratorToHDT(Iterator<TripleString> tripleStringIterator,
                                                       String hdtFileLocation,
                                                       String baseURI,
                                                       boolean indexed) throws ParserException, IOException {
        // In this case we don't write the HDT file in-memory, since it can get big pretty easily for some metrics
        try (HDT hdt = HDTManager.generateHDTDisk(
                tripleStringIterator,
                baseURI,
                new HDTSpecification(),
                null
        )) {
            hdt.saveToHDT(hdtFileLocation, null);
            return new LocalHDTSPARQLEndpoint(hdtFileLocation, baseURI, indexed);
        }
    }
}

package sid.MetricsGeneration.HDT;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sid.MetricsGeneration.MetricsGenerator;
import sid.MetricsGeneration.PageRank.HDT.WeightedPageRankMetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;

import static sid.MetricsGeneration.SPARQL.InfoRankMetricsGenerator.*;

/**
 * HDT specialization for the InfoRank metrics, which saves only the non-intermediate metrics and works directly in
 * memory, without querying the metrics HDT itself, and is massively multithreaded.
 * <p>
 * Optimized for large KGs (tested on DBPedia 2016-10)
 */
public class InfoRankMetricsGenerator implements MetricsGenerator {
    private static final String TEMP_IW_HDT_FILE_SUFFIX = "_iw_temp";
    private static final String TEMP_ABS_C_HDT_FILE_SUFFIX = "_absC_temp";
    private static final String TEMP_ABS_D_HDT_FILE_SUFFIX = "_absD_temp";
    private static final String TEMP_ABS_P_HDT_FILE_SUFFIX = "_absP_temp";
    private static final String TEMP_IR_HDT_FILE_SUFFIX = "_ir_temp";
    private static final String TEMP_INFORANK_HDT_FILE_SUFFIX = "_inforank_temp";

    private static final String CONFIGURATION_FILE = "configuration/metricsConfiguration.json";
    public static final String METRICS_HDT_FILE_CONF = "metricsToHDTFile";

    public static final String INFORANK_NAMED_GRAPH_URI = "http://sid-unizar-search.com/infoRank";

    private LocalHDTSPARQLEndpoint endpoint;
    private WeightedPageRankMetricsGenerator pagerank;

    // HDT file to which we save all the metrics (Inforank intermediate metrics + Pagerank + InfoRank)
    private final String destinationHDTFile;

    public static InfoRankMetricsGenerator fromConfigurationFile(LocalHDTSPARQLEndpoint endpoint) throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new InfoRankMetricsGenerator(endpoint,
                // Defer the creation of WeightedPageRankMetricsGeneratorHDT until the HDT
                // file is created, as it should have the same metrics input HDT file
                null,
                rootNode.get(METRICS_HDT_FILE_CONF).asText());
    }

    public InfoRankMetricsGenerator(LocalHDTSPARQLEndpoint endpoint,
                                    WeightedPageRankMetricsGenerator pagerank,
                                    String destinationHDTFile) {
        this.endpoint = endpoint;
        this.pagerank = pagerank;
        this.destinationHDTFile = destinationHDTFile;
    }

    @Override
    public void run() {
        try {
            // Delete the previous index, in order to avoid invalid index exceptions 8we are going to modify it right away)
            Files.deleteIfExists(Path.of(destinationHDTFile + ".index.v1-1"));

            Instant start = Instant.now();
            System.out.println("Calculating IW(r)...");
            Map<Long, Long> iws = calculateIW();

            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating absolute informativeness of each class...");
            Map<Long, Long> absCs = calculateAbsC(iws);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating absolute informativeness of each object property...");
            Map<Long, Long> absPs = calculateAbsP(iws);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating absolute informativeness of each datatype property...");
            Map<Long, Long> absDs = calculateAbsD();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating IR(c)..");
            LocalHDTSPARQLEndpoint metricsHDT = calculateIR(absCs, TripleComponentRole.OBJECT, IR_C_URI);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            absCs.clear();
            System.gc(); // Hint that it can purge the entire map

            // Concatenate the first results over the existing metrics file
            if (Files.exists(Path.of(destinationHDTFile)))
                metricsHDT = new LocalHDTSPARQLEndpoint(destinationHDTFile, BASE_INFORANK_SUBGRAPH_URI, false).concatenate(metricsHDT, destinationHDTFile, true, false);

            start = Instant.now();
            System.out.println("Calculating IR(d)..");
            LocalHDTSPARQLEndpoint irDEndpoint = calculateIR(absDs, TripleComponentRole.PREDICATE, IR_D_URI);
            metricsHDT = metricsHDT.concatenate(irDEndpoint, destinationHDTFile, true, false);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            absDs.clear();
            System.gc(); // Hint that it can purge the entire map

            start = Instant.now();
            System.out.println("Calculating IR(p)..");
            LocalHDTSPARQLEndpoint irPEndpoint = calculateIR(absPs, TripleComponentRole.PREDICATE, IR_P_URI);
            metricsHDT = metricsHDT.concatenate(irPEndpoint, destinationHDTFile, true, false);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            absPs.clear();
            System.gc(); // Hint that it can purge the entire map

            if (pagerank == null) {
                // Instantiate it now that we have the input metrics HDT file
                pagerank = WeightedPageRankMetricsGenerator.fromConfigurationFile(metricsHDT);
            }

            System.out.println("Calculating PageRanks...");
            pagerank.compute();
            LocalHDTSPARQLEndpoint pageRankEndpoint = pagerank.writePageRankScoresHDT();
            metricsHDT = metricsHDT.concatenate(pageRankEndpoint, destinationHDTFile, true, true);

            System.out.println("Calculating Inforanks..");
            LocalHDTSPARQLEndpoint inforankEndpoint = calculateInfoRank(metricsHDT.hdt, iws);

            iws.clear();
            System.gc(); // Hint that it can purge the entire map

            System.out.println("Finished! Generating the final HDT (this may take a while...)");
            metricsHDT.concatenate(inforankEndpoint, destinationHDTFile, true, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Long, Long> calculateIW() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNsubjects() + 1).parallel();

        Map<Long, Long> iws = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(id -> { // For each possible subject
                    IteratorTripleID outgoingLinks = endpoint.hdt.getTriples().search(new TripleID(id, 0, 0));

                    long literalCount = 0;

                    while (outgoingLinks.hasNext()) {
                        TripleID nextLink = outgoingLinks.next();

                        long o = nextLink.getObject();

                        if (isObjectLiteral(o)) {
                            literalCount++;
                        }
                    }

                    iws.put(id, literalCount);
                })).get();

        workers.close();

        return iws;
    }

    private Map<Long, Long> calculateAbsC(Map<Long, Long> iws) throws ExecutionException, InterruptedException {
        long idOfTypePredicate = endpoint.hdt.getDictionary().stringToId(TYPE_URI, TripleComponentRole.PREDICATE);

        HashSet<Long> types = new HashSet<>();
        IteratorTripleID typesQuery = endpoint.hdt.getTriples().search(new TripleID(0, idOfTypePredicate, 0));
        while (typesQuery.hasNext()) {
            TripleID next = typesQuery.next();
            types.add(next.getObject());
        }

        Map<Long, Long> absCs = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                types.stream().parallel().forEach(classID -> { // For each known class
                    IteratorTripleID subjectsWithClass = endpoint.hdt.getTriples().search(new TripleID(0, idOfTypePredicate, classID));

                    long maxIW = 0;

                    while (subjectsWithClass.hasNext()) {
                        long subjectID = subjectsWithClass.next().getSubject();

                        // Look for this subject's IW
                        if (iws.containsKey(subjectID)) {
                            long iw = iws.get(subjectID);

                            if (iw > maxIW) {
                                maxIW = iw;
                            }
                        }
                    }

                    absCs.put(classID, maxIW);
                })).get();

        workers.close();

        return absCs;
    }

    private Map<Long, Long> calculateAbsP(Map<Long, Long> iws) throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();
        Map<Long, Long> absPs = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> { // For each predicate
                    IteratorTripleID triplesWithPredicate = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    // We want to find max(IW(r) + IW(s)) of all <r, p, s> triples, where p == predicateID
                    long maxSumOfIWs = -1;

                    while (triplesWithPredicate.hasNext()) {
                        TripleID next = triplesWithPredicate.next();
                        long subjectID = next.getSubject();
                        long objectID = next.getObject();

                        long iwSubject = iws.getOrDefault(subjectID, -1L);
                        long iwObject = -1;

                        // The same for the object, making sure that it's not a literal
                        if (!isObjectLiteral(objectID)) {
                            iwObject = iws.getOrDefault(objectID, -1L);
                        }

                        if (iwSubject != -1 && iwObject != -1) {
                            long sum = iwSubject + iwObject;
                            if (sum > maxSumOfIWs)
                                maxSumOfIWs = sum;
                        }

                    }

                    if (maxSumOfIWs != -1)
                        absPs.put(predicateID, maxSumOfIWs);
                })).get();

        workers.close();

        return absPs;
    }

    private Map<Long, Long> calculateAbsD() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();

        Map<Long, Long> absDs = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> { // For each predicate
                    IteratorTripleID objectsForPredicate = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    // We want to get the count of non-duplicate literals, so we have to keep track of them
                    HashSet<Long> objects = new HashSet<>();
                    while (objectsForPredicate.hasNext()) {
                        TripleID next = objectsForPredicate.next();
                        long object = next.getObject();

                        if (isObjectLiteral(object))
                            objects.add(next.getObject());
                    }

                    if (objects.size() > 0)
                        absDs.put(predicateID, (long) objects.size());
                })).get();

        workers.close();

        return absDs;
    }

    private LocalHDTSPARQLEndpoint calculateIR(Map<Long, Long> abs, TripleComponentRole role, String IrURI) throws ParserException, IOException, ExecutionException, InterruptedException, NotFoundException {
        long maxAbs = abs.values().stream().mapToLong(absValue -> absValue).max().orElse(0);

        Map<Long, Double> irs = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                abs.keySet().stream().parallel().forEach(entry -> {
                    double ir = (double) abs.get(entry) / (double) maxAbs;
                    irs.put(entry, ir);
                })).get();

        workers.close();

        return writeIRs(irs,
                IrURI,
                destinationHDTFile + TEMP_IR_HDT_FILE_SUFFIX,
                role,
                false);
    }

    private LocalHDTSPARQLEndpoint calculateInfoRank(HDT metricsHDT, Map<Long, Long> iws) throws ParserException, IOException, ExecutionException, InterruptedException {
        long idOfPageRankPredicate = metricsHDT.getDictionary().stringToId(PAGERANK_URI, TripleComponentRole.PREDICATE);

        ConcurrentHashMap<Long, Double> infoRanks = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                iws.keySet().stream().parallel().forEach(subjectID -> {
                    long idOfSubjectInMetrics = metricsHDT.getDictionary().stringToId(
                            endpoint.hdt.getDictionary().idToString(subjectID, TripleComponentRole.SUBJECT),
                            TripleComponentRole.SUBJECT);

                    double pagerank = -1;
                    // Get its PageRank
                    IteratorTripleID pageRankForSubjectQuery = metricsHDT.getTriples().search(new TripleID(idOfSubjectInMetrics, idOfPageRankPredicate, 0));
                    if (pageRankForSubjectQuery.hasNext()) {
                        TripleID next = pageRankForSubjectQuery.next();
                        pagerank = MetricsGenerator.floatLiteralToDouble(metricsHDT, next);
                    }

                    if (pagerank != -1) { // It exists
                        double inforank = iws.get(subjectID) * pagerank; // IW(s) * PR(s)
                        infoRanks.put(subjectID, inforank);
                    }
                })).get();

        workers.close();

        return writeIRs(infoRanks,
                INFORANK_URI,
                destinationHDTFile + TEMP_INFORANK_HDT_FILE_SUFFIX,
                TripleComponentRole.SUBJECT,
                true);
    }

    private boolean isObjectLiteral(long id) {
        if (id <= endpoint.hdt.getDictionary().getNshared()) return false; // We know it acts as a subject somewhere

        // We know it acts as an object, but not wether it's an URI or a literal
        CharSequence objectValue = endpoint.hdt.getDictionary().idToString(id, TripleComponentRole.OBJECT); // We ask for objects!

        // We can only do this if we are 100% sure that the ID is in the objects dict, otherwise we may get false
        // positives as the subjects+shared and object dict ranges overlap
        //CharSequence objectValue = endpoint.hdt.getDictionary().getObjects().extract(id);
        //if (objectValue == null) return false;

        //System.out.println("Is " + objectValue + " a literal?");

        return objectValue.charAt(0) == '"';
    }

    // If split == true, create the HDT file via CatTree (allows conversion of huge RDF files)
    private LocalHDTSPARQLEndpoint writeIRs(Map<Long, Double> map,
                                            String predicateURI,
                                            String hdtFileLocation,
                                            TripleComponentRole roleOfKeys,
                                            boolean split) throws ParserException, IOException {
        String tempFilename = hdtFileLocation + "_ir_temp.rdf";
        Files.deleteIfExists(Path.of(tempFilename));
        PrintWriter tempFile = new PrintWriter(tempFilename);

        for (var entry : map.entrySet()) {
            String s = endpoint.hdt.getDictionary().idToString(entry.getKey(), roleOfKeys).toString();

            tempFile.println("<" + s + ">" + " <" + predicateURI + "> " + MetricsGenerator.doubleToFloatLiteral(entry.getValue()) + " .");
        }

        tempFile.flush();
        tempFile.close();

        if (split) { // Only used when writing InfoRanks
            return LocalHDTSPARQLEndpoint.fromRDFCatTree(
                    hdtFileLocation + "_ir_temp.rdf",
                    hdtFileLocation,
                    "unused",
                    "ntriples",
                    false,
                    false);
        } else {
            return LocalHDTSPARQLEndpoint.fromRDF(
                    hdtFileLocation + "_ir_temp.rdf",
                    hdtFileLocation,
                    "unused",
                    "ntriples",
                    false,
                    false);
        }
    }

    // Return an endpoint with the metrics. Note that, if any HDT-backed metrics generator is run after this one,
    // it will be invalid (the underlying HDT file is the same, and it will have changed after concatenating said
    // generator's results)
    public LocalHDTSPARQLEndpoint getMetricsHDT(boolean indexed) throws IOException {
        return new LocalHDTSPARQLEndpoint(destinationHDTFile, BASE_INFORANK_SUBGRAPH_URI, indexed);
    }

    public String getDestinationHDTFile() {
        return destinationHDTFile;
    }
}

package sid.MetricsGeneration.HDT;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sid.MetricsGeneration.MetricsGenerator;
import sid.SPARQLEndpoint.LocalHDTSPARQLEndpoint;
import sid.utils.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static sid.MetricsGeneration.SPARQL.ImportanceMetricsGenerator.*;


/**
 * HDT specialization for the importance metrics, which saves only the non-intermediate metrics and works directly in
 * memory, without querying the metrics HDT itself, and is massively multithreaded.
 * <p>
 * Optimized for large KGs (tested on DBPedia 2016-10)
 */
public class ImportanceMetricsGenerator implements MetricsGenerator {
    private static final String CONFIGURATION_FILE = "configuration/metricsConfiguration.json";
    public static final String METRICS_HDT_FILE_CONF = "metricsToHDTFile";

    // Maximum (Approx.) number of triples to write into a single HDT file. Writes to HDT files are batched and then
    // concatenated
    private static final int TRIPLES_PER_BATCH = 1000000;

    private final LocalHDTSPARQLEndpoint endpoint;

    // HDT file to which we save all the metrics
    private final String destinationHDTFile;

    // Only for subsuming predicates
    //private final HashMap<Long, Set<Long>> subsumingPredicates;
    //private final Set<Long> subsumedPredicates;

    public static ImportanceMetricsGenerator fromConfigurationFile(LocalHDTSPARQLEndpoint endpoint) throws IOException {
        byte[] mapData = Files.readAllBytes(Paths.get(CONFIGURATION_FILE));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(mapData);

        return new ImportanceMetricsGenerator(endpoint,
                rootNode.get(METRICS_HDT_FILE_CONF).asText());
    }

    public ImportanceMetricsGenerator(LocalHDTSPARQLEndpoint endpoint, String destinationHDTFile) throws IOException {
        this.endpoint = endpoint;
        this.destinationHDTFile = destinationHDTFile;

        // Only for subsuming predicates
        /*Pair<HashMap<Long, Set<Long>>, Set<Long>> pair = getSubsumedPredicates(this.endpoint);
        subsumingPredicates = pair.getKey();
        subsumedPredicates = pair.getValue();*/
    }

    @Override
    public void run() {
        try {
            // Delete the previous index, in order to avoid invalid index exceptions (8)we are going to modify it right away)
            Files.deleteIfExists(Path.of(destinationHDTFile + ".index.v1-1"));

            Instant start;
            // We calculate intermediate metrics sequentially, and save to the metrics HDT the ones we are actually going
            // to use
            // Most intermediate metrics could be collapsed inside the functions that use them, but I have left them
            // outside for visibility and easier debugging. Those that are collapsed are for memory usage reasons,
            // and can be found inside the metrics with "Integrated" suffixes
            //
            // Metrics written to HDT:
            //      typeImportance (Note: In this step, we also write the Pred-Type triples)
            //      predicateEntropyType
            //      entropyTypeImportance
            //      entityTypeImportance
            //      entropyEntityTypeImportance

            // Original importance intermediate metrics, unused for now
            /*start = Instant.now();
            System.out.println("Calculating Fact frequency...");
            Map<Long, HashMap<Long, Long>>  factFrequency = calculateFactFrequency();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Fact frequency P...");
            Map<Long, Long> factFreqPEndpoint = calculateFactFrequencyP();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Fact frequency O...");
            Map<Long, Long> factFreqOs = calculateFactFrequencyO();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Entity frequency...");
            Map<Long, HashMap<Long, Long>> entityFrequencies = calculateEntityFrequency();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Entity frequency P...");
            Map<Long, Long> entityFrequencyPs = calculateEntityFrequencyP();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Entity frequency O...");
            Map<Long, Long> entityFrequencyOs = calculateEntityFrequencyO();
            System.out.println("Time: " + Duration.between(start, Instant.now()));*/

            start = Instant.now();
            System.out.println("Calculating Entity Type Frequency (this will take more time)...");
            Map<Long, HashMap<Long, Long>> entityFreqTypes = calculateEntityTypeFrequency();

            // Only used for subsuming
            /*for (long predID : entityFreqTypes.keySet()) {
                if (subsumingPredicates.containsKey(predID)) {
                    var predicatesToSubsume = subsumingPredicates.get(predID);
                    var entityTypeFrequenciesOfSubsumer = entityFreqTypes.get(predID);

                    for (long predicateToSubsumeID : predicatesToSubsume) {
                        if (entityFreqTypes.containsKey(predicateToSubsumeID)) {
                            var entityTypeFrequenciesOfSubsumed = entityFreqTypes.get(predicateToSubsumeID);
                            for (long typeID : entityTypeFrequenciesOfSubsumed.keySet()) {
                                if (entityTypeFrequenciesOfSubsumer.containsKey(typeID)) {
                                    entityTypeFrequenciesOfSubsumer.merge(typeID, entityTypeFrequenciesOfSubsumed.get(typeID), Long::sum);
                                } else {
                                    entityTypeFrequenciesOfSubsumer.put(typeID, entityTypeFrequenciesOfSubsumed.get(typeID));
                                }
                            }
                        }
                    }
                }
            }*/

            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Type frequency of predicates...");
            Map<Long, Long> typeFrequencyPs = calculateTypeFrequencyP();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            start = Instant.now();
            System.out.println("Calculating Type importance...");
            Map<Long, Map<Long, Double>> typeImportances = calculateTypeImportance(typeFrequencyPs, entityFreqTypes);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            typeFrequencyPs.clear();
            System.gc(); // Hint that it can purge the entire map

            LocalHDTSPARQLEndpoint metricsHDT = writeTypeImportancesToHDT(typeImportances);
            // Concatenate the first results over the existing metrics file
            if (Files.exists(Path.of(destinationHDTFile)))
                metricsHDT = new LocalHDTSPARQLEndpoint(destinationHDTFile, BASE_IMPORTANCE_SUBGRAPH_URI, false).concatenate(metricsHDT, destinationHDTFile, true, false);

            start = Instant.now();
            System.out.println("Calculating Predicate Entropy type...");
            Map<Long, Map<Long, Double>> predicateEntropyTypes = calculatePredicateEntropyTypeIntegrated();
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            metricsHDT = metricsHDT.concatenate(writePredicateEntropyTypesToHDT(predicateEntropyTypes), destinationHDTFile, true, false);

            start = Instant.now();
            System.out.println("Calculating Entropy Type Importance...");
            Map<Long, Map<Long, Double>> entropyTypeImportances = calculateEntropyTypeImportance(predicateEntropyTypes, typeImportances);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            typeImportances.clear();
            System.gc(); // Hint that it can purge the entire map

            metricsHDT = metricsHDT.concatenate(writePredicateEntropyTypeImportancesToHDT(entropyTypeImportances), destinationHDTFile, true, false);

            entropyTypeImportances.clear();
            System.gc(); // Hint that it can purge the entire map

            start = Instant.now();
            System.out.println("Calculating Entity Type Importance...");
            Map<Long, Map<Long, Double>> entityTypeImportances = calculateEntityTypeImportance(entityFreqTypes);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            entityFreqTypes.clear();
            System.gc(); // Hint that it can purge the entire map
            metricsHDT = metricsHDT.concatenate(writeEntityTypeImportancesToHDT(entityTypeImportances), destinationHDTFile, true, false);

            start = Instant.now();
            System.out.println("Calculating Entropy Entity Type Importance...");
            Map<Long, Map<Long, Double>> entropyEntityTypeImportances = calculateEntropyEntityTypeImportance(entityTypeImportances, predicateEntropyTypes);
            System.out.println("Time: " + Duration.between(start, Instant.now()));

            predicateEntropyTypes.clear();
            entityTypeImportances.clear();

            System.out.println("Finished! Generating the final HDT (this may take a while...)");
            metricsHDT.concatenate(writeEntropyEntityTypeImportancesToHDT(entropyEntityTypeImportances), destinationHDTFile, true, false);

            entropyEntityTypeImportances.clear();
            System.gc(); // Hint that it can purge the entire map
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private LocalHDTSPARQLEndpoint writeTypeImportancesToHDT(
            // Map of predicate ID -> Map of type ID -> type importance
            Map<Long, Map<Long, Double>> predTypeImportances) throws IOException, ParserException, NotFoundException {

        PrintWriter tempFile = new PrintWriter(destinationHDTFile + "_type_importances_temp_rdf");

        for (var entry : predTypeImportances.entrySet()) {
            long predicateID = entry.getKey();
            String pString = endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString();
            Map<Long, Double> importances = entry.getValue();

            for (var typeImportance : importances.entrySet()) {
                long typeID = typeImportance.getKey();
                String typeString = endpoint.hdt.getDictionary().idToString(typeID, TripleComponentRole.OBJECT).toString();
                String predTypeURI = PREDICATE_TYPE_URI_LOWERCASE + "/" + DigestUtils.md5Hex(pString + "-" + typeString);

                tempFile.println("<" + predTypeURI + ">" + " <" + RDF_TYPE_URI + "> " + "<" + PREDICATE_TYPE_URI + "> .");
                tempFile.println("<" + predTypeURI + ">" + " <" + PREDICATE_URI + "> " + "<" + pString + "> .");
                tempFile.println("<" + predTypeURI + ">" + " <" + TYPE_URI + "> " + "<" + typeString + "> .");
                tempFile.println("<" + predTypeURI + ">" + " <" + TYPE_IMPORTANCE_URI + "> " + MetricsGenerator.doubleToFloatLiteral(typeImportance.getValue()) + " .");
            }
        }

        tempFile.flush();
        tempFile.close();

        return LocalHDTSPARQLEndpoint.fromRDF(
                destinationHDTFile + "_type_importances_temp_rdf",
                destinationHDTFile + "_type_importances_temp",
                "unused",
                "ntriples",
                true,
                false);
    }

    private LocalHDTSPARQLEndpoint writePredicateEntropyTypesToHDT(
            // Map of predicate ID -> Map of type ID -> predicateEntropyType
            Map<Long, Map<Long, Double>> predicateEntropyTypes) throws IOException, ParserException, NotFoundException {
        PrintWriter tempFile = new PrintWriter(destinationHDTFile + "_predicate_entropy_type_temp_rdf");

        for (var entry : predicateEntropyTypes.entrySet()) {
            long predicateID = entry.getKey();
            String pString = endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString();
            Map<Long, Double> entropyTypes = entry.getValue();

            for (var entropyType : entropyTypes.entrySet()) {
                long typeID = entropyType.getKey();
                String typeString = endpoint.hdt.getDictionary().idToString(typeID, TripleComponentRole.OBJECT).toString();
                String predTypeURI = PREDICATE_TYPE_URI_LOWERCASE + "/" + DigestUtils.md5Hex(pString + "-" + typeString);

                tempFile.println("<" + predTypeURI + ">" + " <" + PREDICATE_ENTROPY_TYPE_URI + "> " + MetricsGenerator.doubleToFloatLiteral(entropyType.getValue()) + " .");
            }
        }

        tempFile.flush();
        tempFile.close();

        return LocalHDTSPARQLEndpoint.fromRDF(
                destinationHDTFile + "_predicate_entropy_type_temp_rdf",
                destinationHDTFile + "_predicate_entropy_type_temp",
                "unused",
                "ntriples",
                true,
                false);
    }

    private LocalHDTSPARQLEndpoint writePredicateEntropyTypeImportancesToHDT(
            // Map of predicate ID -> type ID -> entropyTypeImportance
            Map<Long, Map<Long, Double>> predicateEntropyTypeImportances) throws IOException, ParserException, NotFoundException {
        PrintWriter tempFile = new PrintWriter(destinationHDTFile + "_predicate_entropy_type_importance_temp_rdf");

        for (var entry : predicateEntropyTypeImportances.entrySet()) {
            long predicateID = entry.getKey();
            String pString = endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString();
            Map<Long, Double> predicateEntropyTypeImportancesForP = entry.getValue();

            for (var entropyTypeImportance : predicateEntropyTypeImportancesForP.entrySet()) {
                long typeID = entropyTypeImportance.getKey();
                String typeString = endpoint.hdt.getDictionary().idToString(typeID, TripleComponentRole.OBJECT).toString();
                String predTypeURI = PREDICATE_TYPE_URI_LOWERCASE + "/" + DigestUtils.md5Hex(pString + "-" + typeString);

                tempFile.println("<" + predTypeURI + ">" + " <" + ENTROPY_TYPE_IMPORTANCE_URI + "> " + MetricsGenerator.doubleToFloatLiteral(entropyTypeImportance.getValue()) + " .");
            }
        }

        tempFile.flush();
        tempFile.close();

        return LocalHDTSPARQLEndpoint.fromRDF(
                destinationHDTFile + "_predicate_entropy_type_importance_temp_rdf",
                destinationHDTFile + "_predicate_entropy_type_importance_temp",
                "unused",
                "ntriples",
                true,
                false);
    }

    private LocalHDTSPARQLEndpoint writeEntityTypeImportancesToHDT(
            // Map of predicate ID -> type ID -> entityTypeImportance
            Map<Long, Map<Long, Double>> entityTypeImportances) throws IOException, ParserException, NotFoundException {
        PrintWriter tempFile = new PrintWriter(destinationHDTFile + "_predicate_entity_type_importance_temp_rdf");

        for (var entry : entityTypeImportances.entrySet()) {
            long predicateID = entry.getKey();
            String pString = endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString();
            Map<Long, Double> entityTypeImportancesForP = entry.getValue();

            for (var entityTypeImportance : entityTypeImportancesForP.entrySet()) {
                long typeID = entityTypeImportance.getKey();
                String typeString = endpoint.hdt.getDictionary().idToString(typeID, TripleComponentRole.OBJECT).toString();
                String predTypeURI = PREDICATE_TYPE_URI_LOWERCASE + "/" + DigestUtils.md5Hex(pString + "-" + typeString);

                tempFile.println("<" + predTypeURI + ">" + " <" + ENTITY_TYPE_IMPORTANCE_URI + "> " + MetricsGenerator.doubleToFloatLiteral(entityTypeImportance.getValue()) + " .");
            }
        }

        tempFile.flush();
        tempFile.close();

        return LocalHDTSPARQLEndpoint.fromRDF(
                destinationHDTFile + "_predicate_entity_type_importance_temp_rdf",
                destinationHDTFile + "_predicate_entity_type_importance_temp",
                "unused",
                "ntriples",
                true,
                false);
    }

    private LocalHDTSPARQLEndpoint writeEntropyEntityTypeImportancesToHDT(
            // Map of predicate ID -> type ID -> entropyEntityTypeImportance
            Map<Long, Map<Long, Double>> entropyEntityTypeImportances) throws IOException, ParserException, NotFoundException {
        PrintWriter tempFile = new PrintWriter(destinationHDTFile + "_predicate_entropy_entity_type_importance_temp_rdf");

        for (var entry : entropyEntityTypeImportances.entrySet()) {
            long predicateID = entry.getKey();
            String pString = endpoint.hdt.getDictionary().idToString(predicateID, TripleComponentRole.PREDICATE).toString();
            Map<Long, Double> entropyEntityTypeImportancesForP = entry.getValue();


            for (var entropyEntityTypeImportance : entropyEntityTypeImportancesForP.entrySet()) {
                long typeID = entropyEntityTypeImportance.getKey();
                String typeString = endpoint.hdt.getDictionary().idToString(typeID, TripleComponentRole.OBJECT).toString();
                String predTypeURI = PREDICATE_TYPE_URI_LOWERCASE + "/" + DigestUtils.md5Hex(pString + "-" + typeString);

                tempFile.println("<" + predTypeURI + ">" + " <" + ENTROPY_ENTITY_TYPE_IMPORTANCE_URI + "> " + MetricsGenerator.doubleToFloatLiteral(entropyEntityTypeImportance.getValue()) + " .");
            }
        }

        tempFile.flush();
        tempFile.close();

        return LocalHDTSPARQLEndpoint.fromRDF(
                destinationHDTFile + "_predicate_entropy_entity_type_importance_temp_rdf",
                destinationHDTFile + "_predicate_entropy_entity_type_importance_temp",
                "unused",
                "ntriples",
                true,
                false);
    }

    private Map<Long, HashMap<Long, Long>> calculateFactFrequency() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();


        // Map of predicate ID -> Frequencies for each object
        ConcurrentHashMap<Long, HashMap<Long, Long>> factFrequencies = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> {
                    IteratorTripleID factsQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    HashMap<Long, Long> objectFrequencies = new HashMap<>();

                    while (factsQuery.hasNext()) {
                        objectFrequencies.merge(factsQuery.next().getObject(), 1L, Long::sum);
                    }

                    factFrequencies.put(predicateID, objectFrequencies);
                })).get();

        workers.close();


        return factFrequencies;
    }

    private Map<Long, Long> calculateFactFrequencyP() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();


        // Map of predicate ID -> Frequencies for each object
        ConcurrentHashMap<Long, Long> factFrequenciesP = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> {
                    IteratorTripleID factsQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    long count = 0;
                    while (factsQuery.hasNext()) {
                        factsQuery.next();
                        count++;
                    }

                    factFrequenciesP.put(predicateID, count);
                })).get();

        workers.close();

        return factFrequenciesP;
    }

    private Map<Long, Long> calculateFactFrequencyO() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNobjects() + 1).parallel();

        // Map of predicate ID -> Frequencies for each object
        ConcurrentHashMap<Long, Long> factFrequenciesO = new ConcurrentHashMap<>();


        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(objectID -> {
                    IteratorTripleID objQuery = endpoint.hdt.getTriples().search(new TripleID(0, 0, objectID));

                    long count = 0;
                    while (objQuery.hasNext()) {
                        objQuery.next();
                        count++;
                    }

                    factFrequenciesO.put(objectID, count);
                })).get();

        workers.close();

        return factFrequenciesO;
    }


    private Map<Long, HashMap<Long, Long>> calculateEntityFrequency() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();

        // Map of predicate ID -> Map of object -> entity frequency
        ConcurrentHashMap<Long, HashMap<Long, Long>> entityFrequencies = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> {
                    HashMap<Long, Long> subjectFrequencies = new HashMap<>();

                    HashSet<Long> objectsForPredicate = new HashSet<>();
                    IteratorTripleID objectsForPredicateQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));
                    while (objectsForPredicateQuery.hasNext()) {
                        objectsForPredicate.add(objectsForPredicateQuery.next().getObject());
                    }

                    for (long o : objectsForPredicate) {
                        IteratorTripleID entitiesForFactQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, o));

                        HashSet<Long> entities = new HashSet<>();
                        while (entitiesForFactQuery.hasNext()) {
                            entities.add(entitiesForFactQuery.next().getSubject());
                        }

                        subjectFrequencies.put(o, (long) entities.size());
                    }

                    entityFrequencies.put(predicateID, subjectFrequencies);
                })).get();

        workers.close();

        return entityFrequencies;
    }

    private Map<Long, Long> calculateEntityFrequencyP() throws ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();

        // Map of predicate ID -> Map of object -> entity frequency
        ConcurrentHashMap<Long, Long> entityFrequenciesP = new ConcurrentHashMap<>();


        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> {
                    IteratorTripleID entitiesForFactQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    HashSet<Long> entities = new HashSet<>();
                    while (entitiesForFactQuery.hasNext()) {
                        entities.add(entitiesForFactQuery.next().getSubject());
                    }

                    entityFrequenciesP.put(predicateID, (long) entities.size());
                })).get();

        workers.close();

        return entityFrequenciesP;
    }

    private Map<Long, Long> calculateEntityFrequencyO() throws ParserException, IOException, ExecutionException, InterruptedException {
        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNobjects() + 1).parallel();

        // Map of predicate ID -> Map of object -> entity frequency
        ConcurrentHashMap<Long, Long> entityFrequenciesO = new ConcurrentHashMap<>();


        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(objectID -> {
                    IteratorTripleID subjectsForObjectQuery = endpoint.hdt.getTriples().search(new TripleID(0, 0, objectID));

                    HashSet<Long> subjects = new HashSet<>();
                    while (subjectsForObjectQuery.hasNext()) {
                        subjects.add(subjectsForObjectQuery.next().getSubject());
                    }

                    entityFrequenciesO.put(objectID, (long) subjects.size());
                })).get();

        workers.close();

        return entityFrequenciesO;
    }

    private Map<Long, HashMap<Long, Long>> calculateEntityTypeFrequency() throws ExecutionException, InterruptedException {
        LongStream subjectsStream = LongStream.range(1, endpoint.hdt.getDictionary().getNsubjects() + 1).parallel();
        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();
        long idOfTypePredicate = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);

        // Map of predicate ID -> Map of type ID -> subject frequency
        ConcurrentHashMap<Long, HashMap<Long, Long>> entityTypeFrequencies = new ConcurrentHashMap<>();
        // Map of subject ID -> list of type IDs
        ConcurrentHashMap<Long, HashSet<Long>> subjectsAndTypes = new ConcurrentHashMap<>();

        AtomicInteger sCount = new AtomicInteger(1);
        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                subjectsStream.forEach(subjectID -> {
                    System.out.print("\rSubject " + sCount.getAndIncrement() + "/" + endpoint.hdt.getDictionary().getNsubjects() + " processed...");
                    HashSet<Long> types = new HashSet<>();
                    IteratorTripleID typesOfSubjectQuery = endpoint.hdt.getTriples().search(new TripleID(subjectID, idOfTypePredicate, 0));
                    while (typesOfSubjectQuery.hasNext()) {
                        types.add(typesOfSubjectQuery.next().getObject());
                    }

                    if (!types.isEmpty())
                        subjectsAndTypes.put(subjectID, types);
                })).get();
        workers.close();

        System.out.println();

        AtomicInteger pCount = new AtomicInteger(1);
        workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                predicatesStream.forEach(predicateID -> {
                    System.out.print("\rPredicate " + pCount.getAndIncrement() + "/" + endpoint.hdt.getDictionary().getNpredicates() + " processed...");
                    HashMap<Long, Long> typesWithSubjectFrequencies = new HashMap<>();

                    for (var entry : subjectsAndTypes.entrySet()) {
                        long s = entry.getKey();

                        // If it appears somewhere with this predicate, increment the count for each one of its types
                        IteratorTripleID subjectWithPredQuery = endpoint.hdt.getTriples().search(new TripleID(s, predicateID, 0));
                        if (subjectWithPredQuery.hasNext()) {
                            for (long type : entry.getValue()) {
                                typesWithSubjectFrequencies.merge(type, 1L, Long::sum);
                            }
                        }
                    }

                    if (!typesWithSubjectFrequencies.isEmpty())
                        entityTypeFrequencies.put(predicateID, typesWithSubjectFrequencies);
                })).get();

        workers.close();

        System.out.println();

        return entityTypeFrequencies;
    }

    private Map<Long, Long> calculateTypeFrequencyP() throws ExecutionException, InterruptedException {
        long idOfTypePredicate = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);

        LongStream stream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();

        // Map of predicate ID -> type frequency
        ConcurrentHashMap<Long, Long> typeFrequencyPs = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                stream.forEach(predicateID -> {
                    IteratorTripleID subjectsWithPred = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));

                    HashSet<Long> subjects = new HashSet<>();
                    while (subjectsWithPred.hasNext()) {
                        subjects.add(subjectsWithPred.next().getSubject());
                    }

                    HashSet<Long> types = new HashSet<>();
                    for (long subject : subjects) {
                        IteratorTripleID typesOfSubject = endpoint.hdt.getTriples().search(new TripleID(subject, idOfTypePredicate, 0));
                        while (typesOfSubject.hasNext()) {
                            types.add(typesOfSubject.next().getObject());
                        }
                    }

                    // Only used for subsuming
                    /*if (subsumingPredicates.containsKey(predicateID)) {
                        var predicatesToSubsume = subsumingPredicates.get(predicateID);

                        for (long predicateToSubsumeID : predicatesToSubsume) {
                            subjectsWithPred = endpoint.hdt.getTriples().search(new TripleID(0, predicateToSubsumeID, 0));

                            HashSet<Long> subjectsSubsumed = new HashSet<>();
                            while (subjectsWithPred.hasNext()) {
                                subjectsSubsumed.add(subjectsWithPred.next().getSubject());
                            }

                            for (long subject : subjectsSubsumed) {
                                IteratorTripleID typesOfSubject = endpoint.hdt.getTriples().search(new TripleID(subject, idOfTypePredicate, 0));
                                while (typesOfSubject.hasNext()) {
                                    types.add(typesOfSubject.next().getObject());
                                }
                            }
                        }
                    }*/

                    if (!types.isEmpty()) // Predicates with 0 type frequency are redundant
                        typeFrequencyPs.put(predicateID, (long) types.size());
                })).get();

        workers.close();

        return typeFrequencyPs;
    }

    private Map<Long, Map<Long, Double>> calculateTypeImportance(
            // Map of predicate ID -> type frequency
            Map<Long, Long> typeFrequencyPs,
            // Map of predicate ID -> Map of type ID -> subject frequency
            Map<Long, HashMap<Long, Long>> entityFreqTypes) throws ExecutionException, InterruptedException {
        HashSet<Long> types = getTypesInKG();
        long nTypes = types.size();
        types.clear();

        // Map of predicate ID -> Map of type ID -> type importance
        Map<Long, Map<Long, Double>> predTypeImportances = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                entityFreqTypes.keySet().stream().parallel().forEach(predicateID -> {
                    long typeFrequencyP = typeFrequencyPs.getOrDefault(predicateID, 0L);

                    // Map of type ID -> type importance
                    Map<Long, Double> typeImportancesForP = new HashMap<>();

                    for (var entryForFact : entityFreqTypes.get(predicateID).entrySet()) {
                        long typeID = entryForFact.getKey();
                        long entityFrequency = entryForFact.getValue();

                        double typeImportance = 0.0;
                        if (typeFrequencyP != 0)
                            typeImportance = (double) entityFrequency * Math.log(((double) nTypes) / (double) typeFrequencyP);

                        typeImportancesForP.put(typeID, typeImportance);
                    }

                    predTypeImportances.put(predicateID, typeImportancesForP);
                })).get();

        workers.close();

        return predTypeImportances;
    }

    private Map<Long, Long> getSubjectCountsForType(Map<Long, HashSet<Long>> typesPerSubject, long predicateID) {
        Map<Long, Long> subjectCountsForType = new HashMap<>();

        // Now we don't care whether they are duplicate or not, so we don't store them in a Set
        IteratorTripleID subjectsWithFact = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));
        while (subjectsWithFact.hasNext()) {
            long subjectID = subjectsWithFact.next().getSubject();

            HashSet<Long> typesForSubject = typesPerSubject.get(subjectID);
            if (typesForSubject != null) {
                for (long type : typesForSubject) {
                    subjectCountsForType.merge(type, 1L, Long::sum);
                }
            }
        }

        // Only used for subsuming
        /*if (subsumingPredicates.containsKey(predicateID)) {
            var predicatesToSubsume = subsumingPredicates.get(predicateID);

            for (long predicateToSubsumeID : predicatesToSubsume) {
                subjectsWithFact = endpoint.hdt.getTriples().search(new TripleID(0, predicateToSubsumeID, 0));
                while (subjectsWithFact.hasNext()) {
                    long subjectID = subjectsWithFact.next().getSubject();

                    HashSet<Long> typesForSubject = typesPerSubject.get(subjectID);
                    if (typesForSubject != null) {
                        for (long type : typesForSubject) {
                            subjectCountsForType.merge(type, 1L, Long::sum);
                        }
                    }
                }
            }
        }*/

        return subjectCountsForType;
    }

    private static Map<Long, Map<Long, Double>> getFactTypeProbabilitiesForP(Map<Long, Long> factFrequencyP, Map<Long, Map<Long, Long>> factTypeFrequenciesForP) {
        Map<Long, Map<Long, Double>> factTypeProbabilitiesForP = new HashMap<>();

        for (var factType : factTypeFrequenciesForP.entrySet()) {
            // Map of type ID-> factTypeProbability
            Map<Long, Double> typeProbabilities = new HashMap<>();

            long objectID = factType.getKey();

            for (var typeFrequency : factType.getValue().entrySet()) {
                long typeID = typeFrequency.getKey();
                long factTypeFrequency = typeFrequency.getValue();
                long factTypeFrequencyP = factFrequencyP.get(typeID);

                if (factTypeFrequencyP != 0)
                    typeProbabilities.put(typeID, ((double) factTypeFrequency / (double) factTypeFrequencyP));
                else
                    typeProbabilities.put(typeID, 0.0);
            }

            factTypeProbabilitiesForP.put(objectID, typeProbabilities);
        }
        return factTypeProbabilitiesForP;
    }

    private Map<Long, Map<Long, Long>> getFactTypeFrequenciesForP(Map<Long, HashSet<Long>> typesPerSubject, long predicateID) {
        // Map of object -> typeID -> frequencies
        Map<Long, Map<Long, Long>> factTypeFrequenciesForP = new HashMap<>();

        HashSet<Long> objects = new HashSet<>();
        IteratorTripleID objectsForPredicateQuery = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, 0));
        while (objectsForPredicateQuery.hasNext()) {
            objects.add(objectsForPredicateQuery.next().getObject());
        }

        // We iterate like this as we want the count of types among all subjects associated with the fact <predicateID, objectID>
        for (long objectID : objects) {
            IteratorTripleID subjectsForFact = endpoint.hdt.getTriples().search(new TripleID(0, predicateID, objectID));
            // Map of typeID -> frequencies
            Map<Long, Long> typeFrequenciesForFact = new HashMap<>();

            while (subjectsForFact.hasNext()) {
                long subjectID = subjectsForFact.next().getSubject();

                Set<Long> typesForSubject = typesPerSubject.get(subjectID);

                if (typesForSubject != null) { // Make sure that this subject is typed, otherwise it's redundant
                    for (long typeID : typesForSubject) {
                        typeFrequenciesForFact.merge(typeID, 1L, Long::sum);
                    }
                }
            }

            if (!typeFrequenciesForFact.isEmpty())
                factTypeFrequenciesForP.put(objectID, typeFrequenciesForFact);
        }

        objects.clear();
        return factTypeFrequenciesForP;
    }

    // calculatePredicateEntropyType, but calculating FactTypeProbability online, for every predicate (in order to avoid OOMs)
    private Map<Long, Map<Long, Double>> calculatePredicateEntropyTypeIntegrated() throws ExecutionException, InterruptedException {
        //long idOfVirtualType = endpoint.hdt.getDictionary().stringToId(endpoint.VIRTUAL_TYPE, TripleComponentRole.OBJECT);
        LongStream subjectsStream = LongStream.range(1, endpoint.hdt.getDictionary().getNsubjects() + 1).parallel();
        long idOfTypePredicate = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);

        // Map of subject ID -> type IDs
        Map<Long, HashSet<Long>> subjectsAndTypes = new ConcurrentHashMap<>();

        System.out.println("Caching subject types...");
        AtomicInteger sCount = new AtomicInteger(1);
        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                subjectsStream.forEach(subjectID -> {
                    System.out.print("\rSubject " + sCount.getAndIncrement() + "/" + endpoint.hdt.getDictionary().getNsubjects() + " processed...");
                    HashSet<Long> types = new HashSet<>();
                    IteratorTripleID typesOfSubjectQuery = endpoint.hdt.getTriples().search(new TripleID(subjectID, idOfTypePredicate, 0));
                    while (typesOfSubjectQuery.hasNext()) {
                        types.add(typesOfSubjectQuery.next().getObject());
                    }

                    if (!types.isEmpty())
                        subjectsAndTypes.put(subjectID, types);
                })).get();
        workers.close();

        LongStream predicatesStream = LongStream.range(1, endpoint.hdt.getDictionary().getNpredicates() + 1).parallel();

        // Map of predicate ID -> type ID -> shannon's entropy
        Map<Long, Map<Long, Double>> predTypeEntropies = new ConcurrentHashMap<>();

        AtomicInteger pCount = new AtomicInteger(1);

        workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                predicatesStream.forEach(predicateID -> {
                    System.out.print("\rPredicate " + pCount.getAndIncrement() + "/" + endpoint.hdt.getDictionary().getNpredicates() + " processed...");

                    // Map of type ID -> shannon's entropy
                    Map<Long, Double> typeEntropiesForP = new HashMap<>();

                    // Map of Type  t -> Number of subjects of type t within a fact containing predicateID
                    Map<Long, Long> factFrequencyP = getSubjectCountsForType(subjectsAndTypes, predicateID);

                    // Map of object ID -> Map of typeID -> frequencies
                    Map<Long, Map<Long, Long>> factTypeFrequenciesForP = getFactTypeFrequenciesForP(subjectsAndTypes, predicateID);

                    // Only used for subsuming
                    /*if (subsumingPredicates.containsKey(predicateID)) {
                        var predicatesToSubsume = subsumingPredicates.get(predicateID);

                        for (long predicateToSubsumeID : predicatesToSubsume) {
                             Map<Long, Map<Long, Long>> factTypeFrequenciesForPSubsumed = getFactTypeFrequenciesForP(subjectsAndTypes, predicateToSubsumeID);
                             for (long objectID : factTypeFrequenciesForPSubsumed.keySet()) {
                                 if (factTypeFrequenciesForP.containsKey(objectID)) {
                                     for (var typeID : factTypeFrequenciesForPSubsumed.get(objectID).keySet()) {
                                         factTypeFrequenciesForP.get(objectID).merge(typeID, factTypeFrequenciesForPSubsumed.get(objectID).get(typeID), Long::sum);
                                     }
                                 } else {
                                     factTypeFrequenciesForP.put(objectID, factTypeFrequenciesForPSubsumed.get(objectID));
                                 }
                             }
                        }
                    }*/

                    // Map of object ID -> type ID -> factTypeProbability
                    Map<Long, Map<Long, Double>> factTypeProbabilitiesForP = getFactTypeProbabilitiesForP(factFrequencyP, factTypeFrequenciesForP);

                    factFrequencyP.clear();
                    factTypeFrequenciesForP.clear();


                    // Check all types associated to facts containing this predicate
                    for (var factType : factTypeProbabilitiesForP.entrySet()) {
                        //long objectID = factType.getKey();

                        for (var typeProbability : factType.getValue().entrySet()) {
                            long typeID = typeProbability.getKey();
                            double factTypeProbability = typeProbability.getValue();

                            double singleEntropy = Math.log(factTypeProbability) / Math.log(2);
                            typeEntropiesForP.merge(typeID, factTypeProbability * singleEntropy, Double::sum); // p(x) * log_2(p(x))
                        }
                    }

                    factTypeProbabilitiesForP.clear();

                    // Negate all sums
                    typeEntropiesForP.replaceAll((typeID, v) -> -typeEntropiesForP.get(typeID));

                    if (!typeEntropiesForP.isEmpty())
                        predTypeEntropies.put(predicateID, typeEntropiesForP);
                })).get();

        workers.close();

        return predTypeEntropies;
    }

    private Map<Long, Map<Long, Double>> calculateEntropyTypeImportance(
            // Map of predicate ID -> type ID -> shannon's entropy
            Map<Long, Map<Long, Double>> predTypeEntropies,
            // Map of predicate ID -> Map of type ID -> type importance
            Map<Long, Map<Long, Double>> predTypeImportances) throws ExecutionException, InterruptedException {
        // Map of predicate ID -> type ID -> entropyTypeImportance
        Map<Long, Map<Long, Double>> entropyTypeImportances = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                predTypeEntropies.keySet().stream().parallel().forEach(predicateID -> {
                    Map<Long, Double> entropyTypeImportancesForP = new HashMap<>();

                    for (var typeEntropy : predTypeEntropies.get(predicateID).entrySet()) {
                        long typeID = typeEntropy.getKey();
                        double predTypeEntropy = typeEntropy.getValue();
                        double predTypeImportance = predTypeImportances.get(predicateID).get(typeID);

                        entropyTypeImportancesForP.put(typeID, predTypeEntropy * predTypeImportance);
                    }

                    entropyTypeImportances.put(predicateID, entropyTypeImportancesForP);
                })).get();

        workers.close();

        return entropyTypeImportances;
    }

    private Map<Long, Map<Long, Double>> calculateEntityTypeImportance(
            // Map of predicate ID -> Map of type ID -> subject frequency
            Map<Long, HashMap<Long, Long>> entityFreqTypes) throws ExecutionException, InterruptedException {

        // Map of predicate ID -> type ID -> entityTypeImportance
        Map<Long, Map<Long, Double>> entityTypeImportances = new ConcurrentHashMap<>();

        HashSet<Long> types = getTypesInKG();

        // Map of type ID -> no. of distinct subjects associated with the type
        ConcurrentHashMap<Long, Long> subjectsWithType = new ConcurrentHashMap<>();
        long idOfTypePredicateInKG = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);
        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                types.stream().parallel().forEach(type -> {
                    HashSet<Long> uniqueSubjects = new HashSet<>();
                    IteratorTripleID subjectsWithTypeQuery = endpoint.hdt.getTriples().search(new TripleID(0, idOfTypePredicateInKG, type));
                    while (subjectsWithTypeQuery.hasNext()) {
                        uniqueSubjects.add(subjectsWithTypeQuery.next().getSubject());
                    }

                    subjectsWithType.put(type, (long) uniqueSubjects.size());
                })).get();
        workers.close();

        workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                entityFreqTypes.keySet().stream().parallel().forEach(predicateID -> {
                    Map<Long, Double> entityTypeImportancesForP = new HashMap<>();

                    for (var entityFreqType : entityFreqTypes.get(predicateID).entrySet()) {
                        long typeID = entityFreqType.getKey();
                        long subjectFrequency = entityFreqType.getValue();

                        entityTypeImportancesForP.put(typeID, subjectFrequency * Math.log((double) subjectsWithType.get(typeID) / (double) subjectFrequency));
                    }

                    entityTypeImportances.put(predicateID, entityTypeImportancesForP);
                })).get();
        workers.close();

        return entityTypeImportances;
    }

    private Map<Long, Map<Long, Double>> calculateEntropyEntityTypeImportance(
            // Map of predicate ID -> type ID -> entityTypeImportance
            Map<Long, Map<Long, Double>> entityTypeImportances,
            // Map of predicate ID -> type ID -> entropyTypeImportance
            Map<Long, Map<Long, Double>> predicateEntropyTypes
    ) throws ExecutionException, InterruptedException {
        // Map of predicate ID -> type ID -> entropyEntityTypeImportance
        Map<Long, Map<Long, Double>> entropyEntityTypeImportances = new ConcurrentHashMap<>();

        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        workers.submit(() ->
                entityTypeImportances.keySet().stream().parallel().forEach(predicateID -> {
                    // Map of type ID -> entropyEntityTypeImportance
                    Map<Long, Double> entropyEntityTypeImportancesForP = new HashMap<>();

                    for (var entry : entityTypeImportances.get(predicateID).entrySet()) {
                        long typeID = entry.getKey();
                        double entityTypeImportance = entry.getValue();
                        double predicateEntropyType = predicateEntropyTypes.get(predicateID).get(typeID);

                        entropyEntityTypeImportancesForP.put(typeID, entityTypeImportance * predicateEntropyType);
                    }

                    entropyEntityTypeImportances.put(predicateID, entropyEntityTypeImportancesForP);
                })).get();

        workers.close();

        return entropyEntityTypeImportances;
    }

    // Write a list of TripleString iterators with the following strategy:
    // 1- Write each iterator to a separate HDT file, concurrently
    // 2- Concatenate all iterators via HDT's KCat
    //
    // This is extremely heavy in time, but avoids writing the iterators to a non-HDT format (the files would be huge)
    // while at the same time not consuming too much memory
    //
    // iteratorSizes needs to have the iterator sizes corresponding to the iterators in the same positions from the main Deque,
    // and are used to join them based on their internal sizes, avoiding at the same time to iterate through them to compute them
    //
    // Note: This is not needed anymore, but kept just in case. If any memory constraints appear in the future, using
    //       LocalHDTSPARQLEndpoint.fromRDFCatTree should be preferable
    /*private LocalHDTSPARQLEndpoint writeIteratorsToHDT(Deque<Iterator<TripleString>> iterators, Deque<Long> iteratorSizes, String tempName) throws ExecutionException, InterruptedException, IOException {
        ConcurrentLinkedDeque<LocalHDTSPARQLEndpoint> convertedIterators = new ConcurrentLinkedDeque<>();

        AtomicInteger workerIDs = new AtomicInteger(0);
        ForkJoinPool workers = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

        System.out.println("Creating batches...");
        List<Iterator<TripleString>> batches = new ArrayList<>();
        while (!iterators.isEmpty()) {
            Iterator<TripleString> batch = iterators.pop();
            long size = iteratorSizes.pop();

            while (size < TRIPLES_PER_BATCH) {
                if (iterators.isEmpty()) { // We are done here, this is the last batch
                    break;
                } else { // We can still add more
                    batch = Iterators.concat(batch, iterators.pop());
                    size += iteratorSizes.pop();
                }
            }

            batches.add(batch);
        }

        System.out.println("Converting " + batches.size() + " batches of <=" + TRIPLES_PER_BATCH + " triples to HDT files...");

        // Force HDTManager to be silent and stop reporting index generation times
        PrintStream stdOut = System.out;
        System.setOut(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));

        workers.submit(() ->
                batches.stream().parallel().forEach(iterator -> {
                    int workerID = workerIDs.getAndIncrement();
                    try {
                        convertedIterators.add(LocalHDTSPARQLEndpoint.iteratorToHDT(iterator,
                                tempName+"_"+workerID,
                                BASE_IMPORTANCE_URI,
                                false));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })).get();
        workers.close();
        System.setOut(stdOut);

        return LocalHDTSPARQLEndpoint.concatenateEndpoints(convertedIterators);
    }*/

    private HashSet<Long> getTypesInKG() {
        long idOfTypePredicateInKG = endpoint.hdt.getDictionary().stringToId(RDF_TYPE_URI, TripleComponentRole.PREDICATE);
        HashSet<Long> types = new HashSet<>();
        IteratorTripleID typesQuery = endpoint.hdt.getTriples().search(new TripleID(0, idOfTypePredicateInKG, 0));
        while (typesQuery.hasNext()) {
            types.add(typesQuery.next().getObject());
        }

        return types;
    }

    // Return an endpoint with the metrics. Note that, if any HDT-backed metrics generator is run after this one,
    // it will be invalid (the underlying HDT file is the same, and it will have changed after concatenating said
    // generator's results)
    public LocalHDTSPARQLEndpoint getMetricsHDT(boolean indexed) throws IOException {
        return new LocalHDTSPARQLEndpoint(destinationHDTFile, BASE_IMPORTANCE_SUBGRAPH_URI, indexed);
    }

    public String getDestinationHDTFile() {
        return destinationHDTFile;
    }


    // Returns a pair <Map of predID -> predicates it subsumes, Set of subsumed predicates>
    //
    // Expects a tab-separated file where the first column indicates the original predicateURI and the third column
    // the subsumed predicate URIs, so that if the first column is non-empty, it indicates that its predicate URI is
    // a subsumer (the third column will be ignored but should be equal to the first one) and all predicates below it
    // with an empty first column will be  subsumed by it
    //
    // The second column should indicate the field name in which the subsumed predicate URI is located in, and is unused
    // for now
    //
    // Not exposed for now
    public static Pair<HashMap<Long, Set<Long>>, Set<Long>> getSubsumedPredicates(LocalHDTSPARQLEndpoint ep) {
        Set<Long> subsumedPredicates = new HashSet<>();
        HashMap<Long, Set<Long>> equivalences = new HashMap<>();

        String filePath = "subsumed_predicates.tsv";

        long equivalentPredID = -1;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");

                String predURI = columns[0];
                String toBeEquivalentPredURI = columns[2];

                if (!predURI.isEmpty()) {
                    equivalentPredID = ep.hdt.getDictionary().stringToId(toBeEquivalentPredURI, TripleComponentRole.PREDICATE);
                    // It's the one we will use in later iterations as the equivalent
                } else {
                    if (equivalentPredID == -1) continue; // The previous subsuming predicate was not found

                    // Get the subsumed predicate
                    long toBeEquivalentPredID = ep.hdt.getDictionary().stringToId(toBeEquivalentPredURI, TripleComponentRole.PREDICATE);
                    if (toBeEquivalentPredID == -1) continue;

                    if (!equivalences.containsKey(equivalentPredID))
                        equivalences.put(equivalentPredID, new HashSet<>());

                    equivalences.get(equivalentPredID).add(toBeEquivalentPredID);
                    subsumedPredicates.add(toBeEquivalentPredID);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Pair<>(equivalences, subsumedPredicates);
    }
}

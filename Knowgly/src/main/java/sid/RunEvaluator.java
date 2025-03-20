package sid;

import org.apache.commons.cli.*;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import sid.Connectors.Elastic.ElasticConnector;
import sid.Connectors.Galago.GalagoConnector;
import sid.Connectors.IndexConnector;
import sid.Connectors.Lucene.LuceneConnector;
import sid.Connectors.Terrier.TerrierConnector;
import sid.Evaluation.Evaluator;
import sid.MetricsAggregation.Field;
import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.VirtualDocumentTemplate;
import sid.Pipeline.IndexingPipeline;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Helper executable that runs a BM25F query using the provided parameters (weights, k1, b) over the chosen
 * system and TREC queries file and stores the results in a .run file
 * <p>
 * If no output path is chosen, it will be stored in 'evaluation/metrics_testing/metrics_aggregator_results'
 */
public class RunEvaluator {
    public static void main(String[] args) throws IOException,
            NotFoundException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            IndexingPipeline.TypeBasedMetricsAggregatorInSPARQLException,
            ExecutionException,
            InterruptedException,
            ParserException, URISyntaxException {
        // Ensure a homogeneous formatting  (avoids conflicts of decimal commas when writing results to ntriples, for example)
        Locale.setDefault(Locale.US);

        Options options = new Options();

        Option c = new Option("c", "Connector", true, "Connector to use: 'galago', 'elastic', 'lucene', 'terrier'");
        c.setRequired(true);
        options.addOption(c);

        Option queries = new Option("q", "Queries file", true, "File containing the queries to run");
        queries.setRequired(true);
        options.addOption(queries);

        Option f = new Option("f", "Fields", true, "Number of fields: 3 or 5");
        f.setRequired(true);
        options.addOption(f);

        Option w0 = new Option("w0", "w0", true, "w0");
        w0.setRequired(true);
        options.addOption(w0);

        Option w1 = new Option("w1", "w1", true, "w1");
        w1.setRequired(true);
        options.addOption(w1);

        Option w2 = new Option("w2", "w2", true, "w2");
        w2.setRequired(true);
        options.addOption(w2);

        Option w3 = new Option("w3", "w3", true, "w3 (optional, only if using 5 fields)");
        w3.setRequired(false);
        options.addOption(w3);

        Option w4 = new Option("w4", "w4", true, "w4 (optional, only if using 5 fields)");
        w4.setRequired(false);
        options.addOption(w4);

        Option k1 = new Option("k1", "k1", true, "global k1 parameter for BM25F");
        k1.setRequired(true);
        options.addOption(k1);

        Option b = new Option("b", "b", true, "global b parameter for BM25F");
        b.setRequired(true);
        options.addOption(b);

        Option outputFilename = new Option("o", "output", true, "Output filename (optional, otherwise based on weights and parameters)");
        outputFilename.setRequired(false);
        options.addOption(outputFilename);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(" ", options);

            System.exit(1);
        }

        int fields = Integer.parseInt(cmd.getOptionValue("f"));

        VirtualDocumentTemplate t = MetricsAggregator.getEmptyVirtualDocumentTemplate();
        Field f0 = t.fields.get(0);
        Field f1 = t.fields.get(1);
        Field f2 = t.fields.get(2);

        f0.weight = Double.parseDouble(cmd.getOptionValue("w0"));
        f1.weight = Double.parseDouble(cmd.getOptionValue("w1"));
        f2.weight = Double.parseDouble(cmd.getOptionValue("w2"));

        String wString = String.format("%.4f", t.fields.get(0).weight) +
                "_" + String.format("%.4f", t.fields.get(1).weight) +
                "_" + String.format("%.4f", t.fields.get(2).weight);

        if (fields == 5) {
            Field f3 = t.fields.get(3);
            Field f4 = t.fields.get(4);

            f3.weight = Double.parseDouble(cmd.getOptionValue("w3"));
            f4.weight = Double.parseDouble(cmd.getOptionValue("w4"));

            wString += "_" + String.format("%.4f", t.fields.get(3).weight) +
                    "_" + String.format("%.4f", t.fields.get(4).weight);
        } else {
            throw new RuntimeException("Invalid number of fields. Allowed: 3 or 5");
        }

        double k1Value = Double.parseDouble(cmd.getOptionValue("k1"));
        double bValue = Double.parseDouble(cmd.getOptionValue("b"));

        String connectorName = cmd.getOptionValue("c");

        String outputFile;
        if (cmd.hasOption("o")) {
            outputFile = cmd.getOptionValue("o");
        } else {
            wString += "_k1_" + k1Value + "_b_" + bValue;
            outputFile = "evaluation/metrics_testing/metrics_aggregator_results/run_" + connectorName + "_" + wString + ".run";
        }

        IndexConnector connector = null;
        if (connectorName.equals("galago")) {
            connector = GalagoConnector.fromConfigurationFile();

            Map<String, String> queriesMap = new HashMap<>();
            try {
                FileInputStream stream = new FileInputStream(cmd.getOptionValue("q"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    queriesMap.put(parts[0], parts[1]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            ((GalagoConnector) connector).searchWithBM25F(queriesMap, t, k1Value, bValue, outputFile);
        } else {
            if (connectorName.equals("elastic")) {
                connector = ElasticConnector.fromConfigurationFile();
            } else if (connectorName.equals("lucene")) {
                connector = LuceneConnector.fromConfigurationFile();
            } else if (connectorName.equals("terrier")) {
                connector = TerrierConnector.fromConfigurationFile();
            } else {
                throw new RuntimeException("Invalid connector name. Allowed names: 'galago', 'elastic', 'lucene', 'terrier'");
            }

            Evaluator ev = new Evaluator(cmd.getOptionValue("q"), connector);
            ev.runQueriesAndSaveAsTRECResultsFile(outputFile,
                    true,
                    t,
                    connectorName,
                    k1Value,
                    bValue);
        }

        System.exit(0);
    }
}
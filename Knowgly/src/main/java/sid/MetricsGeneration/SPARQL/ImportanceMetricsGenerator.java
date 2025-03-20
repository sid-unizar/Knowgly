package sid.MetricsGeneration.SPARQL;

import sid.MetricsGeneration.MetricsGenerator;
import sid.SPARQLEndpoint.SPARQLEndpointWithNamedGraphs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * SPARQL specialization for the importance metrics. Not recommended for efficiency reasons,
 * use ImportanceMetricsGeneratorHDT instead.
 */
public class ImportanceMetricsGenerator implements MetricsGenerator {
    public static final String FACT_FREQUENCY_SPARQL = "configuration/queries/importance_queries/fact_frequency/factFreq.sparql";
    public static final String FACT_FREQUENCY_P_SPARQL = "configuration/queries/importance_queries/fact_frequency/factFreqP.sparql";
    public static final String FACT_FREQUENCY_O_SPARQL = "configuration/queries/importance_queries/fact_frequency/factFreqO.sparql";
    public static final String ENTITY_FREQUENCY_SPARQL = "configuration/queries/importance_queries/entity_frequency/entityFreq.sparql";
    public static final String ENTITY_FREQUENCY_P_SPARQL = "configuration/queries/importance_queries/entity_frequency/entityFreqP.sparql";
    public static final String ENTITY_FREQUENCY_O_SPARQL = "configuration/queries/importance_queries/entity_frequency/entityFreqO.sparql";
    public static final String ENTITY_TYPE_FREQUENCY_SPARQL = "configuration/queries/importance_queries/entity_type_frequency/entityFreqType.sparql";
    public static final String TYPE_FREQUENCY_PREDICATE_SPARQL = "configuration/queries/importance_queries/type_frequency/typeFrequencyP.sparql";
    public static final String TYPE_IMPORTANCE_SPARQL = "configuration/queries/importance_queries/typeImportance.sparql";
    public static final String FACT_FREQUENCY_TYPE_SPARQL = "configuration/queries/importance_queries/fact_type_frequency/factFreqType.sparql";
    public static final String FACT_FREQUENCY_TYPE_P_SPARQL = "configuration/queries/importance_queries/fact_type_frequency/factFreqTypeP.sparql";
    public static final String FACT_PROB_TYPE_SPARQL = "configuration/queries/importance_queries/factProbType.sparql";
    public static final String PREDICATE_ENTROPY_TYPE_SPARQL = "configuration/queries/importance_queries/predEntropyType.sparql";
    public static final String ENTROPY_TYPE_IMPORTANCE_SPARQL = "configuration/queries/importance_queries/entTypeImportance.sparql";
    public static final String ENTITY_TYPE_IMPORTANCE_SPARQL = "configuration/queries/importance_queries/entityTypeImportance.sparql";
    public static final String ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL = "configuration/queries/importance_queries/entEntityTypeImportance.sparql";

    public static final String PREDICATE_URI = "http://sid-unizar-search.com/importance/predicate";
    public static final String RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String PREDICATE_TYPE_URI = "http://sid-unizar-search.com/importance/Predicate-Type";
    public static final String PREDICATE_TYPE_URI_LOWERCASE = "http://sid-unizar-search.com/importance/predicate-type";
    public static final String TYPE_URI = "http://sid-unizar-search.com/importance/type";
    public static final String TYPE_IMPORTANCE_URI = "http://sid-unizar-search.com/importance/typeImportance";
    public static final String PREDICATE_ENTROPY_TYPE_URI = "http://sid-unizar-search.com/importance/predicateEntropyType";
    public static final String ENTROPY_TYPE_IMPORTANCE_URI = "http://sid-unizar-search.com/importance/entropyTypeImportance";
    public static final String ENTITY_TYPE_IMPORTANCE_URI = "http://sid-unizar-search.com/importance/entityTypeImportance";
    public static final String ENTROPY_ENTITY_TYPE_IMPORTANCE_URI = "http://sid-unizar-search.com/importance/entropyEntityTypeImportance";

    private final SPARQLEndpointWithNamedGraphs endpoint;

    public ImportanceMetricsGenerator(SPARQLEndpointWithNamedGraphs endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void run() {
        try {
            addImportanceMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addImportanceMetrics() throws IOException {
        List<Metric> metrics = Arrays.asList(
                new Metric(MetricsGenerator.getQueryString(FACT_FREQUENCY_SPARQL), "Fact frequency"),
                new Metric(MetricsGenerator.getQueryString(FACT_FREQUENCY_P_SPARQL), "Fact frequency_p"),
                new Metric(MetricsGenerator.getQueryString(FACT_FREQUENCY_O_SPARQL), "Fact frequency_o"),

                new Metric(MetricsGenerator.getQueryString(ENTITY_FREQUENCY_SPARQL), "Entity frequency"),
                new Metric(MetricsGenerator.getQueryString(ENTITY_FREQUENCY_P_SPARQL), "Entity frequency_p"),
                new Metric(MetricsGenerator.getQueryString(ENTITY_FREQUENCY_O_SPARQL), "Entity frequency_o"),
                new Metric(MetricsGenerator.getQueryString(ENTITY_TYPE_FREQUENCY_SPARQL), "Entity type frequency"),

                new Metric(MetricsGenerator.getQueryString(TYPE_FREQUENCY_PREDICATE_SPARQL), "Type frequency of predicate"),

                new Metric(MetricsGenerator.getQueryString(TYPE_IMPORTANCE_SPARQL), "Type importance"),

                new Metric(MetricsGenerator.getQueryString(FACT_FREQUENCY_TYPE_SPARQL), "Fact type frequency"),
                new Metric(MetricsGenerator.getQueryString(FACT_FREQUENCY_TYPE_P_SPARQL), "Fact type frequency for predicate"),

                new Metric(MetricsGenerator.getQueryString(FACT_PROB_TYPE_SPARQL), "fact probabilities for each predicate and type"),
                new Metric(MetricsGenerator.getQueryString(PREDICATE_ENTROPY_TYPE_SPARQL), "Predicate entropies for each type"),

                new Metric(MetricsGenerator.getQueryString(ENTROPY_TYPE_IMPORTANCE_SPARQL), "Entropy-type importances for each type"),
                new Metric(MetricsGenerator.getQueryString(ENTITY_TYPE_IMPORTANCE_SPARQL), "Entity type importances"),
                new Metric(MetricsGenerator.getQueryString(ENTROPY_ENTITY_TYPE_IMPORTANCE_SPARQL), "Entropy-entity type importances")
        );

        for (Metric metric : metrics) {
            System.out.println("Calculating " + metric.name + "...");
            endpoint.runUpdate(metric.query);
        }
    }
}

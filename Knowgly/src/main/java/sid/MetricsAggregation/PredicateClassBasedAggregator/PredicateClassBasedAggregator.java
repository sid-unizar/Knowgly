package sid.MetricsAggregation.PredicateClassBasedAggregator;

import sid.MetricsAggregation.MetricsAggregator;
import sid.MetricsAggregation.PredicateBasedAggregator.PredicateBasedMetricsAggregator;
import sid.SPARQLEndpoint.SPARQLEndpoint;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A Predicate-based aggregator which weighs datatype and object properties differently
 * <p>
 * It takes as an input two different metrics aggregators, so any aggregator combination can be done, independently
 * of their value ranges, etc., as each group of fields will be clustered separately
 */
public class PredicateClassBasedAggregator extends PredicateBasedMetricsAggregator {
    private final MetricsAggregator aggregatorForDatatypeProperties;
    private final MetricsAggregator aggregatorForObjectProperties;

    public PredicateClassBasedAggregator(MetricsAggregator aggregatorForDatatypeProperties,
                                         MetricsAggregator aggregatorForObjectProperties,
                                         int kMeansClusters,
                                         int kMeansIterations) {
        super(kMeansClusters, kMeansIterations);
        this.aggregatorForDatatypeProperties = aggregatorForDatatypeProperties;
        this.aggregatorForObjectProperties = aggregatorForObjectProperties;
    }

    public PredicateClassBasedAggregator(MetricsAggregator aggregatorForDatatypeProperties,
                                         MetricsAggregator aggregatorForObjectProperties) throws IOException {
        super();
        this.aggregatorForDatatypeProperties = aggregatorForDatatypeProperties;
        this.aggregatorForObjectProperties = aggregatorForObjectProperties;
    }

    @Override
    public OptionalClusterInputOrTemplate getOptionalClusterInput(SPARQLEndpoint endpoint, boolean forceReturnClusterInput) throws IOException, ExecutionException, InterruptedException {
        Set<String> datatypeProperties = MetricsAggregator.getDataTypeProperties(endpoint);
        Set<String> objectProperties = MetricsAggregator.getObjectProperties(endpoint);

        // Remove spurious predicates from the object properties, treating them as datatype properties
        // (this is the same behavior as in the MetricsAggregator when separating the clusters)
        objectProperties.removeAll(datatypeProperties);

        aggregatorForDatatypeProperties.setAllowedPredicates(datatypeProperties);
        aggregatorForObjectProperties.setAllowedPredicates(objectProperties);
        List<ResourceWrapper> clusterInput = aggregatorForDatatypeProperties.getOptionalClusterInput(endpoint, true).getClusterInput();
        clusterInput.addAll(aggregatorForObjectProperties.getOptionalClusterInput(endpoint, true).getClusterInput());

        return new OptionalClusterInputOrTemplate(clusterInput);
    }
}

package sid.MetricsAggregation;

import java.util.Set;

/**
 * An index field, containing a list of predicates and a weight assigned to it
 */
public class Field {
    public static class FieldElement {
        private String predicateURI;
        private long repetitions;

        /**
         * Basic constructor
         *
         * @param appearances How many times the predicate (and its objects) should appear in the document's field this
         *                    FieldElement is in. It should be 1 in all cases except when using type-based templates
         *                    with the repetitions combination method
         */
        public FieldElement(String predicateURI, long appearances) {
            this.predicateURI = predicateURI;
            this.repetitions = appearances;
        }

        public String getPredicateURI() {
            return predicateURI;
        }

        public void setPredicateURI(String predicateURI) {
            this.predicateURI = predicateURI;
        }

        public long getRepetitions() {
            return repetitions;
        }

        public void setRepetitions(int repetitions) {
            this.repetitions = repetitions;
        }
    }

    public String name;
    public Set<FieldElement> predicates;
    public double weight;
    public final boolean isForObjectProperties;
    public boolean isForEntityLinking;

    public Field(String name, Set<FieldElement> predicates, double weight, boolean isForObjectProperties, boolean isForEntityLinking) {
        this.name = name;
        this.predicates = predicates;
        this.weight = weight;
        this.isForObjectProperties = isForObjectProperties;
        this.isForEntityLinking = isForEntityLinking;
    }
}

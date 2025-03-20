package sid.Connectors;

/**
 * Abstraction of a retrieval result from any system, composed of an URI and a numerical score
 * Allows ordering via its score
 */
public class ScoredSearchResult implements Comparable<ScoredSearchResult> {
    public String URI;
    public double score;

    public ScoredSearchResult(String URI, double score) {
        this.URI = URI;
        this.score = score;
    }

    @Override
    public int compareTo(ScoredSearchResult other) {
        return Double.compare(score, other.score);
    }
}

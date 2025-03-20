package sid.SPARQLEndpoint;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

/**
 * An additional SPARQL endpoint specialization which supports having named graphs, and thus loading named models
 * into it
 * <p>
 * While local Jena models support them, they do not provide interfaces for manipulating them, thus local Jena models
 * (including HDT endpoints) do not have them
 */
public interface SPARQLEndpointWithNamedGraphs extends SPARQLEndpoint {
    void addNamedModel(String URI, Model model);

    /**
     * Return an iterator over all triples from a given graph
     */
    ResultSet getIteratorOverSubgraph(String graphURI);
}

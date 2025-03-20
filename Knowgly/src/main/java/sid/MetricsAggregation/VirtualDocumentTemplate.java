package sid.MetricsAggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Vdoc template which contains a list of fields, with each containing a list of predicates and a weight assigned to it
 * <p>
 * VirtualDocumentTemplates should be ideally created by the EntityIndexer's createEntityDocument helper method
 */
public class VirtualDocumentTemplate implements Cloneable {
    public List<Field> fields;

    public static VirtualDocumentTemplate fromJSON(String JSONFile) throws IOException {
        Gson gson = new Gson();
        return gson.fromJson(Files.readString(Path.of(JSONFile)), VirtualDocumentTemplate.class);
    }

    /**
     * Only for serializing and deserializing. You probably want to use
     * `MetricsAggregator.getEmptyVirtualDocumentTemplate()` in order to
     * get a VDoc containing the configured fields and weights
     */
    public VirtualDocumentTemplate() {
        this.fields = new ArrayList<>();
    }

    /**
     * @param normalizeWeights Whether to normalize the weights assigned to each field (this is not used for now)
     */
    @JsonCreator
    public VirtualDocumentTemplate(List<Field> fields, boolean normalizeWeights) {
        this.fields = fields;
        if (normalizeWeights) normalizeWeights();
    }

    private void normalizeWeights() {
        double maxWeight = 0.0;
        for (Field f : fields) {
            maxWeight = Math.max(f.weight, maxWeight);
        }

        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            f.weight /= maxWeight;
            fields.set(i, f);
        }
    }

    /**
     * Serialize this template for later use (with the fromJSON method). Useful to avoid applying inference again.
     */
    public void serialize(String destFile) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        PrintWriter out = new PrintWriter(destFile);
        out.println(gson.toJson(this));
        out.close();
    }

    @Override
    public VirtualDocumentTemplate clone() {
        try {
            VirtualDocumentTemplate clone = (VirtualDocumentTemplate) super.clone();
            clone.fields = new ArrayList<>(this.fields);

            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}

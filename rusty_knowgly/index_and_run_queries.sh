#!/bin/sh

cd lucene_indexer
mvn clean package
mv target/Knowgly-2.0-Main.jar lucene_indexer.jar


QUERIES_FILE="../LaQuE/queries/queries.test.tsv"

FILES="
../entity_type_importance_metrics_vdocs_5.json
"


K1="1.2" # 0.9
B="0.75" # 0.4

for FILE in $FILES; do
    if [ -f "$FILE" ]; then
        N_CLUSTERS=$(echo "$FILE" | sed 's/.*_\([0-9]*\)\.json/\1/')
        OUT_NAME=../$(basename "$FILE" .json)_queries.trec

        echo "Running tests for: $FILE"
        echo "$QUERIES_FILE" 1.0 "$N_CLUSTERS" "$K1" "$B" 1000 "$OUT_NAME"
        java -jar lucene_indexer.jar clear
        java -jar lucene_indexer.jar index "$FILE"
        java -jar lucene_indexer.jar search "$QUERIES_FILE" 1.0 "$N_CLUSTERS" "$K1" "$B" 1000 "$OUT_NAME"
    else
        echo "Warning: $FILE not found, skipping."
    fi
done
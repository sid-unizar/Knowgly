#!/bin/bash

# Runs trec_eval on every results file, and echoes back the results as valid .csv
# output 
# Needs to have a trec_eval executable named as "trec_eval", in the same folder
# the script is in
#
# GOTCHAS: If two elements have the same score, their rankings are completely ignored and 
# trec_eval will disambiguate them by the docID lexicographical ordering (in this case, the 
# URIs), which WILL result in weird behaviours


# Point as decimal separator
LC_NUMERIC="en_US.UTF-8"

# Define your qrels file location here
QRELS_FILE="../qrels-v2.txt"

max_NDCG10=0
max_NDCG100=0
max_NDCG10_file=""
max_NDCG100_file=""

echo "Metrics Aggregator Results"
echo "===================="
printf "%-75s %-10s %-10s\n" "File" "NDCG@10" "NDCG@100"

FILES="./metrics_aggregator_results/*.run"
for f in $FILES
do
    NDCG10=$(./trec_eval -m ndcg_cut.10 "$QRELS_FILE" "$f" | awk '{print $3}')
    NDCG100=$(./trec_eval -m ndcg_cut.100 "$QRELS_FILE" "$f" | awk '{print $3}')

    if (( $(echo "$NDCG10 > $max_NDCG10" | bc -l) )); then
        max_NDCG10=$NDCG10
        max_NDCG10_file=$f
    fi

    if (( $(echo "$NDCG100 > $max_NDCG100" | bc -l) )); then
        max_NDCG100=$NDCG100
        max_NDCG100_file=$f
    fi

    printf "%-75s %-10s %-10s\n" "$(basename "$f")" "$NDCG10" "$NDCG100"
done

printf "\nMax NDCG@10: %s with %f\n" "$max_NDCG10_file" "$max_NDCG10"
printf "Max NDCG@100: %s with %f\n" "$max_NDCG100_file" "$max_NDCG100"

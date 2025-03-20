#!/bin/bash

# Runs trec_eval on every results file, and echoes back the results as valid .csv
# output
# Needs to have a trec_eval executable named as "trec_eval", in the same folder
# the script is in
#
# This is a specialized version for type-based aggregators, which will automatically adapt
# the queries and qrels files depending on the type the .run indicates (Organisation, Person
# or Place within the filename) 
#
# GOTCHAS: If two elements have the same score, their rankings are completely ignored and 
# trec_eval will disambiguate them by the docID lexicographical ordering (in this case, the 
# URIs), which WILL result in weird behaviours


# Point as decimal separator
LC_NUMERIC="en_US.UTF-8"

max_NDCG10=0
max_NDCG100=0
max_NDCG10_file=""
max_NDCG100_file=""

printf "%-75s %-10s %-10s\n" "File" "NDCG@10" "NDCG@100"

#FILES="./prueba/*/*/*.run"
FILES="./metrics_aggregator_results/*.run"
for f in $FILES
do
    if [[ "$f" == *"Organisation"* ]]; then
        TYPE="Organisation"
    fi

    if [[ "$f" == *"Person"* ]]; then
        TYPE="Person"
    fi

    if [[ "$f" == *"Place"* ]]; then
        TYPE="Place"
    fi

    NDCG10=$(python get_ndcg_10_for_type.py "$f" "$TYPE")
    NDCG100=$(python get_ndcg_100_for_type.py "$f" "$TYPE")

    if (( $(echo "$NDCG10 > $max_NDCG10" | bc -l) )); then
        max_NDCG10=$NDCG10
        max_NDCG10_file=$f
    fi

    if (( $(echo "$NDCG100 > $max_NDCG100" | bc -l) )); then
        max_NDCG100=$NDCG100
        max_NDCG100_file=$f
    fi

    printf "%-75s %-10s %-10s\n" "$f" "$NDCG10" "$NDCG100"
done

printf "\nMax NDCG@10: %s with %f\n" "$max_NDCG10_file" "$max_NDCG10"
printf "Max NDCG@100: %s with %f\n" "$max_NDCG100_file" "$max_NDCG100"

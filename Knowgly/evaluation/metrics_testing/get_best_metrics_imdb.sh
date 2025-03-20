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
QRELS_FILE="../qrels_imdb.txt"
#QRELS_FILE="../testing_qrels.txt"

max_MAP=0
max_MAP_file=""

echo "Single Aggregator Results"
echo "===================="
printf "%-75s %-10s %-10s\n" "File" "MAP"

FILES="./metrics_aggregator_results/*.run"
for f in $FILES
do
    MAP=$(./trec_eval -m map "$QRELS_FILE" "$f" | awk '{print $3}')

    if (( $(echo "$MAP > $max_MAP" | bc -l) )); then
        max_MAP=$MAP
        max_MAP_file=$f
    fi

    printf "%-75s %-10s %-10s\n" "$(basename "$f")" "$MAP"
done


printf "\nMax MAP: %s with %f\n" "$max_MAP_file" "$max_MAP"

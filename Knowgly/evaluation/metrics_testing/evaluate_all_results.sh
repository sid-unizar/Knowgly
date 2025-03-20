#!/bin/bash

initial_dir=$(pwd)

process_run_files() {
    local dir="$1"
    
    for file in "$dir"/*; do
        if [ -d "$file" ]; then
            process_run_files "$file"
        elif [ -f "$file" ] && [ "${file##*.}" = "run" ]; then
            mv_dir=$(dirname "$file")

            python "print_ndcgs.py" "$file" > "ndcgs.txt"
            mv "ndcgs.txt" "$mv_dir"

            python "evaluate_single_result_ndcg10.py" "$file" > "results_per_query_family_ndcg10.txt"
            mkdir "$mv_dir"/results_per_query_family
            mv "results_per_query_family_ndcg10.png" "$mv_dir"/results_per_query_family
            mv "results_per_query_family_ndcg10.txt" "$mv_dir"/results_per_query_family

            python "evaluate_single_result_ndcg100.py" "$file" > "results_per_query_family_ndcg100.txt"
            mv "results_per_query_family_ndcg100.png" "$mv_dir"/results_per_query_family
            mv "results_per_query_family_ndcg100.txt" "$mv_dir"/results_per_query_family

            python "evaluate_single_result_per_entity_type_ndcg10.py" "$file" > "results_per_entity_type_ndcg10.txt"
            mkdir "$mv_dir"/results_per_entity_type
            mv "results_per_entity_type_ndcg10.png" "$mv_dir"/results_per_entity_type
            mv "results_per_entity_type_ndcg10.txt" "$mv_dir"/results_per_entity_type

            python "evaluate_single_result_per_entity_type_ndcg100.py" "$file" > "results_per_entity_type_ndcg100.txt"
            mv "results_per_entity_type_ndcg100.png" "$mv_dir"/results_per_entity_type
            mv "results_per_entity_type_ndcg100.txt" "$mv_dir"/results_per_entity_type

            python "evaluate_single_result_per_entity_and_query_type_ndcg10.py" "$file" > "evaluate_single_result_per_entity_and_query_type_ndcg10.txt"
            mkdir "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_INEX_LD_ndcg10.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_ListSearch_ndcg10.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_QALD2_ndcg10.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_SemSearch_ES_ndcg10.png" "$mv_dir"/results_per_entity_and_query_type
            mv "evaluate_single_result_per_entity_and_query_type_ndcg10.txt" "$mv_dir"/results_per_entity_and_query_type

            python "evaluate_single_result_per_entity_and_query_type_ndcg100.py" "$file" > "evaluate_single_result_per_entity_and_query_type_ndcg100.txt"
            mv "results_per_entity_type_INEX_LD_ndcg100.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_ListSearch_ndcg100.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_QALD2_ndcg100.png" "$mv_dir"/results_per_entity_and_query_type
            mv "results_per_entity_type_SemSearch_ES_ndcg100.png" "$mv_dir"/results_per_entity_and_query_type
            mv "evaluate_single_result_per_entity_and_query_type_ndcg100.txt" "$mv_dir"/results_per_entity_and_query_type
        fi
    done
}

# Starting directory
start_dir="metrics_aggregator_results"

process_run_files "$start_dir"
 

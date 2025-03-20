#!/bin/bash

initial_dir=$(pwd)

process_run_files() {
    local dir="$1"
    
    for file in "$dir"/*; do
        if [ -d "$file" ]; then
            process_run_files "$file"
        elif [ -f "$file" ] && [ "${file##*.}" = "run" ]; then
            mv_dir=$(dirname "$file")

            python "print_MAPs_imdb.py" "$file" > "maps.txt"
            mv "maps.txt" "$mv_dir"
        fi
    done
}

# Starting directory
start_dir="metrics_aggregator_results"

process_run_files "$start_dir"
 

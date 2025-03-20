export LC_ALL=en_US.UTF-8 # Force galago to output results using decimal points, and not commas

# Both produce the same results, you can use any
../galago/galago-3.16/core/target/appassembler/bin/galago threaded-batch-search evaluation/queries_galago.json > results_galago.run
#../galago/galago_3.22/contrib/target/appassembler/bin/galago threaded-batch-search evaluation/queries_galago.json > results_galago.run

python fix_galago_results.py results_galago.run results_galago_with_encased_uris.run
mv results_galago_with_encased_uris.run $1

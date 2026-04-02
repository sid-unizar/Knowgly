Prerequisites:
    - All .run files must be located in 'evaluation/results/metrics_aggregator_results' 
    - You will need to place a 'trec_eval' executable on this folder (https://github.com/usnistgov/trec_eval)

- For DBpedia: 
    - Run 'results/get_best_metrics.sh' for global metrics aggregators
    - Run 'results/get_best_metrics_type_based.sh' for type-based metrics aggregators. 
      In this case, the .run files will need to indicate the type they were run for by containing
      'Organisation', 'Person' or 'Place' in their filenames.

- The 'evaluate_all_results.sh' script can also be used to generate a more in-depth analysis of the .run files (Query-type and entity-type results, plots...).
  When using these scripts, every .run file should be contained in individual folders under 'evaluation/results/metrics_aggregator_results'.
    - These scripts will only check 'evaluation/results/metrics_aggregator_results'



Prerequisites:
    - All .run files must be located in 'evaluation/metrics_testing/metrics_aggregator_results' 
    - You will need to place a 'trec_eval' executable on this folder (https://github.com/usnistgov/trec_eval)

- For DBpedia: 
    - Run 'metrics_testing/get_best_metrics.sh' for global metrics aggregators
    - Run 'metrics_testing/get_best_metrics_type_based.sh' for type-based metrics aggregators. 
      In this case, the .run files will need to indicate the type they were run for by containing
      'Organisation', 'Person' or 'Place' in their filenames.

- For IMDb: 
    - Run 'metrics_testing/get_best_metrics_imdb.sh' for global metrics aggregators

- For reranked results:
    - The EMBERT folder contains two helper scripts to convert from/to condensed EMBERT .run files, which are not valid for trec_eval


- The 'evaluate_all_results.sh' and 'evaluate_all_results_imdb.sh' scripts can also be used to generate a more in-depth analysis of the .run files (Query-type and entity-type results, plots...).
  When using these scripts, every .run file should be contained in individual folders under 'evaluation/metrics_testing/metrics_aggregator_results'.
    - These scripts will only check 'evaluation/metrics_testing/metrics_aggregator_results'



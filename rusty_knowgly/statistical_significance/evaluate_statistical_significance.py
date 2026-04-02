# Utility script that calculates the statistical significance of all .run files contained in the
# script's dir wrt. DBpedia-Entity v2's base BM25 run. Needs to have both qrels-v2.txt and bm25.run
# files accesible from the script's cwd

import os
import sys

import scipy.stats
import pytrec_eval

qrels = pytrec_eval.parse_qrel(open("qrels-v2.txt", "r"))
base_run = pytrec_eval.parse_run(open("bm25.run", "r"))

def get_all_runs():
    file_paths = []
    for root, dirs, files in os.walk(os.getcwd()):
        for file in files:
            if file.endswith(".run"):
                file_paths.append(os.path.join(root, file))
    return file_paths

def main():
    runs = get_all_runs()

    evaluator = pytrec_eval.RelevanceEvaluator(qrels, {"ndcg_cut_10"})

    print(f"Run file, p-value, statistic, df")

    first_run_results = evaluator.evaluate(base_run)
    for run in runs:
        second_run_results = evaluator.evaluate(pytrec_eval.parse_run(open(run, "r")))

        query_ids = list(
            set(first_run_results.keys()) & set(second_run_results.keys()))

        first_run_scores = [first_run_results[query_id]["ndcg_cut_10"] for query_id in query_ids]
        second_run_scores = [second_run_results[query_id]["ndcg_cut_10"] for query_id in query_ids]

        stats = scipy.stats.ttest_rel(first_run_scores, second_run_scores)
        print(f"{run}, {stats.pvalue}, {stats.statistic}, {stats.df}")


if __name__ == "__main__":
    sys.exit(main())

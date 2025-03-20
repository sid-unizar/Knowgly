import common

import random
import shutil
import numpy as np

from sklearn.model_selection import KFold

MAX_CA_ATTEMPTS = 10
MAX_CA_ITERATIONS = 750
ADJUST_PARAMETERS = False  # Whether to adjust BM25 params too or not
STEP_SIZE = 0.05 # -/+ change performed to the weights and parameters on each attempt

# Change accordingly
QUERIES_FILE = "evaluation/queries-v2_stopped.txt"  # DBpedia
# QUERIES_FILE = "evaluation/queries_imdb.txt" # IMDb
QRELS_FILE = "../qrels-v2.txt"  # DBpedia
# QRELS_FILE = "../qrels_imdb.txt" # IMDb
QRELS_FILE_KFOLD = "evaluation/qrels-v2.txt" # DBpedia
# QRELS_FILE_KFOLD = "evaluation/qrels_imdb.txt" # IMDb

# The system to launch queries to
#INDEX = "galago"
INDEX = "lucene"
#INDEX = "terrier"
#INDEX = "elastic"

def coordinate_ascent(adjust_parameters):
    best_score = float('-inf')
    # best_params = None

    # Start close to our weights
    weights = [1.0, random.uniform(0.25, 0.75), random.uniform(0.0, 0.25)]

    k1 = 1.2  # Default elasticsearch value (galago's is 0.5, which is quite bad)
    b = 0.5  # Default galago value
    if adjust_parameters:
        # Start close to our BM25 parameters
        k1 = random.uniform(0.75, 1.2)
        b = random.uniform(0.25, 0.75)

    num_iterations = MAX_CA_ITERATIONS
    iter_counter = 0
    for iteration in range(num_iterations):
        iter_counter = iteration
        scores = [best_score]

        # We adjust w0 too, though it should stay very close to 1.0
        # w0
        if weights[0] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[0] - STEP_SIZE >= 1.0):
            scores.append(
                common.search_with_field_weights_3(weights[0] - STEP_SIZE, weights[1], weights[2], k1, b, QUERIES_FILE,
                                                   QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_3(weights[0] + STEP_SIZE, weights[1], weights[2], k1, b, QUERIES_FILE,
                                               QRELS_FILE, INDEX))

        # w1
        if weights[1] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[1] - STEP_SIZE >= 1.0):
            scores.append(
                common.search_with_field_weights_3(weights[0], weights[1] - STEP_SIZE, weights[2], k1, b, QUERIES_FILE,
                                                   QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_3(weights[0], weights[1] + STEP_SIZE, weights[2], k1, b, QUERIES_FILE,
                                               QRELS_FILE, INDEX))

        # w2
        if weights[2] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[2] - STEP_SIZE >= 1.0):
            scores.append(
                common.search_with_field_weights_3(weights[0], weights[1], weights[2] - STEP_SIZE, k1, b, QUERIES_FILE,
                                                   QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_3(weights[0], weights[1], weights[2] + STEP_SIZE, k1, b, QUERIES_FILE,
                                               QRELS_FILE, INDEX))

        if adjust_parameters:
            # k1
            if k1 - STEP_SIZE >= 0.0:
                scores.append(common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1 - STEP_SIZE, b,
                                                                 QUERIES_FILE, QRELS_FILE, INDEX))
            else:
                scores.append(0.0)

            scores.append(
                common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1 + STEP_SIZE, b, QUERIES_FILE,
                                                   QRELS_FILE, INDEX))

            # b
            if b - STEP_SIZE >= 0.0:
                scores.append(common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1, b - STEP_SIZE,
                                                                 QUERIES_FILE, QRELS_FILE, INDEX))
            else:
                scores.append(0.0)

            scores.append(
                common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1, b + STEP_SIZE, QUERIES_FILE,
                                                   QRELS_FILE, INDEX))

        max_i = np.argmax(scores)
        match max_i:
            case 0:  # All directions failed at improving the best score
                break
            # w0
            case 1:
                weights[0] = weights[0] - STEP_SIZE
            case 2:
                weights[0] = weights[0] + STEP_SIZE

            # w1
            case 3:
                weights[1] = weights[1] - STEP_SIZE
            case 4:
                weights[1] = weights[1] + STEP_SIZE

            # w2
            case 5:
                weights[2] = weights[2] - STEP_SIZE
            case 6:
                weights[2] = weights[2] + STEP_SIZE

            # k1
            case 7:
                if adjust_parameters:
                    k1 = k1 - STEP_SIZE
                else:
                    break
            case 8:
                if adjust_parameters:
                    k1 = k1 + STEP_SIZE
                else:
                    break

            # b
            case 9:
                if adjust_parameters:
                    b = b - STEP_SIZE
                else:
                    break
            case 10:
                if adjust_parameters:
                    b = b + STEP_SIZE
                else:
                    break

        best_score = scores[max_i]

    return weights, k1, b, best_score, iter_counter + 1


def run_coordinate_ascent(adjust_parameters, filename_prefix):
    best_score_attempts = 0.0
    best_k1_attempts = -1
    best_b_attempts = -1
    best_weights = []

    # 10 attempts of 500 (max.) iterations
    num_attempts = MAX_CA_ATTEMPTS
    for attempt in range(num_attempts):
        print(f"\rRunning attempt {attempt}/{MAX_CA_ATTEMPTS}...")
        weights_attempt, k1_attempt, b_attempt, score_attempt, _ = coordinate_ascent(adjust_parameters)

        if best_score_attempts < score_attempt:
            best_weights = weights_attempt
            best_score_attempts = score_attempt
            best_k1_attempts = k1_attempt
            best_b_attempts = b_attempt
            print(f"Best score so far: {best_score_attempts}")

    print()
    print(f"Results of BM25 parameter tuning:")
    print(f"\tWeight scheme used: {best_weights}")
    print(f"\tk1: {best_k1_attempts}")
    print(f"\tb: {best_b_attempts}")
    print(f"\tScore: {best_score_attempts}")

    common.save_results_3(best_weights[0], best_weights[1], best_weights[2], best_k1_attempts, best_b_attempts,
                          filename_prefix, QUERIES_FILE)


def get_file(file_path):
    lines = []
    with open(file_path, 'r') as file:
        lines = file.readlines()

    return lines


def write_file(queries_or_qrels, file_path):
    with open(file_path, 'w') as file:
        file.writelines(queries_or_qrels)


def get_ids_in_queries(queries):
    ids = set()
    for q in queries:
        ids.add(q.split()[0])

    return ids


def write_qrels_file(qrels, ids):
    for qrel in qrels:
        if qrel.split()[0] in ids:
            with open("../Knowgly/" + QRELS_FILE_KFOLD, 'w') as file:
                file.write(qrel)


if __name__ == "__main__":
    shutil.copy("../Knowgly/" + QUERIES_FILE, "../Knowgly/" + QUERIES_FILE + ".bak")
    shutil.copy("../Knowgly/" + QRELS_FILE_KFOLD, "../Knowgly/" + QRELS_FILE_KFOLD + ".bak")

    queries = get_file("../Knowgly/" + QUERIES_FILE)
    qrels = get_file("../Knowgly/" + QRELS_FILE_KFOLD)

    best_score = 0.0
    best_params = (None, None, None)

    kf = KFold(n_splits=5, shuffle=True)
    fold_scores = []
    fold_params = []
    for train_index, test_index in kf.split(queries):
        print("Evaluating fold...")
        train_queries = [queries[i] for i in train_index]
        eval_queries = [queries[i] for i in test_index]

        # Overwrite the queries & qrels files with train and do CA on it
        write_file(train_queries, "../Knowgly/" + QUERIES_FILE)
        write_qrels_file(qrels, get_ids_in_queries(train_queries))
        weights, k1, b, _, _ = coordinate_ascent(ADJUST_PARAMETERS)

        # Overwrite the queries & qrels files with eval and get the score for the fold
        write_file(eval_queries, "../Knowgly/" + QUERIES_FILE)
        write_qrels_file(qrels, get_ids_in_queries(eval_queries))
        fold_scores.append(
            common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        fold_params.append((weights, k1, b))

        # Restore the original queries and qrels
        shutil.copy("../Knowgly/" + QUERIES_FILE + ".bak", "../Knowgly/" + QUERIES_FILE)
        shutil.copy("../Knowgly/" + QRELS_FILE_KFOLD + ".bak", "../Knowgly/" + QRELS_FILE_KFOLD)
        print("Score from fold:",
              common.search_with_field_weights_3(weights[0], weights[1], weights[2], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))

    for i, fold_score in enumerate(fold_scores):
        if fold_score > best_score:
            best_score = fold_score
            best_params = fold_params[i]

    (best_weights, best_k1_attempts, best_b_attempts) = best_params
    print()
    print(f"Results of BM25F parameter tuning:")
    print(f"\tWeight scheme used: {best_weights}")
    print(f"\tk1: {best_k1_attempts}")
    print(f"\tb: {best_b_attempts}")
    print(f"\tScore (eval): {best_score}")

    common.save_results_3(best_weights[0], best_weights[1], best_weights[2], best_k1_attempts, best_b_attempts,
                          "ca_results", QUERIES_FILE, INDEX)

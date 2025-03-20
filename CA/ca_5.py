import common

import random
import numpy as np

MAX_CA_ATTEMPTS = 1
MAX_CA_ITERATIONS = 1
ADJUST_PARAMETERS = False  # Whether to adjust BM25 params too or not
STEP_SIZE = 0.05 # -/+ change performed to the weights and parameters on each attempt

# Change accordingly
QUERIES_FILE = "evaluation/queries-v2_stopped.txt"  # DBpedia
# QUERIES_FILE = "evaluation/queries_imdb.txt" # IMDb
QRELS_FILE = "../qrels-v2.txt"  # DBpedia
# QRELS_FILE = "../qrels_imdb.txt" # IMDb

# The system to launch queries to
#INDEX = "galago"
INDEX = "lucene"
#INDEX = "terrier"
#INDEX = "elastic"

def coordinate_ascent(adjust_parameters):
    best_score = float('-inf')
    # best_params = None

    # Start close to our weights
    if INDEX == "lucene":
        weights = [random.uniform(1.8, 2.0),
                   random.uniform(1.6, 1.8),
                   random.uniform(1.4, 1.6),
                   random.uniform(1.2, 1.4),
                   random.uniform(1.0, 1.2)]
    else:
        weights = [1.0,
                   random.uniform(0.25, 0.75),
                   random.uniform(0.0, 0.25),
                   random.uniform(0.0, 0.25),
                   random.uniform(0.0, 0.25)]

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
            scores.append(common.search_with_field_weights_5(weights[0] - STEP_SIZE, weights[1], weights[2], weights[3],
                                                             weights[4], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_5(weights[0] + STEP_SIZE, weights[1], weights[2], weights[3], weights[4],
                                               k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        # w1
        if weights[1] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[1] - STEP_SIZE >= 1.0):
            scores.append(common.search_with_field_weights_5(weights[0], weights[1] - STEP_SIZE, weights[2], weights[3],
                                                             weights[4], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_5(weights[0], weights[1] + STEP_SIZE, weights[2], weights[3], weights[4],
                                               k1, b, QUERIES_FILE, QRELS_FILE, INDEX))

        # w2
        if weights[2] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[2] - STEP_SIZE >= 1.0):
            scores.append(common.search_with_field_weights_5(weights[0], weights[1], weights[2] - STEP_SIZE, weights[3],
                                                             weights[4], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_5(weights[0], weights[1], weights[2] + STEP_SIZE, weights[3], weights[4],
                                               k1, b, QUERIES_FILE, QRELS_FILE, INDEX))

        # w3
        if weights[3] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[3] - STEP_SIZE >= 1.0):
            scores.append(common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3] - STEP_SIZE,
                                                             weights[4], k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3] + STEP_SIZE, weights[4],
                                               k1, b, QUERIES_FILE, QRELS_FILE, INDEX))

        # w4
        if weights[4] - STEP_SIZE >= 0.0 and (INDEX != "lucene" or weights[4] - STEP_SIZE >= 1.0):
            scores.append(common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3],
                                                             weights[4] - STEP_SIZE, k1, b, QUERIES_FILE, QRELS_FILE, INDEX))
        else:
            scores.append(0.0)

        scores.append(
            common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3], weights[4] + STEP_SIZE,
                                               k1, b, QUERIES_FILE, QRELS_FILE, INDEX))

        if adjust_parameters:
            # k1
            if k1 - STEP_SIZE >= 0.0:
                scores.append(
                    common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3], weights[4],
                                                       k1 - STEP_SIZE, b, QUERIES_FILE, QRELS_FILE, INDEX))
            else:
                scores.append(0.0)

            scores.append(common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3], weights[4],
                                                             k1 + STEP_SIZE, b, QUERIES_FILE, QRELS_FILE, INDEX))

            # b
            if b - STEP_SIZE >= 0.0:
                scores.append(
                    common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3], weights[4], k1,
                                                       b - STEP_SIZE, QUERIES_FILE, QRELS_FILE, INDEX))
            else:
                scores.append(0.0)

            scores.append(
                common.search_with_field_weights_5(weights[0], weights[1], weights[2], weights[3], weights[4], k1,
                                                   b + STEP_SIZE, QUERIES_FILE, QRELS_FILE, INDEX))

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

            # w3
            case 7:
                weights[3] = weights[3] - STEP_SIZE
            case 8:
                weights[3] = weights[3] + STEP_SIZE

            # w4
            case 9:
                weights[4] = weights[4] - STEP_SIZE
            case 10:
                weights[4] = weights[4] + STEP_SIZE

            # k1
            case 11:
                if adjust_parameters:
                    k1 = k1 - STEP_SIZE
                else:
                    break
            case 12:
                if adjust_parameters:
                    k1 = k1 + STEP_SIZE
                else:
                    break

            # b
            case 13:
                if adjust_parameters:
                    b = b - STEP_SIZE
                else:
                    break
            case 14:
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

    common.save_results_5(best_weights[0],
                          best_weights[1],
                          best_weights[2],
                          best_weights[3],
                          best_weights[4],
                          best_k1_attempts,
                          best_b_attempts,
                          filename_prefix,
                          QUERIES_FILE, 
                          INDEX)


if __name__ == "__main__":
    run_coordinate_ascent(ADJUST_PARAMETERS, "ca_5_results")

import os
import subprocess


# https://stackoverflow.com/questions/783897/how-to-truncate-float-values
def truncate(f, n):
    """Truncates/pads a float f to n decimal places without rounding"""
    s = '%.12f' % f
    i, p, d = s.partition('.')
    return '.'.join([i, (d + '0' * n)[:n]])


def search_with_field_weights_5(w0, w1, w2, w3, w4, k1, b, queries_file, qrels_file, index):
    score = 0.0

    w0_str = truncate(w0, 4)
    w1_str = truncate(w1, 4)
    w2_str = truncate(w2, 4)
    w3_str = truncate(w3, 4)
    w4_str = truncate(w4, 4)
    k1_str = truncate(k1, 4)
    b_str = truncate(b, 4)

    subprocess.check_output(
        ["java", "-jar", "../Knowgly/RunEvaluator.jar", "-q", queries_file, "-f", "5", "-w0", w0_str, "-w1",
         w1_str, "-w2", w2_str, "-w3", w3_str, "-w4", w4_str, "-k1", k1_str, "-b", b_str, "-c", index, "-o", "evaluation/metrics_testing/metrics_aggregator_results/ca.run"],
        stderr=subprocess.DEVNULL,
        cwd="../Knowgly")

    trec_eval_path = os.path.join(os.getcwd(), "../Knowgly/evaluation/metrics_testing/trec_eval")
    filename = "metrics_aggregator_results/ca.run"
    trec_output = subprocess.run([trec_eval_path, "-m", "ndcg_cut.10", qrels_file, filename],
                                 cwd="../Knowgly/evaluation/metrics_testing",
                                 capture_output=True,
                                 text=True)
    for line in str(trec_output.stdout).splitlines():  # There should only be one
        score = float(line.split()[2])

    os.remove("../Knowgly/evaluation/metrics_testing/metrics_aggregator_results/ca.run")

    return score


def save_results_5(w0, w1, w2, w3, w4, k1, b, filename_prefix, queries_file, index):
    w0_str = truncate(w0, 4)
    w1_str = truncate(w1, 4)
    w2_str = truncate(w2, 4)
    w3_str = truncate(w3, 4)
    w4_str = truncate(w4, 4)
    k1_str = truncate(k1, 4)
    b_str = truncate(b, 4)

    subprocess.run(
        ["java", "-jar", "../Knowgly/RunEvaluator.jar", "-q", queries_file, "-f", "5", "-w0", w0_str, "-w1",
         w1_str, "-w2", w2_str, "-w3", w3_str, "-w4", w4_str, "-k1", k1_str, "-b", b_str, "-c", index, "-o", "evaluation/metrics_testing/metrics_aggregator_results/ca.run"],
        stderr=subprocess.DEVNULL,
        cwd="../Knowgly")

    subprocess.check_output(["mv", "evaluation/metrics_testing/metrics_aggregator_results/ca.run",
                             f"{filename_prefix}_w0_{w0_str}_w1_{w1_str}_w2_{w2_str}_w3_{w3_str}_w4_{w4_str}_k1_{k1_str}_b_{b_str}.run"],
                            stderr=subprocess.DEVNULL,
                            cwd="../Knowgly")


def search_with_field_weights_3(w0, w1, w2, k1, b, queries_file, qrels_file, index):
    score = 0.0

    w0_str = truncate(w0, 4)
    w1_str = truncate(w1, 4)
    w2_str = truncate(w2, 4)
    k1_str = truncate(k1, 4)
    b_str = truncate(b, 4)

    subprocess.run(
        ["java", "-jar", "../Knowgly/RunEvaluator.jar", "-q", queries_file, "-f", "3", "-w0", w0_str, "-w1",
         w1_str, "-w2", w2_str, "-k1", k1_str, "-b", b_str, "-c", index, "-o", "evaluation/metrics_testing/metrics_aggregator_results/ca.run"],
        stderr=subprocess.DEVNULL,
        cwd="../Knowgly")

    trec_eval_path = os.path.join(os.getcwd(), "../Knowgly/evaluation/metrics_testing/trec_eval")
    filename = "metrics_aggregator_results/ca.run"
    trec_output = subprocess.run([trec_eval_path, "-m", "ndcg_cut.10", qrels_file, filename],
                                 cwd="../Knowgly/evaluation/metrics_testing",
                                 capture_output=True,
                                 text=True)
    for line in str(trec_output.stdout).splitlines():  # There should only be one
        score = float(line.split()[2])

    os.remove("../Knowgly/evaluation/metrics_testing/metrics_aggregator_results/ca.run")

    return score


def save_results_3(w0, w1, w2, k1, b, filename_prefix, queries_file, index):
    w0_str = truncate(w0, 4)
    w1_str = truncate(w1, 4)
    w2_str = truncate(w2, 4)
    k1_str = truncate(k1, 4)
    b_str = truncate(b, 4)

    subprocess.run(
        ["java", "-jar", "../Knowgly/RunEvaluator.jar", "-q", queries_file, "-f", "3", "-w0", w0_str, "-w1",
         w1_str, "-w2", w2_str, "-w3", "-k1", k1_str, "-b", b_str, "-c", index, "-o", "evaluation/metrics_testing/metrics_aggregator_results/ca.run"],
        stderr=subprocess.DEVNULL,
        cwd="../Knowgly")

    subprocess.check_output(["mv", "evaluation/metrics_testing/metrics_aggregator_results/ca.run",
                             f"{filename_prefix}_w0_{w0_str}_w1_{w1_str}_w2_{w2_str}_k1_{k1_str}_b_{b_str}.run"],
                            stderr=subprocess.DEVNULL,
                            cwd="../Knowgly")

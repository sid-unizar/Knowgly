import sys
import subprocess
import matplotlib.pyplot as plt

QRELS_FILE = "../qrels-v2.txt"
QUERY_RESULTS_FILE = sys.argv[1]

trec_output_ndcg10 = subprocess.check_output(["./trec_eval", "-q", "-m", "ndcg_cut.10", QRELS_FILE, QUERY_RESULTS_FILE])
trec_output_ndcg100 = subprocess.check_output(["./trec_eval", "-q", "-m", "ndcg_cut.100", QRELS_FILE, QUERY_RESULTS_FILE])


dbpedia_entity_prefixes = ["SemSearch_ES", "INEX_LD", "QALD2"]

for line in trec_output_ndcg10.splitlines():
    line = line.decode("utf-8")
    if line.startswith("ndcg_cut_10"):
        query_id, ndcg10 = line.split()[1], line.split()[2]
                
        if query_id == 'all':
            print(f"NDCG@10:  {ndcg10}")
            break 

for line in trec_output_ndcg100.splitlines():
    line = line.decode("utf-8")
    if line.startswith("ndcg_cut_100"):
        query_id, ndcg100 = line.split()[1], line.split()[2]

        if query_id == 'all':
            print(f"NDCG@100: {ndcg100}")
            break

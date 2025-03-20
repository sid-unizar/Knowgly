import sys
import subprocess
import matplotlib.pyplot as plt

QRELS_FILE="../qrels_imdb.txt"
QUERY_RESULTS_FILE = sys.argv[1]

trec_output_ndcg10 = subprocess.check_output(["./trec_eval", "-q", "-m", "map", QRELS_FILE, QUERY_RESULTS_FILE])

for line in trec_output_ndcg10.splitlines():
    line = line.decode("utf-8")
    if line.startswith("map"):
        query_id, ndcg10 = line.split()[1], line.split()[2]
                
        if query_id == 'all':
            print(f"MAP:  {ndcg10}")
            break 

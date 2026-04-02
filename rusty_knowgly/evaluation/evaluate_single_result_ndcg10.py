import sys
import subprocess
import matplotlib.pyplot as plt

QRELS_FILE = "../qrels-v2.txt"
QUERY_RESULTS_FILE = sys.argv[1]

trec_output = subprocess.check_output(["./trec_eval", "-q", "-m", "ndcg_cut.10", QRELS_FILE, QUERY_RESULTS_FILE])

dbpedia_entity_prefixes = ["SemSearch_ES", "INEX_LD", "QALD2"]

prefix_dict = {}
for line in trec_output.splitlines():
    line = line.decode("utf-8")
    if line.startswith("ndcg_cut_10"):
        query_id, ndcg10 = line.split()[1], line.split()[2]
        prefix = query_id.rsplit('-', 1)[0]
                
        if query_id == 'all':
            prefix_dict[query_id] = {}
            prefix_dict[query_id][query_id] = float(ndcg10)
            continue
        
        # Do the same categorization as in the DBpedia-entity result tables
        found = False
        for known_prefix in dbpedia_entity_prefixes:
            if known_prefix in prefix:
                found = True
                if known_prefix not in prefix_dict:
                    prefix_dict[known_prefix] = {}
                prefix_dict[known_prefix][query_id] = float(ndcg10)
            
        if not found:
            known_prefix = "ListSearch"
            
            if known_prefix not in prefix_dict:
                prefix_dict[known_prefix] = {}
            prefix_dict[known_prefix][query_id] = float(ndcg10)
        


num_plots = len(prefix_dict) - 1
num_rows = (num_plots + 1) // 2
num_cols = min(2, num_plots)

fig, axes = plt.subplots(num_rows, num_cols, figsize=(10, 5))

if num_plots == 1:
    axes = [axes]

for (prefix, ndcgs), ax in zip(prefix_dict.items(), axes.flatten()):
    for (q, s) in ndcgs.items():
        if s == 0.0:
            print(f"Example of a 0.0 score for {prefix}: {q}")
        elif s == 1.0:
            print(f"Example of a 1.0 score for {prefix}: {q}")
    
    if prefix == 'all':
        continue
    colors = ['red' if v < 0.25 else 'orange' if v < 0.5 else 'green' for v in ndcgs.values()]
    ax.bar(ndcgs.keys(), ndcgs.values(), color=colors)

    #ax.tick_params(axis='x', labelrotation=90)
    ax.set_xticklabels([])  # Remove x-labels, the query strings are too long

    ax.set_title(f"{prefix}", weight="bold")
    ax.set_xlabel("Query")
    ax.set_ylabel("NDCG@10")

    ax.set_ylim([0, 1])

    avg_ndcg = sum(ndcgs.values()) / len(ndcgs)
    print(f"Average NDCG@10 for {prefix}: {avg_ndcg:.4f}")
    print()
    print()

# Hide any empty subplots
if num_plots < num_rows * num_cols:
    for ax in axes.flatten()[num_plots:]:
        ax.axis('off')

plt.tight_layout()  # Adjust spacing between subplots
#plt.show()
plt.savefig("results_per_query_family_ndcg10.png")

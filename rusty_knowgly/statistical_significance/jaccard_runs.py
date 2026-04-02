# Utility to calculate the jaccard distance between the URIs contained in two runs
#   Usage: python jaccard_runs.py run_1.run run_2.run

import sys

queries = dict()

with open(sys.argv[1]) as run_1:
    with open(sys.argv[2]) as run_2:
        run_1_lines = run_1.readlines()
        run_2_lines = run_2.readlines()

        uris_1 = set()
        uris_2 = set()

        for line_run in run_1_lines:
            query = line_run.split()[0]
            uri = line_run.split()[2]
            
            if query not in queries:
                uris_1 = set()
                uris_1.add(uri)
                queries[query] = (uris_1, set())
            else:
                uris_1, _ = queries[query]
                uris_1.add(uri)
            
            

        for line_run in run_2_lines:
            query = line_run.split()[0]
            uri = line_run.split()[2]
            
            if query not in queries:
                uris_2 = set()
                uris_2.add(uri)
                queries[query] = (set(), uris_2)
            else:
                _, uris_2 = queries[query]
                uris_2.add(uri)



        jaccard_avg = 0
        for query in queries:
            uris_1, uris_2 = queries[query]
            
            intersection_runs = list(uris_1 & uris_2)
            union_runs = list(uris_1 | uris_2)
            
            jaccard_avg += len(intersection_runs) / len(union_runs)

            #print(f"Jaccard: {len(intersection_runs) / len(union_runs)}")
            #print(f"Intersection size: {len(intersection_runs)}")
            #print(f"Union size: {len(union_runs)}")

        print(f"Jaccard Avg.: {jaccard_avg / len(queries)}")

These are the EMBERT results, as-is (will need to be converted to trec_eval valid results with the helper scripts provided on the system's evaluation folder), for:
    - EMBERT (1st): MSMARCO-only model
    - EMBERT (best): MSMARCO + Dbpedia-Entity model

All models and the source code can be downloaded from https://github.com/informagi/EMBERT

We chose the first fold for the EMBERT (best) model as it was the one that provided the best 
results for their BM25F-CA runs, which don't match the published results (the ones we
obtained have better scores). The results from each fold are also provided.

# Statistical analysis
As we know that Knowgly's final scores are going to be slightly lower than the state of the art due to us not employing supervised learning, we have looked into assessing if our result sets are **statistically similar** to them. This is achieved by failing to refuse a null hypothesis of the two sets having the same origin distribution. This indicates that, despite not surpassing the state of the art, we obtain very similar results. We have implemented this by calculating the Paired Student's t-test, comparing each of the query's NDCG@10 scores, with a threshold of 0.05.

The table below shows the results, with values in **bold** indicating that they are higher than the threshold value and thus are similar.


These results are highly interesting: Our runs are statistically different from BM25, whereas all runs that employ the default BM25's k1 and b parameters fail the test for SDM, and all runs that employ Hasibi et al.'s k1 and b parameters fail it for BM25F. This indicates that the non-optimized choice of parameters is close to SDM, whereas the optimized choice is close to BM25F. Due to both involving supervised approaches (SDM also requires supervised learning to tune its three unigram and unordered/ordered bigram weights), we can successfully confirm that our approaches are close to the supervised ones, both statistically and in score.



|     Configuration    |                 |             |          |     |      |   | $p$-value (BM25) |   | $p$-value (SDM) |   | $p$-value (BM25F-CA) |
|:--------------------:|:---------------:|:-----------:|:--------:|-----|------|---|:----------------:|---|:---------------:|---|:--------------------:|
|    Metrics Aggr. 1   | Metrics Aggr. 2 |   Weights   | Clusters |  k1 |   b  |   |               |   |             |   |                      |
| Pred. Entr. for Type |        -        |      -      |     3    | 1.2 | 0.75 |   |         9.63E-43 |   |    **1.33E-01** |   |             1.26E-03 |
|                      |                 |             |          | 0.9 |  0.4 |   |         1.59E-48 |   |        1.00E-03 |   |         **3.85E-01** |
|    Ent. Type Imp.    |        -        |      -      |     3    | 1.2 | 0.75 |   |         1.73E-41 |   |    **3.24E-01** |   |             1.08E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         1.77E-46 |   |        6.44E-03 |   |         **7.03E-02** |
| Entr.-Ent. Type Imp. |        -        |      -      |     3    | 1.2 | 0.75 |   |         6.23E-43 |   |    **1.23E-01** |   |             1.94E-03 |
|                      |                 |             |          | 0.9 |  0.4 |   |         7.20E-48 |   |        1.58E-03 |   |         **2.60E-01** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.25 - 0.75 |     3    | 1.2 | 0.75 |   |         1.94E-42 |   |    **2.45E-01** |   |             2.57E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         5.35E-47 |   |        5.01E-03 |   |         **1.06E-01** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.50 - 0.50 |     3    | 1.2 | 0.75 |   |         1.85E-42 |   |    **2.66E-01** |   |             1.50E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         1.40E-45 |   |        9.83E-03 |   |         **5.07E-02** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.75 - 0.25 |     3    | 1.2 | 0.75 |   |         2.42E-43 |   |    **1.55E-01** |   |             8.09E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         1.10E-49 |   |        5.91E-04 |   |         **4.87E-01** |
|                      |                 |             |          |     |      |   |                  |   |                 |   |                      |
| Pred. Entr. for Type |        -        |      -      |     5    | 1.2 | 0.75 |   |         9.51E-43 |   |    **1.28E-01** |   |             1.54E-03 |
|                      |                 |             |          | 0.9 |  0.4 |   |         3.13E-48 |   |        7.60E-04 |   |         **4.56E-01** |
|   Entity Type Imp.   |        -        |      -      |     5    | 1.2 | 0.75 |   |         1.47E-40 |   |    **5.60E-01** |   |             1.38E-05 |
|                      |                 |             |          | 0.9 |  0.4 |   |         3.64E-48 |   |        1.73E-03 |   |         **2.77E-01** |
| Entr.-Ent. Type Imp. |        -        |      -      |     5    | 1.2 | 0.75 |   |         1.89E-41 |   |    **1.72E-01** |   |             1.64E-03 |
|                      |                 |             |          | 0.9 |  0.4 |   |         5.24E-48 |   |        1.64E-03 |   |         **2.83E-01** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.25 - 0.75 |     5    | 1.2 | 0.75 |   |         3.32E-40 |   |    **3.49E-01** |   |             1.29E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         1.93E-47 |   |        2.52E-03 |   |         **2.12E-01** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.50 - 0.50 |     5    | 1.2 | 0.75 |   |         4.67E-41 |   |    **2.52E-01** |   |             2.55E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         7.53E-48 |   |        1.72E-03 |   |         **2.58E-01** |
| Pred. Entr. for Type |  Ent. Type Imp. | 0.75 - 0.25 |     5    | 1.2 | 0.75 |   |         6.23E-42 |   |    **2.07E-01** |   |             4.36E-04 |
|                      |                 |             |          | 0.9 |  0.4 |   |         6.39E-48 |   |        1.58E-03 |   |         **2.73E-01** |
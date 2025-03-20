# Fields and Weight schemes analysis
## Field Weights and Centroid Correspondences

<div align="justify">
  
One of our additional findings from the evaluation is that we have been able to validate our assumption that the uniformly spaced values for weights were directly related to their corresponding field importances, which are in turn dictated by their centroid values. As an example, for the models shown in the paper, the Coordinate Ascent algorithm learned a 3-cluster weight scheme of $[0.7999,\ 0.4638,\ 0.3015]$ when only training field weights, and a scheme of $[0.8999,\ 0.4597,\ 0.3748]$ when also training global BM25 parameters. Similarly, on a 5-cluster configuration, we obtained the schemes $[0.8499,\ 0.4922,\ 0.0921, 0.3765,\ 0.2984]$ and $[0.8999,\ 0.6029,\ 0.2236, 0.3153,\ 0.1570]$, where it only broke our scheme on the third field. 

Given that the difference between using an optimal weight scheme and a uniformly-spaced one is small, we don't need to rely on supervised techniques to fine-tune retrieval models, as we can already obtain a performant retrieval system.

## Effects of the number of fields
One crucial aspect to decide is how many clusters, and as such index fields, we should use. We have explored different numbers of clusters, namely 3, 5, 7 and 15 with the same uniformly-spaced weight technique, whose results are shown in the table below (for the best global Metrics Aggregator configuration for 5 clusters in the DBPedia KG). When compared against the numbers of fields tested in the paper, 7 and 15 clusters yielded slightly better and worse results, respectively. Due to not observing any surprising results, we decided to stick to 3 and 5 clusters as they allowed an easier analysis of the generated index templates (e.g., we can find most of the "informative" predicates such as rdfs:label, foaf:name or dbp:name, among others, in the first cluster, whereas in 7 or 15 clusters they may be spread across more fields, difficulting their interpretability).

A more in-depth exploration of the number of clusters may yield even better results, but this would defeat our goal of not requiring an evaluation dataset. Similarly, we could also apply a one-field-per-predicate strategy, but it would result in thousands of fields in the case of DBpedia.

We recommend using 5 clusters for both the DBpedia and IMDb graphs, as this offers a finer granularity without compromising the retrieval performance (an excessive amount of fields may result in slower BM25F queries and increased storage use). Nonetheless, a KG with an even bigger number of predicates may benefit from a higher number of fields.


|     Configuration    |                 |         |          |     |      |   | SemSearch ES |        | INEX-LD |        | ListSearch |        | QALD-2 |        |   |  Total |        |
|:--------------------:|:---------------:|:-------:|:--------:|-----|------|---|:------------:|:------:|:-------:|:------:|:----------:|:------:|:------:|:------:|---|:------:|:------:|
|    Metrics Aggr. 1   | Metrics Aggr. 2 | Weights | Clusters |  k1 |   b  |   |      @10     |  @100  |   @10   |  @100  |     @10    |  @100  |   @10  |  @100  |   |   @10  |  @100  |
| Pred. Entr. for Type |        -        |    -    |     3    | 1.2 | 0.75 |   |       0.6205 | 0.6905 |  0.4041 | 0.4527 |     0.3982 | 0.4476 | 0.3373 | 0.4078 |   | 0.4350 | 0.4955 |
|                      |                 |         |          | 0.9 |  0.4 |   |       0.6424 | 0.7084 |  0.4118 | 0.4680 |     0.4266 | 0.4711 | 0.3553 | 0.4261 |   | 0.4543 | 0.5144 |
|                      |                 |         |          |     |      |   |              |        |         |        |            |        |        |        |   |        |        |
| Pred. Entr. for Type |        -        |    -    |     5    | 1.2 | 0.75 |   |       0.6178 | 0.6884 |  0.4053 | 0.4518 |     0.3957 | 0.4463 | 0.3416 | 0.4056 |   | 0.4353 | 0.4939 |
|                      |                 |         |          | 0.9 |  0.4 |   |       0.6462 | 0.7085 |  0.4170 | 0.4733 |     0.4248 | 0.4724 | 0.3529 | 0.4265 |   | 0.4552 | 0.5160 |
|                      |                 |         |          |     |      |   |              |        |         |        |            |        |        |        |   |        |        |
| Pred. Entr. for Type |        -        |    -    |     7    | 1.2 | 0.75 |   |       0.6324 | 0.7049 |  0.4033 | 0.4545 |     0.3955 | 0.4409 | 0.3354 | 0.4071 |   | 0.4365 | 0.4975 |
|                      |                 |         |          | 0.9 |  0.4 |   |       0.6544 | 0.7160 |  0.4243 | 0.4817 |     0.4244 | 0.4662 | 0.3490 | 0.4209 |   | 0.4574 | 0.5164 |
|                      |                 |         |          |     |      |   |              |        |         |        |            |        |        |        |   |        |        |
| Pred. Entr. for Type |        -        |    -    |    15    | 1.2 | 0.75 |   |       0.5883 | 0.6620 |  0.3981 | 0.4416 |     0.3943 | 0.4423 | 0.3239 | 0.3928 |   | 0.4209 | 0.4805 |
|                      |                 |         |          | 0.9 |  0.4 |   |       0.6222 | 0.6944 |  0.4078 | 0.4646 |     0.4304 | 0.4720 | 0.3391 | 0.4170 |   | 0.4447 | 0.5078 |

## Field Weights and Cluster Ordering
We have also explored the effects of different strategies for defining a field weight scheme, as shown in the table below (for the best global Metrics Aggregator configuration for 5 clusters in the DBpedia KG). During this experimentation, we have been able to assess that the best results are obtained by applying a penalization strategy, where fields of lesser importance are assigned weights $w<1.0$. Some techniques, such as applying normalized distributions, have shown promising results, and we have observed that it is also possible to apply a normalized weight distribution solely based on the centroid values of each cluster.

</div>

<div align="center">


|           Distribution          |        |       | Weights |       |       | Parameters |      |  Total |        |
|:-------------------------------:|:------:|:-----:|:-------:|-------|-------|------------|------|:------:|:------:|
|                                 |  **w. 4**  |  **w. 3** |   **w. 2**  |  **w. 1** |  **w. 0** |     **k1**     |   **b**  |   **NDCG @10**  |  **NDCG @100**  |
| Default distribution            |   1.0  |  0.8  |   0.6   |  0.4  |  0.2  |     1.2    | 0.75 | 0.4353 | 0.4939 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4552 | 0.5160 |
| Normalized default distribution | 0.333  | 0.266 |  0.200  | 0.133 | 0.066 |     1.2    | 0.75 | 0.4089 | 0.4578 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4469 | 0.4991 |
| Offset default distribution     |   1.8  |  1.6  |   1.4   |  1.2  |  1.0  |     1.2    | 0.75 | 0.4238 | 0.4880 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4292 | 0.4933 |
|                                 |        |       |         |       |       |            |      |        |        |
| Normalized distribution         |   1.0  |  0.75 |   0.5   |  0.25 |  0.0  |     1.2    | 0.75 | 0.4326 | 0.4928 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4525 | 0.5150 |
| Increasing weights              |   5.0  |  4.0  |   3.0   |  2.0  |  1.0  |     1.2    | 0.75 | 0.3978 | 0.4638 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4058 | 0.4680 |
| Normalized centroid values      | 0.708  | 0.201 |  0.068  | 0.019 | 0.001 |     1.2    | 0.75 | 0.4205 | 0.4779 |
|                                 |        |       |         |       |       |     0.9    |  0.4 | 0.4463 | 0.5062 |

</div>

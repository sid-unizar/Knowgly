# Implementation details

## Importance Metrics Calculation

<div align="justify">

The Importance Metrics calculation stage requires a careful implementation due to the quadratic cost in time, memory and storage of our metrics, which corresponds to $\mathcal{O}(n \cdot m)$, where $n$ is the number of predicates in the Knowledge Graph and $m$ is the number of possible entity types. This means that, on KGs with deep type hierarchies such as DBpedia, this cost is easily noticeable.


In order to offset this, we calculate and store all necessary metrics in an offline manner, prior to being used by any Metrics Aggregator, using a [HDT](https://www.rdfhdt.org/) version of the Knowledge Graph. HDT's low-level access to the Knowledge Graph handles references to subjects, predicates and objects as 64-bit unsigned integers, enabling us to cache intermediate metrics in memory. Furthermore, it allows us to easily parallelize every metric generation step by considering the metric for a given predicate $p$ as a single independent job (A metric for $(p, t)$ won't have any dependencies on other predicates or types, so it is trivially parallelizable). A pure SPARQL-based implementation is also possible, and provided as part of the supplemental materials, but its performance will highly depend on the SPARQL engine it is used on. 

The following table shows a breakdown of time and memory costs for the different stages of our system (Note the differences in time between DBpedia and IMDb, due to IMDb's *easier* type hierarchy). All tasks were executed on a desktop PC with a i9-12900K 3.2GHz CPU (16-cores/24-threads) and 64GiB of memory.

</div>

<div align="center">

|                           Task                           |   KG    |            Time needed            | Max. memory  usage |
|:--------------------------------------------------------:|:-------:|:---------------------------------:|:------------------:|
| Submetrics calculation                                   | DBpedia | 09hr. 15m. 07s.                   |      15.01 GiB     |
| Submetrics calculation                                   | IMDb    | 00hr. 02m. 55s.                   |      11.63 GiB     |
|                                                          |         |                                   |                    |
| Generate Predicate Entropy for Type Metrics              | DBpedia | 00hr. 03m. 42s.                   |      15.01 GiB     |
| Generate Predicate Entropy for Type Metrics              | IMDb    | 00hr. 02m. 21s.                   |      11.63 GiB     |
|                                                          |         |                                   |                    |
| Generate Entity Type Importance Metrics                  | DBpedia | 00hr. 00m. 05s.                   |      15.01 GiB     |
| Generate Entity Type Importance Metrics                  | IMDb    | 00hr. 00m. 07s.                   |      11.63 GiB     |
|                                                          |         |                                   |                    |
| Create a global template (HDT read + KMeans++ clustering)          | DBpedia |                           06.90s. |          -         |
| Create a global template (HDT read + KMeans++ clustering)          | IMDb    |                           00.05s. | -                  |
|                                                          |         |                                   |                    |
| Create and write VTs for all entities (Global template)  | DBpedia |                         18m. 45s. |          -         |
| Create and write VTs for all entities (Global template)            | IMDb    |                         07m. 40s. |          -         |

</div>

<div align="justify">
This metrics calculation step only needs to be performed once, as they will be stored in a separate KG or as part of the original one. All Metrics Aggregators will then use these stored metrics to obtain a Virtual Document Template containing the final index schema.

## Clustering 
Using KMeans++ for clustering each predicate's metric data across $n$ clusters has proven to be extremely efficient during our testing, as shown in the above table for VDoc creation, due to it only needing to cluster singleton values.

Modifications to the maximum number of iterations have shown to slightly change the location of a reduced number of predicates across clusters of least importance (i.e., those with the smallest centroid values), while clusters of higher importance remained unchanged. This behavior was offset by using [KMeans++](https://en.wikipedia.org/wiki/K-means%2B%2B) instead of its classic implementation, choosing the best clusterization from several repetitions in order to ensure its stability, as each clusterization step is negligible in terms of time. For this, we employed the `MultiKMeansPlusPlusClusterer` implementation from [`commons-math`](https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/ml/clustering/KMeansPlusPlusClusterer.html), which compares clusters via intra-cluster distance variances. We fixed the number of clustering repetitions to 5.

</div>

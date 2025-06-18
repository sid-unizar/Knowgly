<div align="center">

# Repository of Information-aware Entity Indexing in Knowledge Graphs to Enable Semantic Search and its implementation, `Knowgly`.

![screenshot](figures/vdoc_example.png)

</div>

# Project overview
### Project Name
`Knowgly`
### Project Purpose and Goals
Entity Indexing and Retrieval in Knowledge Graphs, via an information-based and fully unsupervised index schema construction
### Paper:
García, S., Bobed, C. (2025). Information-Aware Entity Indexing in Knowledge Graphs to Enable Semantic Search. In: Curry, E., et al. The Semantic Web. ESWC 2025. Lecture Notes in Computer Science, vol 15718. Springer, Cham. 

https://doi.org/10.1007/978-3-031-94575-5_12

# Table of Contents
1. [Requirements](#requirements)
2. [Compiling](#compiling)
3. [Usage](#usage)
4. [Supplemental materials](#supplemental-materials)
5. [Licensing](#licensing)



## Requirements
    
The system consists of a Java project implementing the whole pipeline presented in the paper. There are also several helper scripts written in both Python and bash for evaluation and handling external systems, such as Terrier or Galago.
The requirements are the following:
- `Maven`
- openJDK >=17. `pom.xml` assumes `openJDK 20`, but can be easily changed in the `maven.compiler.source` and `maven.compiler.target` settings.
- If using `galago` (recommended):
    - A valid installation of galago under `../galago/galago-3.16/` and `openJDK 8`. It is also possible to use newer versions.
      See how_to_setup_galago.txt on `../galago/` for more information.

- If using `pyTerrier`:
    - A valid installation of pyTerrier under `../terrier/`. See how_to_setup_terrier.txt on `../terrier/` for more information.

- If using `elastic`:
    - An accesible local or remote elastic instance. We have tested our system under version 8.6.2. See `configuration/examples/elasticEndpointConfiguration.json` for more details.

- If using `Lucene`:
    - Nothing. The Lucene libraries are already included in Knowgly, and it will use them to create a local index.     
  
- If performing evaluations
    - A compilled executable of https://github.com/usnistgov/trec_eval, and a python environment with `numpy` and `scikit-learn` installed. See `Knowgly/evaluation/metrics_testing/README.txt` for more details.

-----------------------------------------------------------------------------------------------------------------------------------------------

## Compiling
To compile the system, simply run `compile.sh`. It will compile and move to this folder all required .jar files, with all dependencies statically linked.

> [!WARNING]
> Some systems have limitations
> - `Lucene` and `elastic` cannot use weights below 1.0 in BM25F queries
> - `pyTerrier` does not properly tune k1 and b parameters, despite exposing them
> - `galago` requires running `build_galago_index.sh` after performing indexing within Knowgly, as it requires calling a Java 8 executable.


> [!TIP]
> We recommend using `galago` for reproducing our results, or `Lucene` for considerably faster indexing and retrieval (albeit with slightly worse performance due to its field weighting limitations)

-----------------------------------------------------------------------------------------------------------------------------------------------

## Usage
If you want to:
- Run Knowgly's metrics generation and indexing pipelines:
    - Freely edit the demo shown in the `Main.java` file, and run `execute.sh` (or use Knowgly as a library).
    - A simple demo on how to run each pipeline is already provided in the file.

- Perform individual queries:
    - Please refer to the demo shown in the `Main.java` file

- Perform a full evaluation (multiple queries and .run file generation):
    - Execute the `RunEvaluator.jar` file, which has been prepared as a CLI tool for any system. Our evaluation scripts
      employ this executable too.

- Perform Coordinate Ascent **(Note: We allow all systems, but it has only been tested on galago)**:
    - Run the `ca*.py` scripts in the `CA` folder. There are currently scripts for 3 and 5 fields.

- Evaluate .run files:
    - See `Knowgly/evaluation/metrics_testing/README.txt` for more details.

- Build the datasets we used for evaluation (`DBpedia` and `IMDb`):
    - See `Knowgly/datasets/README.txt` for more details.

> [!IMPORTANT]
> - Please check all configuration files under `configuration/examples/`. All neccesary configuration files should be placed under `configuration`
> **before** running any of the pipelines.
> - Although some parts of the pipelines may support classic SPARQL endpoints (Local Jena/Jena--fuseki models/endpoints and remote SPARQL endpoints),
> **all functionalities are only feature-complete and tested for local HDT endpoints**. In particular, metrics generation is too computationally expensive
> when done naively on SPARQL, and thus not fully implemented nor tested on non-HDT endpoints.

-----------------------------------------------------------------------------------------------------------------------------------------------


## Supplemental materials
Aside from Knowgly's implementation, we also provide additional documentation, as mentioned throughout the paper:
  - [dataset_analysis.md](additional_documentation/dataset_analysis.md): An overview of the dataset details and Predicate-Type distributions.
  - [implementation_details.md](additional_documentation/implementation_details.md): An analysis of Importance Metrics Calculation times for both datasets (DBpedia and IMDb) and additional clustering (KMeans) details.
  - [fields_and_weight_scheme_analysis.md](additional_documentation/fields_and_weight_scheme_analysis.md): An analysis of the effect of different numbers of fields and an overview of alternative weight scheme strategies, such as directly assigning normalized centroid values.

Additionally, the paper's [figures](figures) are also available. A small subset of VDoc templates are also available in [example_vdocs](example_vdocs), and the best results are available as `.run` files in [best_runs](best_runs).

-----------------------------------------------------------------------------------------------------------------------------------------------

## Licensing
This software is licensed under the GNU Affero General Public License v3.0

<div align="center">
    <img src="https://img.shields.io/badge/We_love-Pommi-blue"/>
</div>

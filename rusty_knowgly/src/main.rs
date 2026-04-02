mod entity_indexer;
mod metrics_generation;
mod qlever_client;

use crate::entity_indexer::{
    create_entity_representations_from_entity_iris_list,
    create_entity_representations_from_extracted_entities, get_entity_iris_from_laque_dataset,
};
use crate::metrics_generation::clustering::{
    clusters_to_json, coalesce_metrics, get_clusters, json_to_clusters,
};
use crate::metrics_generation::entity_type_importance_metrics_generator::get_entity_type_importances;
use crate::metrics_generation::entropy_type_importance_metrics_generator::get_entropy_type_importances;
use crate::metrics_generation::{json_to_metrics, metrics_to_json};
use crate::qlever_client::SPARQLEndpoint;
use clap::{Parser, ValueEnum};
use env_logger::Builder;
use log::LevelFilter;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    task: Task,
}

#[derive(Debug, clap::Subcommand)]
enum Task {
    /// Calculate one of the metrics
    CalculateMetrics(CalculateMetrics),

    /// Cluster previously calculated metrics
    ClusterMetricResults(ClusterMetricResults),

    /// Create VDocs from an entity IRIs file
    IndexFromEntityIris(IndexFromEntityIRIs),

    /// Create VDocs from an extracted entities file
    IndexFromExtractedEntities(IndexFromExtractedEntities),
}

#[derive(Debug, clap::Args)]
pub struct SPARQLEndpointIRI {
    #[arg(long)]
    endpoint: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum MetricToCalculate {
    #[value(name = "entity_type_importance")]
    EntityTypeImportance,
    #[value(name = "entropy_type_importance")]
    EntropyTypeImportance,
}

#[derive(Debug, clap::Args)]
pub struct CalculateMetrics {
    /// Which metric to calculate
    #[arg(long, value_enum)]
    metric_to_calculate: MetricToCalculate,

    /// .json file path containing a dictionary of Type IRI -> Predicate IRI -> metric value
    #[arg(long)]
    output: String,

    #[command(flatten)]
    pub sparqlendpoint_iri: SPARQLEndpointIRI,
}

#[derive(Debug, clap::Args)]
pub struct ClusterMetricResults {
    /// .json file path containing a dictionary of Type IRI -> Predicate IRI -> metric value
    #[arg(long)]
    calculated_metrics_file: String,

    /// .json file path containing a list of tuples (metric value, list of predicate IRIs) (i.e., A fielded VDoc template)
    #[arg(long)]
    output: String,

    /// Number of clusters / fields to create
    #[arg(long)]
    n_clusters: u8,
}

#[derive(Debug, clap::Args)]
pub struct CommonIndexParams {
    /// .json file path containing a dictionary of Type IRI -> Predicate IRI -> metric value
    #[arg(long)]
    clustered_metrics_file: String,

    /// .jsonl file containing one EntityRecord (i.e., a VDoc consisting of a tuple of
    /// (entity_iri, dictionary of lexicalized predicate -> list of lexicalized objects))
    /// per line
    #[arg(long)]
    output: String,
}

#[derive(Debug, clap::Args)]
pub struct IndexFromEntityIRIs {
    /// A headerless (i.e.,  no column names) .tsv file containing entity Iris in its first column
    /// Used to load LaQuE_collection.tsv
    #[arg(long)]
    entity_iris_file: String,

    #[command(flatten)]
    pub common_index_params: CommonIndexParams,

    #[command(flatten)]
    pub sparqlendpoint_iri: SPARQLEndpointIRI,
}

#[derive(Debug, clap::Args)]
pub struct IndexFromExtractedEntities {
    /// .jsonl file containing one EntityRecord (i.e., a VDoc consisting of a tuple of
    /// (entity_iri, dictionary of lexicalized predicate -> list of lexicalized objects))
    /// per line
    #[arg(long)]
    extracted_entities_file: String,

    #[command(flatten)]
    pub common_index_params: CommonIndexParams,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new().filter_level(LevelFilter::Info).init();

    let cli = Cli::parse();

    match cli.task {
        Task::CalculateMetrics(args) => {
            SPARQLEndpoint::new(&args.sparqlendpoint_iri.endpoint);

            let metrics = match args.metric_to_calculate {
                MetricToCalculate::EntityTypeImportance => get_entity_type_importances()?,
                MetricToCalculate::EntropyTypeImportance => get_entropy_type_importances()?,
            };
            metrics_to_json(&metrics, &args.output)?;
        }

        Task::ClusterMetricResults(args) => {
            let metrics = json_to_metrics(&args.calculated_metrics_file)?;
            let clustered_predicates = get_clusters(&coalesce_metrics(&metrics), args.n_clusters);
            clusters_to_json(&clustered_predicates, &args.output)?;
        }

        Task::IndexFromEntityIris(args) => {
            SPARQLEndpoint::new(&args.sparqlendpoint_iri.endpoint);

            let clustered_predicates =
                json_to_clusters(&args.common_index_params.clustered_metrics_file)?;

            create_entity_representations_from_entity_iris_list(
                get_entity_iris_from_laque_dataset(&args.entity_iris_file)?,
                clustered_predicates,
                &args.common_index_params.output,
            )?;
        }

        Task::IndexFromExtractedEntities(args) => {
            let clustered_predicates =
                json_to_clusters(&args.common_index_params.clustered_metrics_file)?;

            create_entity_representations_from_extracted_entities(
                &args.extracted_entities_file,
                clustered_predicates,
                &args.common_index_params.output,
            )?;
        }
    }

    Ok(())
}

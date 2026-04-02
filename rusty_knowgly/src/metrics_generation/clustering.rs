//! Clustering functions to apply on metrics generator results
//!

use ckmeans::ckmeans_indices;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use itertools::Itertools;

/// Coalesces any metrics generated for type-predicate pairs into predicate-only metrics
/// This is done currently as a sum over all types
///
/// **Arguments**
/// * `metrics` - HashMap of Type IRI -> Predicate IRI -> Metric value.
///
/// **Outputs**
/// * A HashMap of Predicate IRI -> Metric value
pub fn coalesce_metrics(metrics: &HashMap<String, HashMap<String, f64>>) -> HashMap<String, f64> {
    metrics
        .values()
        .fold(HashMap::new(), |mut coalesced_metrics, inner_map| {
            for (key, &value) in inner_map {
                *coalesced_metrics.entry(key.clone()).or_insert(0.0) += value;
            }
            coalesced_metrics
        })
}

/// A cluster resulting from calling `get_clusters`
#[derive(Serialize, Deserialize)]
pub struct PredicatesCluster {
    /// Average of all predicate metrics in this cluster
    pub avg_metric: f64,
    /// Predicate IRIs present in this cluster
    pub predicate_iris: Vec<String>,
}

/// Clusters the predicates based on their metric values
/// The metrics must be coalesced beforehand. This can be done via e.g. `coalesce_metrics`
///
/// **Arguments**
/// * `k` - Number of clusters
/// * `coalesced_metrics` - HashMap of Predicate IRI -> Metric value.
///
/// **Outputs**
/// * A Vec of `PredicatesCluster`
pub fn get_clusters(coalesced_metrics: &HashMap<String, f64>, k: u8) -> Vec<PredicatesCluster> {
    let mut entries: Vec<(String, f64)> = coalesced_metrics
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();

    entries.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let sorted_values: Vec<f64> = entries.iter().map(|e| e.1).collect();
    let (_, cluster_indices) = ckmeans_indices(&sorted_values, k).unwrap();

    cluster_indices
        .into_iter()
        .map(|(start, end)| {
            let cluster_slice = &entries[start..=end];
            let count = cluster_slice.len() as f64;

            let sum: f64 = cluster_slice.iter().map(|(_, v)| v).sum();
            let avg_metric = sum / count;

            let predicate_iris = cluster_slice.iter().map(|(k, _)| k.clone()).collect();

            PredicatesCluster {
                avg_metric,
                predicate_iris,
            }
        })
        .sorted_by(|a, b| {
            b.avg_metric
                .partial_cmp(&a.avg_metric)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .collect()
}

/// Converts the results of `get_clusters` into a JSON file
///
/// **Arguments**
/// * `clusters`: Results of `get_clusters`
/// * `path`: Where to write the JSON file
pub fn clusters_to_json(clusters: &[PredicatesCluster], path: &str) -> std::io::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, clusters)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}

/// Converts a JSON file back into a Vec of PredicatesCluster
///
/// **Arguments**
/// * `path`: The path to the JSON file
///
/// **Outputs**
/// * A Vec of `PredicatesCluster`
pub fn json_to_clusters(path: &str) -> std::io::Result<Vec<PredicatesCluster>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let clusters: Vec<PredicatesCluster> = serde_json::from_reader(reader)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(clusters)
}

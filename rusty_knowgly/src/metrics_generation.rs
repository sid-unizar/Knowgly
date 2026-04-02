use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};

pub mod clustering;
mod common;
pub mod entity_type_importance_metrics_generator;
pub mod entropy_type_importance_metrics_generator;

/// Converts the results of any metrics into a JSON file
///
/// **Arguments**
/// * `metrics`: A Hashmap of Type IRI -> Predicate IRI -> value
/// * `path`: Where to write the JSON file
pub fn metrics_to_json(
    metrics: &HashMap<String, HashMap<String, f64>>,
    path: &str,
) -> std::io::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, metrics)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}

/// Converts a JSON file back into a metrics HashMap
///
/// **Arguments**
/// * `path`: The path to the JSON file
///
/// **Output**
/// * A Hashmap of Type IRI -> Predicate IRI -> value
pub fn json_to_metrics(path: &str) -> std::io::Result<HashMap<String, HashMap<String, f64>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let metrics: HashMap<String, HashMap<String, f64>> = serde_json::from_reader(reader)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(metrics)
}

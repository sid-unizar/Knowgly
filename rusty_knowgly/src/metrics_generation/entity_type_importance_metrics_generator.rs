//! Metrics generator for the EntityTypeImportance metric

use crate::metrics_generation::common::{dbpedia_namespace_filter, fetch_all_type_iris};
use crate::qlever_client::SPARQLEndpoint;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use sophia::api::sparql::{SparqlDataset, SparqlResult};
use sophia::api::term::Term;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;

/// Given a fixed type $t$, calculates $EF_p(p,t)$ for every predicate $p$ associated with it (Formula (2))
///
/// **Arguments**
/// * `type_iri` - Type for which the metric will be calculated
///
/// **Output**
/// * A Hashmap of Predicate IRI -> value
fn calculate_entity_frequency_p_t(type_iri: &str) -> HashMap<String, u64> {
    let query = format!(
        r#"SELECT ?p (COUNT(DISTINCT ?s) AS ?ef_p_t) WHERE {{
            ?s a <{}> .
            ?s ?p ?o .
        }} GROUP BY ?p"#,
        type_iri
    );

    let mut counts = HashMap::new();
    if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
        for row in bindings.flatten() {
            let prop_iri = row
                .get(0)
                .and_then(|t| t.as_ref())
                .and_then(|t| t.iri())
                .map(|i| i.to_string());
            let ef_p_t = row
                .get(1)
                .and_then(|t| t.as_ref())
                .and_then(|t| t.lexical_form())
                .and_then(|s| s.parse::<u64>().ok());

            if let (Some(prop_iri), Some(ef_p_t)) = (prop_iri, ef_p_t) {
                counts.insert(prop_iri, ef_p_t);
            }
        }
    }
    counts
}

/// Calculates $|\mathcal{E}_t|$ for every type $t$ in the KG (Table (1))
///
/// **Output**
/// * A Hashmap of Type IRI -> value
fn calculate_entity_frequencies_for_all_types() -> HashMap<String, u64> {
    let query = format!(
        r#"SELECT ?type (COUNT(DISTINCT ?s) AS ?ef_t) WHERE {{
            ?s a ?type.
        }} GROUP BY ?type"#,
    );

    let mut counts = HashMap::new();
    if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
        for row in bindings.flatten() {
            let type_iri = row
                .get(0)
                .and_then(|t| t.as_ref())
                .and_then(|t| t.iri())
                .map(|i| i.to_string());
            let ef_t = row
                .get(1)
                .and_then(|t| t.as_ref())
                .and_then(|t| t.lexical_form())
                .and_then(|s| s.parse::<u64>().ok());

            if let (Some(type_iri), Some(ef_t)) = (type_iri, ef_t) {
                counts.insert(type_iri, ef_t);
            }
        }
    }
    counts
}

/// Calculates $ETImp_p(p, t)$ for every type $t$ and predicate $p$ in the KG (Formula (1))
/// Entries will be missing for any predicate-type pair not present in the KG via any of its entities
///
/// **Output**
/// * A Hashmap of Type IRI -> Predicate IRI -> value
pub fn get_entity_type_importances()
-> Result<HashMap<String, HashMap<String, f64>>, Box<dyn std::error::Error>> {
    // We divide the calculation across all types explicitly. This avoids query optimization and join
    // problems if we were to launch this as a "mega-query" for every predicate and type instead
    let type_iris = fetch_all_type_iris(Some(dbpedia_namespace_filter))?;
    log::info!(
        "Calculating entity type importances for {} types",
        type_iris.len()
    );

    let pb = ProgressBar::new(type_iris.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{wide_msg}: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} (ETA: {eta})")?
        .progress_chars("#>-"));

    // Type IRI -> Predicate IRI -> Unique entities
    let entity_frequencies_p_t: HashMap<String, HashMap<String, u64>> = type_iris
        .par_iter()
        .progress_with(pb.with_message("Calculating EF_p_t"))
        .map(|type_iri| {
            let frequencies = calculate_entity_frequency_p_t(type_iri);

            (type_iri.clone(), frequencies)
        })
        .collect();

    log::info!("Calculating entity frequencies for all types");
    // Type IRI -> $|\mathcal{E}_t|$
    let e_t = calculate_entity_frequencies_for_all_types();

    // Type IRI -> Predicate IRI -> Entity Type Importance
    let et_imp: HashMap<String, HashMap<String, f64>> = entity_frequencies_p_t
        .into_iter()
        .map(|(type_iri, ef_p_t_counts)| {
            // Get total entities for this type from ef_t
            let total_entities_t = *e_t.get(&type_iri).unwrap_or(&0) as f64;

            let importances = ef_p_t_counts
                .into_iter()
                .map(|(pred_iri, ef_p_t)| {
                    let ef_p_t = ef_p_t as f64;

                    // ETImp_p(p, t) = EF_p(p, t) * log(|EF(t)| / EF_p(p, t))
                    let score = if ef_p_t > 0.0 && total_entities_t > 0.0 {
                        ef_p_t * (total_entities_t / ef_p_t).log(2.0)
                    } else {
                        0.0
                    };

                    (pred_iri, score)
                })
                .collect();

            (type_iri, importances)
        })
        .collect();

    log::info!(
        "Calculated entity type importance scores for {} types",
        et_imp.len()
    );

    Ok(et_imp)
}

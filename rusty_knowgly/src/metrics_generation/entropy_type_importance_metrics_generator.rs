//! Metrics generator for the EntropyTypeImportance metric
//!

use crate::metrics_generation::common::{
    dbpedia_namespace_filter, fetch_all_properties_for_each_type,
};
use crate::qlever_client::SPARQLEndpoint;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use sophia::api::sparql::{SparqlDataset, SparqlResult};
use sophia::api::term::Term;
use std::collections::HashMap;


/// Given a fixed type $t$ and all properties found associated with it (via the entities in the KG),
/// calculates $FF(f, t)$ (Formula (5))
///
/// **Arguments**
/// * `type_iri` - Type for which the metric will be calculated
/// * `property_iris` - Properties found associated with $t$ (via the entities in the KG)
///
/// **Output**
/// * A Hashmap of Predicate IRI -> Object IRI or literal -> value
fn calculate_fact_frequencies_for_type(
    type_iri: &str,
    property_iris: &Vec<String>,
) -> HashMap<String, HashMap<String, u64>> {
    let mut counts: HashMap<String, HashMap<String, u64>> = HashMap::new();
    for prop_iri in property_iris.iter() {
        let query = format!(
            r#"SELECT ?o (COUNT(?s) AS ?ff_f_t) WHERE {{
                ?s a <{}> .
                ?s <{}> ?o .
        }} GROUP BY ?o"#,
            type_iri, prop_iri
        );

        if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
            for row in bindings.flatten() {
                let obj = row
                    .get(0)
                    .and_then(|t| t.as_ref())
                    .and_then(|t| {
                        t.iri()
                            .map(|i| i.to_string())
                            .or_else(|| t.lexical_form().map(|l| l.to_string()))
                    });
                    //.and_then(|t| t.iri()) // We want both IRIs and literals, so we return the wrapping term itself
                let ff_f_t = row
                    .get(1)
                    .and_then(|t| t.as_ref())
                    .and_then(|t| t.lexical_form())
                    .and_then(|s| s.parse::<u64>().ok());

                if let (Some(obj), Some(ff_f_t)) = (obj, ff_f_t) {
                    counts
                        .entry(prop_iri.to_string())
                        .or_default()
                        .insert(obj, ff_f_t);
                }
            }
        }
    }

    counts
}

/// Given a fixed type $t$ and all properties found associated with it (via the entities in the KG),
/// calculates $FF_p(f_p, t)$ (Formula (6))
///
/// **Arguments**
/// * `type_iri` - Type for which the metric will be calculated
/// * `property_iris` - Properties found associated with $t$ (via the entities in the KG)
///
/// **Output**
/// * A Hashmap of Predicate IRI -> value
fn calculate_fact_frequencies_for_p_and_type(
    type_iri: &str,
    property_iris: &Vec<String>,
) -> HashMap<String, u64> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for prop_iri in property_iris.iter() {
        let query = format!(
            r#"SELECT (COUNT(?s) AS ?ff_p_t) WHERE {{
                ?s a <{}> .
                ?s <{}> ?o .
            }}"#,
            type_iri, prop_iri
        );

        if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
            for row in bindings.flatten() {
                let ff_p_t = row
                    .get(0)
                    .and_then(|t| t.as_ref())
                    .and_then(|t| t.lexical_form())
                    .and_then(|s| s.parse::<u64>().ok());

                if let Some(ff_p_t) = ff_p_t {
                    counts.insert(prop_iri.to_string(), ff_p_t);
                }
            }
        }
    }
    counts
}

/// Calculates $EntF_p(p, t)$ for every type $t$ and predicate $p$ in the KG (Formula (3))
/// Entries will be missing for any predicate-type pair not present in the KG via any of its entities
///
/// **Output**
/// * A Hashmap of Type IRI -> Predicate IRI -> value
pub fn get_entropy_type_importances()
-> Result<HashMap<String, HashMap<String, f64>>, Box<dyn std::error::Error>> {
    // Similarly to the entity type importance metrics, we divide the calculation to avoid launching
    // a "mega-query" for every predicate and type instead
    //
    // In this case, since we need to scan objects instead of entities, we have optimized this more
    // aggressively by precalculating all property-type pairs that appear in the KG. This prevents us
    // from launching queries that are going to yield no results anyways.
    let types_and_properties = fetch_all_properties_for_each_type(Some(dbpedia_namespace_filter))?;
    log::info!(
        "Calculating entropy type importances for {} types",
        types_and_properties.len()
    );

    let pb_style = ProgressStyle::default_bar()
        .template("{wide_msg}: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} (ETA: {eta})")?
        .progress_chars("#>-");

    let mut pb = ProgressBar::new(types_and_properties.len() as u64);
    pb.set_style(pb_style.clone());

    // Type IRI -> Predicate IRI -> Object IRI or literal > unique entities
    let fact_frequencies_f_t: HashMap<String, HashMap<String, HashMap<String, u64>>> =
        types_and_properties
            .iter()
            .par_bridge()
            .progress_with(pb.with_message("Calculating FF_t"))
            .map(|(type_iri, properties)| {
                let frequencies = calculate_fact_frequencies_for_type(
                    type_iri,
                    properties,
                );

                (type_iri.clone(), frequencies)
            })
            .collect();

    pb = ProgressBar::new(types_and_properties.len() as u64);
    pb.set_style(pb_style);

    // Type IRI -> Predicate IRI -> unique entities
    let fact_frequencies_p_t: HashMap<String, HashMap<String, u64>> = types_and_properties
        .keys()
        .par_bridge()
        .progress_with(pb.with_message("Calculating FF_p_t"))
        .map(|type_iri| {
            let frequencies = calculate_fact_frequencies_for_p_and_type(
                type_iri,
                &types_and_properties.get(type_iri).unwrap(),
            );

            (type_iri.clone(), frequencies)
        })
        .collect();

    // Type IRI -> Predicate IRI -> Entropy Type Importance
    let entropy_type_importances: HashMap<String, HashMap<String, f64>> = fact_frequencies_p_t
        .into_iter()
        .map(|(type_iri, ff_p_t_counts)| {
            let n_preds_fact_frequencies_p_t = ff_p_t_counts.len();
            let n_preds_fact_frequencies_f_t = fact_frequencies_f_t.get(&type_iri).unwrap().len();

            // Predicate IRI -> Object IRI or literal -> PF_f_t
            let pf_f_t_counts: HashMap<String, HashMap<String, f64>> = fact_frequencies_f_t
                .get(&type_iri)
                .unwrap()
                .iter()
                .map(|(p_iri, ff_t_for_p_counts)| {
                    let inner_map: HashMap<String, f64> = ff_t_for_p_counts
                        .iter()
                        .map(|(obj, ff_t_for_p_and_o)| {
                            // FF_p_t_o / FF_p_t
                            let value = *ff_t_for_p_and_o as f64
                                / *ff_p_t_counts.get(p_iri).unwrap() as f64; // TODO fact_frequencies_p_t does not have a p_iri, but fact_frequencies_f_t does
                            (obj.clone(), value)
                        })
                        .collect();

                    (p_iri.to_string(), inner_map)
                })
                .collect();

            let importances: HashMap<String, f64> = pf_f_t_counts
                .into_iter()
                .map(|(pred_iri, pf_f_t_values_for_p)| {
                    let entropy: f64 = pf_f_t_values_for_p
                        .into_iter()
                        .map(|(_obj, value)| {
                            if value > 0.0 {
                                value * value.log2()
                            } else {
                                0.0
                            }
                        })
                        .sum();

                    (pred_iri, -entropy)
                })
                .collect();

            (type_iri, importances)
        })
        .collect();

    log::info!(
        "Calculated entropy type importance scores for {} types",
        entropy_type_importances.len()
    );

    Ok(entropy_type_importances)
}

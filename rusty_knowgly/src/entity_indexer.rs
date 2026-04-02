#![allow(unused)]

use crate::metrics_generation::clustering::PredicatesCluster;
use crate::qlever_client::SPARQLEndpoint;
use csv::ReaderBuilder;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use once_cell::sync::Lazy;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_jsonlines::JsonLinesReader;
use sophia::api::prelude::SparqlDataset;
use sophia::api::sparql::SparqlResult;
use sophia::api::term::Term;
use sophia::api::MownStr;
use sophia::iri::IriRef;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use reqwest::blocking::Client;
use sophia::sparql_client::SparqlClient;


static CAMEL_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"([a-z0-9])([A-Z])").unwrap());
static ENTITY_DETAILS_QUERY_CHUNK_SIZE: usize = 1000;
static ENTITY_DETAILS_WORKERS: usize = 8; // As many as there are threads in the default Qlever settings


/// Fetch all unique entity (subject) IRIs in the KG
///
/// **Outputs**
/// * A Vec of Entity IRIs
pub fn get_all_entity_iris() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    log::info!("Fetching all entity iris");

    let query = "SELECT DISTINCT ?s WHERE { ?s ?p ?o. }";
    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        let iris = bindings
            .filter_map(|row| {
                let r = row.ok()?;
                let s_iri = r.get(0)?.as_ref()?;
                Some(s_iri.iri()?.to_string())
            })
            .collect();
        Ok(iris)
    } else {
        Ok(vec![])
    }
}

/// Fetch all unique predicate labels in the KG
///
/// **Outputs**
/// * A HashMap of Predicate IRI -> Predicate label
pub fn fetch_all_pred_labels() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    log::info!("Fetching all predicate labels");

    let query = "SELECT DISTINCT ?p ?p_label WHERE { ?s ?p ?o. ?p <http://www.w3.org/2000/01/rdf-schema#label> ?p_label. }";
    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        let p_label_pairs = bindings
            .filter_map(|row| {
                let r = row.ok()?;
                let pred_iri = r.get(0)?.as_ref()?;
                let pred_label = r.get(1)?.as_ref()?;
                Some((
                    pred_iri.iri()?.to_string(),
                    pred_label.lexical_form()?.to_string(),
                ))
            })
            .collect();
        Ok(p_label_pairs)
    } else {
        Ok(HashMap::new())
    }
}

/// Fetch all unique object labels in the KG
///
/// **Outputs**
/// * A HashMap of Object IRI -> Object label
pub fn fetch_all_obj_labels() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    log::info!("Fetching all object labels");

    let query = "SELECT DISTINCT ?o ?o_label WHERE { ?o <http://www.w3.org/2000/01/rdf-schema#label> ?o_label. ?s ?p ?o. }";
    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        let o_label_pairs = bindings
            .filter_map(|row| {
                let r = row.ok()?;
                let obj_iri = r.get(0)?.as_ref()?;
                let obj_label = r.get(1)?.as_ref()?;
                Some((
                    obj_iri.iri()?.to_string(),
                    obj_label.lexical_form()?.to_string(),
                ))
            })
            .collect();
        Ok(o_label_pairs)
    } else {
        Ok(HashMap::new())
    }
}

/// Reads the entity IRIs from the LaQuE dataset (LaQuE_collection.tsv)
///
/// **Outputs**
/// * A Vec of entity IRIs
pub fn get_entity_iris_from_laque_dataset(
    path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_path(path)?;

    Ok(reader
        .records()
        .filter_map(|result| result.ok())
        .filter_map(|record| record.get(0).map(String::from))
        .collect())
}

/// Creates a lexicalized version of the IRI's string
/// This should only be used as a fallback for those IRIs that lack a label, as it assumes
/// that the IRI contains some form of meaningful string
///
/// **Outputs**
/// * The lexicalized IRI
fn lexicalize_iri(iri: &IriRef<MownStr>) -> String {
    // Extract the IRI's path or fragment
    let iri_str = iri.as_str();
    let fragment = iri_str
        .split(|c| c == '/' || c == '#')
        .last()
        .unwrap_or(iri_str);

    // Split camelCases
    let split_camel = CAMEL_REGEX.replace_all(fragment, "$1 $2");

    // Split underscores
    let split_camel = split_camel.replace('_', " ");

    split_camel.to_lowercase()
}

/// Given an entity IRI and caches of predicate and object labels, create a lexicalized representation
/// of the entity (i.e., a Virtual Document / VDoc)
/// All predicates and objects will be represented as a string, either through their labels or
/// via a fallback `lexicalize_iri` function that assumes that there is some meaningful string
/// inside the IRIs
///
/// **Arguments**
/// * `entity_iri` - Entity IRI
/// * `pred_labels` - HashMap of Predicate IRI -> Predicate label
/// * `obj_labels` - HashMap of Object IRI -> Object label
///
/// **Output**
/// * A Hashmap of (Predicate IRI, Predicate label) -> Vec of lexicalized objects
pub fn get_lexicalized_entity_p_o_entries(
    entity_iri: &str,
    pred_labels: &HashMap<String, String>,
    obj_labels: &HashMap<String, String>,
) -> HashMap<(String, String), Vec<String>> {
    let mut p_o_entries: HashMap<(String, String), Vec<String>> = HashMap::new();

    let query = format!(
        r#"
        SELECT ?p ?o WHERE {{
            <{}> ?p ?o .
        }}"#,
        entity_iri
    );

    if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
        for row in bindings.flatten() {
            let p = row.get(0).and_then(|t| t.as_ref());
            let o = row.get(1).and_then(|t| t.as_ref());

            let p_str = p.map(|term| {
                if let Some(label) = pred_labels.get(&term.iri().unwrap().to_string()) {
                    return label.to_string();
                } else {
                    // Lexicalize the IRI's path or fragment as well as we can
                    return term.iri().map(|i| lexicalize_iri(&i)).unwrap();
                }
            });

            let o_str = o.map(|term| {
                match term {
                    t if t.is_iri() => {
                        if let Some(label) = obj_labels.get(&t.iri().unwrap().to_string()) {
                            return label.to_string();
                        } else {
                            // Lexicalize the IRI's path or fragment as well as we can
                            return t.iri().map(|i| lexicalize_iri(&i)).unwrap();
                        }
                    }
                    t if t.is_literal() => t.lexical_form().map(|s| s.to_string()).unwrap(),
                    _ => "".to_string(),
                }
            });

            if let (Some(lexicalized_p), Some(lexicalized_o)) = (p_str, o_str) {
                p_o_entries
                    .entry((p.unwrap().iri().unwrap().to_string(), lexicalized_p))
                    .or_insert_with(Vec::new)
                    .push(lexicalized_o);
            }
        }
    }
    p_o_entries
}

/// Given a list of entity IRIs and caches of predicate and object labels, create a lexicalized
/// representation of each entity (i.e., a Virtual Document / VDoc)
/// All predicates and objects will be represented as a string, either through their labels or
/// via a fallback `lexicalize_iri` function that assumes that there is some meaningful string
/// inside the IRIs
///
/// **Arguments**
/// * `entity_iris` - Vec of entity IRIs
/// * `pred_labels` - HashMap of Predicate IRI -> Predicate label
/// * `obj_labels` - HashMap of Object IRI -> Object label
///
/// **Output**
/// * A HashMap of Entity IRI -> Hashmap of (Predicate IRI, Predicate label) -> Vec of lexicalized objects
pub fn get_lexicalized_entities_p_o_entries_batch(
    entity_iris: &[String],
    pred_labels: &HashMap<String, String>,
    obj_labels: &HashMap<String, String>,
) -> HashMap<String, HashMap<(String, String), Vec<String>>> {
    let mut results: HashMap<String, HashMap<(String, String), Vec<String>>> = HashMap::new();

    let values_clause = entity_iris
        .iter()
        .map(|iri| format!("<{}>", iri))
        .collect::<Vec<_>>()
        .join(" ");

    let query = format!(
        r#"
        SELECT ?s ?p ?o WHERE {{
            VALUES ?s {{ {} }}
            ?s ?p ?o .
        }}"#,
        values_clause
    );

    if let Ok(SparqlResult::Bindings(bindings)) = SPARQLEndpoint::access().query(&*query) {
        for row in bindings.flatten() {
            let s = row.get(0).and_then(|t| t.as_ref());
            let p = row.get(1).and_then(|t| t.as_ref());
            let o = row.get(2).and_then(|t| t.as_ref());

            let s_iri = s.unwrap().iri().unwrap().to_string();

            let p_str = p.map(|term| {
                if let Some(label) = pred_labels.get(&term.iri().unwrap().to_string()) {
                    return label.to_string();
                } else {
                    // Lexicalize the IRI's path or fragment as well as we can
                    return term.iri().map(|i| lexicalize_iri(&i)).unwrap();
                }
            });

            let o_str = o.map(|term| {
                match term {
                    t if t.is_iri() => {
                        if let Some(label) = obj_labels.get(&t.iri().unwrap().to_string()) {
                            return label.to_string();
                        } else {
                            // Lexicalize the IRI's path or fragment as well as we can
                            return t.iri().map(|i| lexicalize_iri(&i)).unwrap();
                        }
                    }
                    t if t.is_literal() => t.lexical_form().map(|s| s.to_string()).unwrap(),
                    _ => "".to_string(),
                }
            });

            if let (Some(lexicalized_p), Some(lexicalized_o)) = (p_str, o_str) {
                results
                    .entry(s_iri)
                    .or_default()
                    .entry((p.unwrap().iri().unwrap().to_string(), lexicalized_p))
                    .or_default()
                    .push(lexicalized_o);
            }

        }
    }
    results
}

/// The field of an entity's lexicalization (VDoc)
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityField {
    /// Tuple of (Predicate IRI, exicalized predicate) -> Vec of lexicalized objects associated with it for the entity
    #[serde_as(as = "Vec<(_, _)>")] // Avoids explosions due to the tuple
    predicate_texts: HashMap<(String ,String), Vec<String>>,
}

// An entity's lexicalization (VDoc)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityRecord {
    pub entity_iri: String,
    pub fields: Vec<EntityField>,
}

/// Creates lexicalized entity documents (Virtual Documents, AKA VDocs) to index them in e.g. Lucene
///
/// **Arguments**
/// * `entity_iris` - A Vec of entity IRIs to index
/// * `clustered_predicates` - A Vec of clustered predicates obtained from `get_clusters`
/// * `output_path` - Where to write the lexicalized entities. This will be written as a list of
///                   EntityRecord objects in .jsonl format
///
/// **Outputs**
/// * A HashMap of Entity IRI -> Vec of lexicalized entity fields
pub fn create_entity_representations_from_entity_iris_list(
    entity_iris: Vec<String>,
    clustered_predicates: Vec<PredicatesCluster>,
    output_path: &str,
) -> std::io::Result<()> {
    let entity_iris = &entity_iris;
    log::info!("Indexing {:?} entities", entity_iris.len());

    let mut pred_iri_to_field_id: HashMap<String, usize> = HashMap::new();
    for (idx, cluster) in clustered_predicates.iter().enumerate() {
        for iri in &cluster.predicate_iris {
            pred_iri_to_field_id.insert(iri.to_string(), idx);
        }
    }

    let pred_labels = fetch_all_pred_labels().unwrap();
    let obj_labels = fetch_all_obj_labels().unwrap();

    //let file = File::create(output_path)?;
    //let writer = Mutex::new(BufWriter::new(file));

    let pb = ProgressBar::new(entity_iris.len() as u64);
    pb.set_style(
        ProgressStyle::default_spinner()
            .template(
                "{msg}: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} (ETA: {eta})",
            )
            .unwrap()
            .tick_chars("#>-"),
    );
    pb.set_message("Querying and indexing entities");

    let (tx, rx) = channel();

    let output_path = output_path.to_string();
    let writer_thread = thread::spawn(move || {
        let file = File::create(output_path).unwrap();
        let mut w = BufWriter::new(file);
        for line in rx {
            writeln!(w, "{}", line).unwrap();
        }
        w.flush().unwrap();
    });

    // I was expecting it to make Qlever explode, but turns out it works like a charm
    //let entity_details_workers: usize = thread::available_parallelism().unwrap().get() / 2;
    let pool = rayon::ThreadPoolBuilder::new().num_threads(ENTITY_DETAILS_WORKERS).build().unwrap();

    pool.install(|| {
        entity_iris.par_chunks(ENTITY_DETAILS_QUERY_CHUNK_SIZE).for_each_with(tx, |tx, chunk| {
            let batch_results = get_lexicalized_entities_p_o_entries_batch(
                chunk,
                &pred_labels,
                &obj_labels
            );

            for entity_iri in chunk {
                let mut entity_fields = vec![
                    EntityField { predicate_texts: HashMap::new() };
                    clustered_predicates.len()
                ];

                for ((p_iri, p_str), o_strs) in batch_results.get(entity_iri).unwrap().iter() {
                    if let Some(&field_idx) = pred_iri_to_field_id.get(p_iri) {
                        entity_fields[field_idx]
                            .predicate_texts
                            .insert((p_iri.clone(), p_str.clone()), o_strs.clone());
                    }
                }

                let record = EntityRecord {
                    entity_iri: entity_iri.to_string(),
                    fields: entity_fields,
                };

                if let Ok(json_line) = serde_json::to_string(&record) {
                    tx.send(json_line).unwrap();
                }
            }
            pb.inc(chunk.len() as u64);
        })
    });

    writer_thread.join().unwrap();
    Ok(())
}

/// Creates lexicalized entity documents (Virtual Documents, AKA VDocs) to index them in e.g. Lucene
///
/// **Arguments**
/// * `input_path` - A .jsonl formatted file with already lexicalized EntityRecord objects.
///                  It does not matter if they have multiple fields (they will be reshuffled according
///                  to `clustered_predicates`), but a single field is recommended to avoid constant
///                  lookups
/// * `clustered_predicates` - A Vec of clustered predicates obtained from `get_clusters`
/// * `output_path` - Where to write the lexicalized entities. This will be written as a list of
///                   EntityRecord objects in .jsonl format
///
/// **Outputs**
/// * A HashMap of Entity IRI -> Vec of lexicalized entity fields
pub fn create_entity_representations_from_extracted_entities(
    input_path: &str,
    clustered_predicates: Vec<PredicatesCluster>,
    output_path: &str,
) -> std::io::Result<()> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let mut json_reader = JsonLinesReader::new(reader);

    let mut pred_iri_to_field_id: HashMap<String, usize> = HashMap::new();
    for (idx, cluster) in clustered_predicates.iter().enumerate() {
        for iri in &cluster.predicate_iris {
            pred_iri_to_field_id.insert(iri.to_string(), idx);
        }
    }

    let file = File::create(output_path)?;
    let writer = Mutex::new(BufWriter::new(file));

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template(
                "{msg}: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos} ({per_sec})",
            )
            .unwrap()
            .tick_chars("#>-"),
    );
    pb.set_message("Indexing entities");

    let (tx, rx) = channel();

    let output_path = output_path.to_string();
    let writer_thread = thread::spawn(move || {
        let file = File::create(output_path).unwrap();
        let mut w = BufWriter::new(file);
        for line in rx {
            writeln!(w, "{}", line).unwrap();
        }
        w.flush().unwrap();
    });

    json_reader
        .read_all::<EntityRecord>()
        .par_bridge()
        .for_each(|entity_record| {
            let entity_record = entity_record.unwrap();

            let mut entity_fields = vec![
                EntityField {
                    predicate_texts: HashMap::new()
                };
                clustered_predicates.len()
            ];

            for field in &entity_record.fields {
                for ((p_iri, p_str), o_strs) in &field.predicate_texts {
                    if let Some(&field_idx) = pred_iri_to_field_id.get(p_iri) {
                        entity_fields[field_idx]
                            .predicate_texts
                            .insert((p_iri.clone(), p_str.clone()), o_strs.clone());
                    }
                }
            }

            let record = EntityRecord {
                entity_iri: entity_record.entity_iri.clone(),
                fields: entity_fields,
            };


            let json_line = serde_json::to_string(&record).unwrap();
            if let Ok(json_line) = serde_json::to_string(&record) {
                tx.send(json_line).unwrap();
            }

            pb.inc(1);
        });

    writer_thread.join().unwrap();
    Ok(())
}

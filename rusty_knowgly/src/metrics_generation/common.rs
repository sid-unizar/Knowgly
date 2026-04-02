#![allow(unused)]

//! Common queries used by the metrics generators
//!

use crate::metrics_generation::clustering::PredicatesCluster;
use crate::qlever_client::SPARQLEndpoint;
use ckmeans::ckmeans_indices;
use serde::Serialize;
use sophia::api::sparql::{SparqlDataset, SparqlResult};
use sophia::api::term::Term;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;

/// Filtering function used to indicate if the type IRI belongs to DBpedia's namespace or not
///  Used in fetch_all_type_iris
///
/// *Arguments**
/// * `type_iri` - Type IRI as a str
///
///
/// **Output**
/// * true if valid, false if not
pub fn dbpedia_namespace_filter(iri: &str) -> bool {
    iri.starts_with("http://dbpedia.org/")
}

/// Fetch all unique type IRIs in the KG
///
/// *Arguments**
/// * `type_iri_filter_fn` - An optional filtering function for the type IRIs
///
///
/// **Output**
/// * A Vec of Type IRIs
pub fn fetch_all_type_iris<F>(
    type_iri_filter_fn: Option<F>,
) -> Result<Vec<String>, Box<dyn std::error::Error>>
where
    F: Fn(&str) -> bool,
{
    log::info!("Fetching all type iris with custom filter");

    let query = "SELECT DISTINCT ?type WHERE { ?s a ?type. }";
    let mut type_iris = Vec::new();

    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        for row in bindings {
            let r = row?;
            if let Some(type_iri) = r.get(0).and_then(|node| node.as_ref()?.iri()) {
                let iri_str = type_iri.to_string();

                let keep = match &type_iri_filter_fn {
                    Some(f) => f(&iri_str),
                    None => true,
                };

                if keep {
                    type_iris.push(iri_str);
                }
            }
        }
    }

    Ok(type_iris)
}

/// Fetch all unique type IRIs in the KG
///
/// **Output**
/// * A Vec of Type IRIs
pub fn fetch_all_property_iris() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    log::info!("Fetching all property iris");

    let query = "SELECT DISTINCT ?p WHERE { ?s ?p ?o. }";
    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        let iris = bindings
            .filter_map(|row| {
                let r = row.ok()?;
                let p_iri = r.get(0)?.as_ref()?;
                Some(p_iri.iri()?.to_string())
            })
            .collect();
        Ok(iris)
    } else {
        Ok(vec![])
    }
}

/// Fetch all unique types and the properties found associated with them (via the entities in the KG),
///
/// *Arguments**
/// * `type_iri_filter_fn` - An optional filtering function for the type IRIs
///
/// **Output**
/// * A Hashmap of Type IRI -> Vec of Predicate IRIss
pub fn fetch_all_properties_for_each_type<F>(
    type_iri_filter_fn: Option<F>,
) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>>
where
    F: Fn(&str) -> bool,
{
    log::info!("Fetching all property-type pairs");

    let query = "SELECT DISTINCT ?p ?type WHERE { ?s a ?type. ?s ?p ?o. }";

    if let SparqlResult::Bindings(bindings) = SPARQLEndpoint::access().query(query)? {
        let mut type_to_props: HashMap<String, Vec<String>> = HashMap::new();

        bindings
            .filter_map(|row| {
                let r = row.ok()?;
                let p_iri = r.get(0)?.as_ref()?.iri()?.to_string();
                let type_iri = r.get(1)?.as_ref()?.iri()?.to_string();

                match &type_iri_filter_fn {
                    Some(f) if !f(&type_iri) => None,
                    _ => Some((type_iri, p_iri)),
                }
            })
            .for_each(|(type_iri, p_iri)| {
                type_to_props.entry(type_iri).or_default().push(p_iri);
            });

        Ok(type_to_props)
    } else {
        Ok(HashMap::new())
    }
}

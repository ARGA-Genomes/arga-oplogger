use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, NameField};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize, Hash, Eq, PartialEq)]
pub struct Name {
    pub entity_id: String,
    pub canonical_name: String,
    pub scientific_name: String,
    pub scientific_name_authorship: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Name>, Error> {
    use rdf::Name::*;

    let iris = dataset.scope(&["names"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);


    info!("Resolving data");
    let data: HashMap<Literal, Vec<NameField>> =
        resolve_data(&graph, &[EntityId, CanonicalName, ScientificName, ScientificNameAuthorship])?;


    let mut names = Vec::new();

    for (_idx, fields) in data {
        let mut name = Name::default();

        for field in fields {
            match field {
                NameField::EntityId(val) => name.entity_id = val,
                NameField::CanonicalName(val) => name.canonical_name = val,
                NameField::ScientificName(val) => name.scientific_name = val,
                NameField::ScientificNameAuthorship(val) => name.scientific_name_authorship = Some(val),
            }
        }

        names.push(name);
    }

    names.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
    names.dedup();

    Ok(names)
}

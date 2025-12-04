use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, DepositionField, Literal};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct Deposition {
    pub entity_id: String,
    pub assembly_id: Option<String>,

    pub event_date: Option<String>,
    pub url: Option<String>,
    pub institution: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Deposition>, Error> {
    use rdf::Deposition::*;

    let models = dataset.scope(&["deposition"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);

    info!("Resolving data");
    let data: HashMap<Literal, Vec<DepositionField>> =
        resolver.resolve(&[EntityId, AssemblyId, EventDate, Url, Institution], &scope)?;


    let mut depositions = Vec::new();

    for (_idx, fields) in data {
        let mut deposition = Deposition::default();

        for field in fields {
            match field {
                DepositionField::EntityId(val) => deposition.entity_id = val,
                DepositionField::AssemblyId(val) => deposition.assembly_id = Some(val),
                DepositionField::EventDate(val) => deposition.event_date = Some(val),
                DepositionField::Url(val) => deposition.url = Some(val),
                DepositionField::Institution(val) => deposition.institution = Some(val),
            }
        }

        depositions.push(deposition);
    }

    Ok(depositions)
}

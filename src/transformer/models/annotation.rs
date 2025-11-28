use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, AnnotationField, Literal};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct Annotation {
    pub entity_id: String,
    pub assembly_id: Option<String>,

    pub name: Option<String>,
    pub provider: Option<String>,
    pub event_date: Option<String>,
    pub number_of_genes: Option<String>,
    pub number_of_proteins: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Annotation>, Error> {
    use rdf::Annotation::*;

    let models = dataset.scope(&["annotation"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);

    info!("Resolving data");
    let data: HashMap<Literal, Vec<AnnotationField>> = resolver.resolve(
        &[
            EntityId,
            AssemblyId,
            EventDate,
            Name,
            Provider,
            EventDate,
            NumberOfGenes,
            NumberOfProteins,
        ],
        &scope,
    )?;


    let mut annotations = Vec::new();

    for (_idx, fields) in data {
        let mut annotation = Annotation::default();

        for field in fields {
            match field {
                AnnotationField::EntityId(val) => annotation.entity_id = val,
                AnnotationField::AssemblyId(val) => annotation.assembly_id = Some(val),
                AnnotationField::Name(val) => annotation.name = Some(val),
                AnnotationField::Provider(val) => annotation.provider = Some(val),
                AnnotationField::EventDate(val) => annotation.event_date = Some(val),
                AnnotationField::NumberOfGenes(val) => annotation.number_of_genes = Some(val),
                AnnotationField::NumberOfProteins(val) => annotation.number_of_proteins = Some(val),
            }
        }

        annotations.push(annotation);
    }

    Ok(annotations)
}

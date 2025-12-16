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
    pub method: Option<String>,
    pub r#type: Option<String>,
    pub version: Option<String>,
    pub software: Option<String>,
    pub software_version: Option<String>,
    pub event_date: Option<String>,

    pub number_of_genes: Option<u64>,
    pub number_of_coding_proteins: Option<u64>,
    pub number_of_non_coding_proteins: Option<u64>,
    pub number_of_pseudogenes: Option<u64>,
    pub number_of_other_genes: Option<u64>,
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
            Method,
            Type,
            Version,
            Software,
            SoftwareVersion,
            EventDate,
            NumberOfGenes,
            NumberOfCodingProteins,
            NumberOfNonCodingProteins,
            NumberOfPseudogenes,
            NumberOfOtherGenes,
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
                AnnotationField::Method(val) => annotation.method = Some(val),
                AnnotationField::Type(val) => annotation.r#type = Some(val),
                AnnotationField::Version(val) => annotation.version = Some(val),
                AnnotationField::Software(val) => annotation.software = Some(val),
                AnnotationField::SoftwareVersion(val) => annotation.software_version = Some(val),
                AnnotationField::EventDate(val) => annotation.event_date = Some(val),
                AnnotationField::NumberOfGenes(val) => annotation.number_of_genes = Some(val),
                AnnotationField::NumberOfCodingProteins(val) => annotation.number_of_coding_proteins = Some(val),
                AnnotationField::NumberOfNonCodingProteins(val) => annotation.number_of_non_coding_proteins = Some(val),
                AnnotationField::NumberOfPseudogenes(val) => annotation.number_of_pseudogenes = Some(val),
                AnnotationField::NumberOfOtherGenes(val) => annotation.number_of_other_genes = Some(val),
            }
        }

        annotations.push(annotation);
    }

    Ok(annotations)
}

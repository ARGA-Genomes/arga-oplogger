use std::collections::HashMap;

use tracing::instrument;

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, SubsampleField};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct Subsample {
    pub entity_id: String,
    pub specimen_id: Option<String>,
    pub material_sample_id: Option<String>,
    pub tissue_id: Option<String>,
    pub subsample_id: Option<String>,

    pub scientific_name: Option<String>,
    pub sample_type: Option<String>,
    pub institution: Option<String>,
    pub institution_code: Option<String>,
    pub name: Option<String>,
    pub custodian: Option<String>,
    pub description: Option<String>,
    pub notes: Option<String>,
    pub culture_method: Option<String>,
    pub culture_media: Option<String>,
    pub weight_or_volume: Option<String>,
    pub preservation_method: Option<String>,
    pub preservation_temperature: Option<String>,
    pub preservation_duration: Option<String>,
    pub quality: Option<String>,
    pub cell_type: Option<String>,
    pub cell_line: Option<String>,
    pub clone_name: Option<String>,
    pub lab_host: Option<String>,
    pub sample_processing: Option<String>,
    pub sample_pooling: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Subsample>, Error> {
    use rdf::Subsample::*;

    let models = dataset.scope(&["subsamples"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let data: HashMap<Literal, Vec<SubsampleField>> = resolver.resolve(
        &[
            EntityId,
            SpecimenId,
            MaterialSampleId,
            TissueId,
            SubsampleId,
            SampleType,
            Institution,
            InstitutionCode,
            Name,
            Custodian,
            Description,
            Notes,
            CultureMethod,
            CultureMedia,
            WeightOrVolume,
            PreservationMethod,
            PreservationTemperature,
            PreservationDuration,
            Quality,
            CellType,
            CellLine,
            CloneName,
            LabHost,
            SampleProcessing,
            SamplePooling,
        ],
        &scope,
    )?;


    let mut subsamples = Vec::new();

    for (_idx, fields) in data {
        let mut subsample = Subsample::default();

        for field in fields {
            match field {
                SubsampleField::EntityId(val) => subsample.entity_id = val,
                SubsampleField::SpecimenId(val) => subsample.specimen_id = Some(val),
                SubsampleField::MaterialSampleId(val) => subsample.material_sample_id = Some(val),
                SubsampleField::TissueId(val) => subsample.tissue_id = Some(val),
                SubsampleField::SubsampleId(val) => subsample.subsample_id = Some(val),
                SubsampleField::SampleType(val) => subsample.sample_type = Some(val),
                SubsampleField::Institution(val) => subsample.institution = Some(val),
                SubsampleField::InstitutionCode(val) => subsample.institution_code = Some(val),
                SubsampleField::Name(val) => subsample.name = Some(val),
                SubsampleField::Custodian(val) => subsample.custodian = Some(val),
                SubsampleField::Description(val) => subsample.description = Some(val),
                SubsampleField::Notes(val) => subsample.notes = Some(val),
                SubsampleField::CultureMethod(val) => subsample.culture_method = Some(val),
                SubsampleField::CultureMedia(val) => subsample.culture_media = Some(val),
                SubsampleField::WeightOrVolume(val) => subsample.weight_or_volume = Some(val),
                SubsampleField::PreservationMethod(val) => subsample.preservation_method = Some(val),
                SubsampleField::PreservationTemperature(val) => subsample.preservation_temperature = Some(val),
                SubsampleField::PreservationDuration(val) => subsample.preservation_duration = Some(val),
                SubsampleField::Quality(val) => subsample.quality = Some(val),
                SubsampleField::CellType(val) => subsample.cell_type = Some(val),
                SubsampleField::CellLine(val) => subsample.cell_line = Some(val),
                SubsampleField::CloneName(val) => subsample.clone_name = Some(val),
                SubsampleField::LabHost(val) => subsample.lab_host = Some(val),
                SubsampleField::SampleProcessing(val) => subsample.sample_processing = Some(val),
                SubsampleField::SamplePooling(val) => subsample.sample_pooling = Some(val),
            }
        }

        subsamples.push(subsample);
    }

    let names = get_scientific_names(dataset)?;
    for subsample in subsamples.iter_mut() {
        if let Some(scientific_name) = names.get(&subsample.entity_id) {
            subsample.scientific_name = Some(scientific_name.clone());
        }
    }

    Ok(subsamples)
}


/// Get scientific names from tissues.
///
/// This will go through all tissues and retrieve the name associated with the
/// original collection event.
#[instrument(skip_all)]
pub fn get_scientific_names(dataset: &Dataset) -> Result<HashMap<String, String>, Error> {
    let models = dataset.scope(&["subsamples"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let names = super::tissue::get_scientific_names(dataset)?;
    let mut subsamples = HashMap::new();

    let data: HashMap<Literal, Vec<SubsampleField>> =
        resolver.resolve(&[rdf::Subsample::EntityId, rdf::Subsample::TissueId], &scope)?;

    for (_idx, fields) in data.into_iter() {
        let mut entity_id = None;
        let mut tissue_id = None;

        for field in fields {
            match field {
                SubsampleField::EntityId(val) => entity_id = Some(val),
                SubsampleField::TissueId(val) => tissue_id = Some(val),
                _ => {}
            }
        }

        if let (Some(entity_id), Some(tissue_id)) = (entity_id, tissue_id) {
            if let Some(name) = names.get(&tissue_id) {
                subsamples.insert(entity_id, name.clone());
            }
        }
    }


    Ok(subsamples)
}

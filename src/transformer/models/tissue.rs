use std::collections::HashMap;

use tracing::instrument;

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, TissueField};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize)]
pub struct Tissue {
    pub entity_id: String,
    pub organism_id: Option<String>,
    pub tissue_id: Option<String>,
    pub material_sample_id: Option<String>,

    pub scientific_name: Option<String>,
    pub original_catalogue_name: Option<String>,
    pub current_catalogue_name: Option<String>,
    pub identification_verified: Option<String>,
    pub reference_material: Option<String>,
    pub registered_by: Option<String>,
    pub registration_date: Option<String>,
    pub custodian: Option<String>,
    pub institution: Option<String>,
    pub institution_code: Option<String>,
    pub collection: Option<String>,
    pub collection_code: Option<String>,
    pub status: Option<String>,
    pub current_status: Option<String>,
    pub sampling_protocol: Option<String>,
    pub tissue_type: Option<String>,
    pub disposition: Option<String>,
    pub fixation: Option<String>,
    pub storage: Option<String>,
    pub source: Option<String>,
    pub source_url: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Tissue>, Error> {
    use rdf::Tissue::*;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/tissues",
    ];
    let graph = dataset.graph(&graphs);

    let data: HashMap<Literal, Vec<TissueField>> = resolve_data(
        &graph,
        &[
            OrganismId,
            TissueId,
            MaterialSampleId,
            OriginalCatalogueName,
            CurrentCatalogueName,
            IdentificationVerified,
            ReferenceMaterial,
            RegisteredBy,
            RegistrationDate,
            Custodian,
            Institution,
            InstitutionCode,
            Collection,
            CollectionCode,
            Status,
            CurrentStatus,
            SamplingProtocol,
            TissueType,
            Disposition,
            Fixation,
            Storage,
            Citation,
            SourceUrl,
        ],
    )?;


    let mut tissues = Vec::new();

    for (entity_id, fields) in data {
        let Literal::String(entity_id) = entity_id;

        let mut tissue = Tissue {
            entity_id,
            ..Default::default()
        };

        for field in fields {
            match field {
                TissueField::OrganismId(val) => tissue.organism_id = Some(val),
                TissueField::TissueId(val) => tissue.tissue_id = Some(val),
                TissueField::MaterialSampleId(val) => tissue.material_sample_id = Some(val),
                TissueField::OriginalCatalogueName(val) => tissue.original_catalogue_name = Some(val),
                TissueField::CurrentCatalogueName(val) => tissue.current_catalogue_name = Some(val),
                TissueField::IdentificationVerified(val) => tissue.identification_verified = Some(val),
                TissueField::ReferenceMaterial(val) => tissue.reference_material = Some(val),
                TissueField::RegisteredBy(val) => tissue.registered_by = Some(val),
                TissueField::RegistrationDate(val) => tissue.registration_date = Some(val),
                TissueField::Custodian(val) => tissue.custodian = Some(val),
                TissueField::Institution(val) => tissue.institution = Some(val),
                TissueField::InstitutionCode(val) => tissue.institution_code = Some(val),
                TissueField::Collection(val) => tissue.collection = Some(val),
                TissueField::CollectionCode(val) => tissue.collection_code = Some(val),
                TissueField::Status(val) => tissue.status = Some(val),
                TissueField::CurrentStatus(val) => tissue.current_status = Some(val),
                TissueField::SamplingProtocol(val) => tissue.sampling_protocol = Some(val),
                TissueField::TissueType(val) => tissue.tissue_type = Some(val),
                TissueField::Disposition(val) => tissue.disposition = Some(val),
                TissueField::Fixation(val) => tissue.fixation = Some(val),
                TissueField::Storage(val) => tissue.storage = Some(val),
                TissueField::Citation(val) => tissue.source = Some(val),
                TissueField::SourceUrl(val) => tissue.source_url = Some(val),
            }
        }

        tissues.push(tissue);
    }

    let names = get_scientific_names(dataset)?;
    for tissue in tissues.iter_mut() {
        if let Some(scientific_name) = names.get(&Literal::String(tissue.entity_id.clone())) {
            tissue.scientific_name = Some(scientific_name.clone());
        }
    }

    Ok(tissues)
}


/// Get scientific names associated with material samples.
///
/// This will go through all material samples and retrieve the name associated with the
/// original collection event.
#[instrument(skip_all)]
pub fn get_scientific_names(dataset: &Dataset) -> Result<HashMap<Literal, String>, Error> {
    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/tissues",
    ];
    let graph = dataset.graph(&graphs);

    let names = super::collecting::get_scientific_names(dataset)?;
    let mut collecting = HashMap::new();

    let data: HashMap<Literal, Vec<TissueField>> = resolve_data(&graph, &[rdf::Tissue::MaterialSampleId])?;
    for (entity_id, fields) in data.into_iter() {
        if let Some(TissueField::MaterialSampleId(material_sample_id)) = fields.into_iter().next() {
            if let Some(name) = names.get(&Literal::String(material_sample_id)) {
                collecting.insert(entity_id, name.clone());
            }
        }
    }

    Ok(collecting)
}

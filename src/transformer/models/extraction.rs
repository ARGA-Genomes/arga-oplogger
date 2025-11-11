use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, ExtractionField, Literal};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct Extraction {
    pub entity_id: String,
    pub subsample_id: Option<String>,
    pub publication_id: Option<String>,
    pub extract_id: Option<String>,
    pub extracted_by: Option<String>,
    pub material_extracted_by: Option<String>,
    pub scientific_name: Option<String>,
    pub extraction_date: Option<String>,
    pub nucleic_acid_type: Option<String>,
    pub nucleic_acid_conformation: Option<String>,
    pub nucleic_acid_preservation_method: Option<String>,
    pub nucleic_acid_concentration: Option<String>,
    pub nucleic_acid_quantification: Option<String>,
    pub concentration_unit: Option<String>,
    pub absorbance_260_230_ratio: Option<String>,
    pub absorbance_260_280_ratio: Option<String>,
    pub cell_lysis_method: Option<String>,
    pub action_extracted: Option<String>,
    pub extraction_method: Option<String>,
    pub number_of_extracts_pooled: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Extraction>, Error> {
    use rdf::Extraction::*;

    let models = dataset.scope(&["extractions"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    info!("Resolving data");
    let data: HashMap<Literal, Vec<ExtractionField>> = resolver.resolve(
        &[
            EntityId,
            SubsampleId,
            ExtractId,
            ExtractionDate,
            NucleicAcidType,
            NucleicAcidConformation,
            NucleicAcidPreservationMethod,
            NucleicAcidConcentration,
            NucleicAcidQuantification,
            // ConcentrationUnit,
            Absorbance260230Ratio,
            Absorbance260280Ratio,
            CellLysisMethod,
            ActionExtracted,
            ExtractionMethod,
            NumberOfExtractsPooled,
            ExtractedBy,
            ExtractedByOrcid,
            ExtractedByEntityId,
            MaterialExtractedBy,
            MaterialExtractedByOrcid,
            MaterialExtractedByEntityId,
            PublicationEntityId,
            Doi,
            Citation,
        ],
        &scope,
    )?;


    let mut extractions = Vec::new();

    for (_idx, fields) in data {
        let mut extraction = Extraction::default();

        for field in fields {
            match field {
                ExtractionField::EntityId(val) => extraction.entity_id = val,
                ExtractionField::SubsampleId(val) => extraction.subsample_id = Some(val),
                ExtractionField::ExtractId(val) => extraction.extract_id = Some(val),
                ExtractionField::ExtractionDate(val) => extraction.extraction_date = Some(val),
                ExtractionField::NucleicAcidType(val) => extraction.nucleic_acid_type = Some(val),
                ExtractionField::NucleicAcidConformation(val) => extraction.nucleic_acid_conformation = Some(val),
                ExtractionField::NucleicAcidPreservationMethod(val) => {
                    extraction.nucleic_acid_preservation_method = Some(val)
                }
                ExtractionField::NucleicAcidConcentration(val) => extraction.nucleic_acid_conformation = Some(val),
                ExtractionField::NucleicAcidQuantification(val) => extraction.nucleic_acid_quantification = Some(val),
                ExtractionField::ConcentrationUnit(val) => extraction.concentration_unit = Some(val),
                ExtractionField::Absorbance260230Ratio(val) => extraction.absorbance_260_230_ratio = Some(val),
                ExtractionField::Absorbance260280Ratio(val) => extraction.absorbance_260_280_ratio = Some(val),
                ExtractionField::CellLysisMethod(val) => extraction.cell_lysis_method = Some(val),
                ExtractionField::ActionExtracted(val) => extraction.action_extracted = Some(val),
                ExtractionField::ExtractionMethod(val) => extraction.extraction_method = Some(val),
                ExtractionField::NumberOfExtractsPooled(val) => extraction.number_of_extracts_pooled = Some(val),

                // only include the entity id for agents as they will be referenced instead
                ExtractionField::ExtractedByEntityId(val) => extraction.extracted_by = Some(val),
                ExtractionField::MaterialExtractedByEntityId(val) => extraction.material_extracted_by = Some(val),
                ExtractionField::PublicationEntityId(val) => extraction.publication_id = Some(val),

                // fields we don't need to action as it's used in the production of the reference entity id
                ExtractionField::ExtractedBy(_) => {}
                ExtractionField::ExtractedByOrcid(_) => {}
                ExtractionField::MaterialExtractedBy(_) => {}
                ExtractionField::MaterialExtractedByOrcid(_) => {}
                ExtractionField::Doi(_) => {}
                ExtractionField::Citation(_) => {}
            }
        }

        extractions.push(extraction);
    }


    let names = get_scientific_names(dataset)?;
    for extraction in extractions.iter_mut() {
        if let Some(scientific_name) = names.get(&extraction.entity_id) {
            extraction.scientific_name = Some(scientific_name.clone());
        }
    }

    Ok(extractions)
}


/// Get scientific names from subsamples.
///
/// This will go through all subsamples and retrieve the name associated with the
/// original collection event, going via tissues if necessary.
#[instrument(skip_all)]
pub fn get_scientific_names(dataset: &Dataset) -> Result<HashMap<String, String>, Error> {
    let models = dataset.scope(&["extractions"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let names = super::subsample::get_scientific_names(&dataset)?;
    let mut extractions = HashMap::new();

    let data: HashMap<Literal, Vec<ExtractionField>> =
        resolver.resolve(&[rdf::Extraction::EntityId, rdf::Extraction::SubsampleId], &scope)?;

    for (_idx, fields) in data.into_iter() {
        let mut entity_id = None;
        let mut subsample_id = None;

        for field in fields {
            match field {
                ExtractionField::EntityId(val) => entity_id = Some(val),
                ExtractionField::SubsampleId(val) => subsample_id = Some(val),
                _ => {}
            }
        }

        if let (Some(entity_id), Some(subsample_id)) = (entity_id, subsample_id) {
            if let Some(name) = names.get(&subsample_id) {
                extractions.insert(entity_id, name.clone());
            }
        }
    }

    Ok(extractions)
}

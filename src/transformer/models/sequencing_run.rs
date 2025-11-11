use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, SequencingRunField};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct SequencingRun {
    pub entity_id: String,
    pub library_id: Option<String>,
    pub sequence_id: Option<String>,
    pub scientific_name: Option<String>,
    pub event_date: Option<String>,
    pub facility: Option<String>,
    pub instrument_or_method: Option<String>,
    pub sra_run_accession: Option<String>,
    pub platform: Option<String>,
    pub dataset_file_format: Option<String>,
    pub kit_chemistry: Option<String>,
    pub flowcell_type: Option<String>,
    pub cell_movie_length: Option<String>,
    pub base_caller_model: Option<String>,
    pub fast5_compression: Option<String>,
    pub analysis_software: Option<String>,
    pub analysis_software_version: Option<String>,
    pub target_gene: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<SequencingRun>, Error> {
    use rdf::SequencingRun::*;

    let models = dataset.scope(&["sequencing_runs"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    info!("Resolving data");
    let data: HashMap<Literal, Vec<SequencingRunField>> = resolver.resolve(
        &[
            EntityId,
            LibraryId,
            SequenceId,
            Facility,
            EventDate,
            InstrumentOrMethod,
            SraRunAccession,
            Platform,
            DatasetFileFormat,
            KitChemistry,
            FlowcellType,
            CellMovieLength,
            BaseCallerModel,
            Fast5Compression,
            AnalysisSoftware,
            AnalysisSoftwareVersion,
            TargetGene,
        ],
        &scope,
    )?;


    let mut sequences = Vec::new();

    for (_idx, fields) in data {
        let mut sequencing_run = SequencingRun::default();

        for field in fields {
            match field {
                SequencingRunField::EntityId(val) => sequencing_run.entity_id = val,
                SequencingRunField::LibraryId(val) => sequencing_run.library_id = Some(val),
                SequencingRunField::SequenceId(val) => sequencing_run.sequence_id = Some(val),
                SequencingRunField::Facility(val) => sequencing_run.facility = Some(val),
                SequencingRunField::EventDate(val) => sequencing_run.event_date = Some(val),
                SequencingRunField::InstrumentOrMethod(val) => sequencing_run.instrument_or_method = Some(val),
                SequencingRunField::SraRunAccession(val) => sequencing_run.sra_run_accession = Some(val),
                SequencingRunField::Platform(val) => sequencing_run.platform = Some(val),
                SequencingRunField::DatasetFileFormat(val) => sequencing_run.dataset_file_format = Some(val),
                SequencingRunField::KitChemistry(val) => sequencing_run.kit_chemistry = Some(val),
                SequencingRunField::FlowcellType(val) => sequencing_run.flowcell_type = Some(val),
                SequencingRunField::CellMovieLength(val) => sequencing_run.cell_movie_length = Some(val),
                SequencingRunField::BaseCallerModel(val) => sequencing_run.base_caller_model = Some(val),
                SequencingRunField::Fast5Compression(val) => sequencing_run.fast5_compression = Some(val),
                SequencingRunField::AnalysisSoftware(val) => sequencing_run.analysis_software = Some(val),
                SequencingRunField::AnalysisSoftwareVersion(val) => {
                    sequencing_run.analysis_software_version = Some(val)
                }
                SequencingRunField::TargetGene(val) => sequencing_run.target_gene = Some(val),
            }
        }

        sequences.push(sequencing_run);
    }

    let names = get_scientific_names(dataset)?;
    for sequence in sequences.iter_mut() {
        if let Some(scientific_name) = names.get(&sequence.entity_id) {
            sequence.scientific_name = Some(scientific_name.clone());
        }
    }

    Ok(sequences)
}


/// Get scientific names associated with libraries.
///
/// This will go through all libraries and retrieve the name associated with it.
#[instrument(skip_all)]
pub fn get_scientific_names(dataset: &Dataset) -> Result<HashMap<String, String>, Error> {
    let models = dataset.scope(&["sequencing_runs"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let names = super::library::get_scientific_names(dataset)?;
    let mut sequences = HashMap::new();

    let data: HashMap<Literal, Vec<SequencingRunField>> =
        resolver.resolve(&[rdf::SequencingRun::EntityId, rdf::SequencingRun::LibraryId], &scope)?;

    for (_idx, fields) in data.into_iter() {
        let mut entity_id = None;
        let mut library_id = None;

        for field in fields {
            match field {
                SequencingRunField::EntityId(val) => entity_id = Some(val),
                SequencingRunField::LibraryId(val) => library_id = Some(val),
                _ => {}
            }
        }

        if let (Some(entity_id), Some(library_id)) = (entity_id, library_id) {
            if let Some(name) = names.get(&library_id) {
                sequences.insert(entity_id, name.clone());
            }
        }
    }

    Ok(sequences)
}

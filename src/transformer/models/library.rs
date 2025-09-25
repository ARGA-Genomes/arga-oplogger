use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, LibraryField, Literal};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize)]
pub struct Library {
    pub entity_id: String,
    pub extract_id: Option<String>,
    pub library_id: Option<String>,
    pub scientific_name: Option<String>,

    pub event_date: Option<String>,
    pub concentration: Option<String>,
    pub concentration_unit: Option<String>,
    pub pcr_cycles: Option<String>,
    pub layout: Option<String>,
    pub prepared_by: Option<String>,
    pub selection: Option<String>,
    pub bait_set_name: Option<String>,
    pub bait_set_reference: Option<String>,
    pub construction_protocol: Option<String>,
    pub source: Option<String>,
    pub insert_size: Option<String>,
    pub design_description: Option<String>,
    pub strategy: Option<String>,
    pub index_tag: Option<String>,
    pub index_dual_tag: Option<String>,
    pub index_oligo: Option<String>,
    pub index_dual_oligo: Option<String>,
    pub location: Option<String>,
    pub remarks: Option<String>,
    pub dna_treatment: Option<String>,
    pub number_of_libraries_pooled: Option<String>,
    pub pcr_replicates: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Library>, Error> {
    use rdf::Library::*;

    let iris = dataset.scope(&["library"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);


    info!("Resolving data");
    let data: HashMap<Literal, Vec<LibraryField>> = resolve_data(
        &graph,
        &[
            EntityId,
            ExtractId,
            LibraryId,
            ScientificName,
            EventDate,
            Concentration,
            // ConcentrationUnit,
            PcrCycles,
            Layout,
            PreparedBy,
            Selection,
            BaitSetName,
            BaitSetReference,
            ConstructionProtocol,
            Source,
            InsertSize,
            DesignDescription,
            Strategy,
            IndexTag,
            IndexDualTag,
            IndexOligo,
            IndexDualOligo,
            Location,
            Remarks,
            DnaTreatment,
            NumberOfLibrariesPooled,
            PcrReplicates,
            CanonicalName,
            ScientificNameAuthorship,
            PreparedByEntityId,
        ],
    )?;


    let mut libraries = Vec::new();

    for (_idx, fields) in data {
        let mut library = Library::default();

        for field in fields {
            match field {
                LibraryField::EntityId(val) => library.entity_id = val,
                LibraryField::ExtractId(val) => library.extract_id = Some(val),
                LibraryField::LibraryId(val) => library.library_id = Some(val),
                LibraryField::ScientificName(val) => library.scientific_name = Some(val),
                LibraryField::EventDate(val) => library.event_date = Some(val),
                LibraryField::Concentration(val) => library.concentration = Some(val),
                LibraryField::ConcentrationUnit(val) => library.concentration_unit = Some(val),
                LibraryField::PcrCycles(val) => library.pcr_cycles = Some(val),
                LibraryField::Layout(val) => library.layout = Some(val),
                LibraryField::PreparedByEntityId(val) => library.prepared_by = Some(val),
                LibraryField::Selection(val) => library.selection = Some(val),
                LibraryField::BaitSetName(val) => library.bait_set_name = Some(val),
                LibraryField::BaitSetReference(val) => library.bait_set_reference = Some(val),
                LibraryField::ConstructionProtocol(val) => library.construction_protocol = Some(val),
                LibraryField::Source(val) => library.source = Some(val),
                LibraryField::InsertSize(val) => library.insert_size = Some(val),
                LibraryField::DesignDescription(val) => library.design_description = Some(val),
                LibraryField::Strategy(val) => library.strategy = Some(val),
                LibraryField::IndexTag(val) => library.index_tag = Some(val),
                LibraryField::IndexDualTag(val) => library.index_dual_tag = Some(val),
                LibraryField::IndexOligo(val) => library.index_oligo = Some(val),
                LibraryField::IndexDualOligo(val) => library.index_dual_oligo = Some(val),
                LibraryField::Location(val) => library.location = Some(val),
                LibraryField::Remarks(val) => library.remarks = Some(val),
                LibraryField::DnaTreatment(val) => library.dna_treatment = Some(val),
                LibraryField::NumberOfLibrariesPooled(val) => library.number_of_libraries_pooled = Some(val),
                LibraryField::PcrReplicates(val) => library.pcr_replicates = Some(val),

                LibraryField::PreparedBy(_) => {}
                LibraryField::CanonicalName(_) => {}
                LibraryField::ScientificNameAuthorship(_) => {}
            }
        }

        libraries.push(library);
    }

    Ok(libraries)
}

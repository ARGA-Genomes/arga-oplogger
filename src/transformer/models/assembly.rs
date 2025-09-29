use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, AssemblyField, Literal};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize)]
pub struct Assembly {
    pub entity_id: String,
    pub library_id: Option<String>,
    pub assembly_id: Option<String>,
    pub scientific_name: Option<String>,
    pub event_date: Option<String>,
    pub name: Option<String>,
    pub r#type: Option<String>,
    pub method: Option<String>,
    pub method_version: Option<String>,
    pub method_link: Option<String>,
    pub size: Option<String>,
    pub minimum_gap_length: Option<String>,
    pub completeness: Option<String>,
    pub completeness_method: Option<String>,
    pub source_molecule: Option<String>,
    pub reference_genome_used: Option<String>,
    pub reference_genome_link: Option<String>,
    pub number_of_scaffolds: Option<String>,
    pub genome_coverage: Option<String>,
    pub hybrid: Option<String>,
    pub hybrid_information: Option<String>,
    pub polishing_or_scaffolding_method: Option<String>,
    pub polishing_or_scaffolding_data: Option<String>,
    pub computational_infrastructure: Option<String>,
    pub system_used: Option<String>,
    pub assembly_n50: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Assembly>, Error> {
    use rdf::Assembly::*;

    let iris = dataset.scope(&["assembly"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);


    info!("Resolving data");
    let data: HashMap<Literal, Vec<AssemblyField>> = resolve_data(
        &graph,
        &[
            EntityId,
            LibraryId,
            AssemblyId,
            ScientificName,
            EventDate,
            Name,
            Type,
            Method,
            MethodVersion,
            MethodLink,
            Size,
            MinimumGapLength,
            Completeness,
            CompletenessMethod,
            SourceMolecule,
            ReferenceGenomeUsed,
            ReferenceGenomeLink,
            NumberOfScaffolds,
            GenomeCoverage,
            Hybrid,
            HybridInformation,
            PolishingOrScaffoldingMethod,
            PolishingOrScaffoldingData,
            ComputationalInfrastructure,
            SystemUsed,
            AssemblyN50,
            CanonicalName,
            ScientificNameAuthorship,
        ],
    )?;


    let mut assemblies = Vec::new();

    for (_idx, fields) in data {
        let mut assembly = Assembly::default();

        for field in fields {
            match field {
                AssemblyField::EntityId(val) => assembly.entity_id = val,
                AssemblyField::LibraryId(val) => assembly.library_id = Some(val),
                AssemblyField::AssemblyId(val) => assembly.assembly_id = Some(val),
                AssemblyField::ScientificName(val) => assembly.scientific_name = Some(val),
                AssemblyField::EventDate(val) => assembly.event_date = Some(val),
                AssemblyField::Name(val) => assembly.name = Some(val),
                AssemblyField::Type(val) => assembly.r#type = Some(val),
                AssemblyField::Method(val) => assembly.method = Some(val),
                AssemblyField::MethodVersion(val) => assembly.method_version = Some(val),
                AssemblyField::MethodLink(val) => assembly.method_link = Some(val),
                AssemblyField::Size(val) => assembly.size = Some(val),
                AssemblyField::MinimumGapLength(val) => assembly.minimum_gap_length = Some(val),
                AssemblyField::Completeness(val) => assembly.completeness = Some(val),
                AssemblyField::CompletenessMethod(val) => assembly.completeness_method = Some(val),
                AssemblyField::SourceMolecule(val) => assembly.source_molecule = Some(val),
                AssemblyField::ReferenceGenomeUsed(val) => assembly.reference_genome_used = Some(val),
                AssemblyField::ReferenceGenomeLink(val) => assembly.reference_genome_link = Some(val),
                AssemblyField::NumberOfScaffolds(val) => assembly.number_of_scaffolds = Some(val),
                AssemblyField::GenomeCoverage(val) => assembly.genome_coverage = Some(val),
                AssemblyField::Hybrid(val) => assembly.hybrid = Some(val),
                AssemblyField::HybridInformation(val) => assembly.hybrid_information = Some(val),
                AssemblyField::PolishingOrScaffoldingMethod(val) => {
                    assembly.polishing_or_scaffolding_method = Some(val)
                }
                AssemblyField::PolishingOrScaffoldingData(val) => assembly.polishing_or_scaffolding_data = Some(val),
                AssemblyField::ComputationalInfrastructure(val) => assembly.computational_infrastructure = Some(val),
                AssemblyField::SystemUsed(val) => assembly.system_used = Some(val),
                AssemblyField::AssemblyN50(val) => assembly.assembly_n50 = Some(val),

                AssemblyField::CanonicalName(_) => {}
                AssemblyField::ScientificNameAuthorship(_) => {}
            }
        }

        assemblies.push(assembly);
    }

    Ok(assemblies)
}

use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, AssemblyField, Literal};
use crate::transformer::resolver::Resolver;


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
    pub size: Option<u64>,
    pub size_ungapped: Option<u64>,
    pub minimum_gap_length: Option<String>,
    pub completeness: Option<String>,
    pub completeness_method: Option<String>,
    pub source_molecule: Option<String>,
    pub reference_genome_used: Option<String>,
    pub reference_genome_link: Option<String>,
    pub number_of_scaffolds: Option<u64>,
    pub number_of_contigs: Option<u64>,
    pub number_of_chromosomes: Option<u64>,
    pub number_of_component_sequences: Option<u64>,
    pub number_of_organelles: Option<u64>,
    pub number_of_gaps_between_scaffolds: Option<u64>,
    pub number_of_atgc: Option<u64>,
    pub number_of_guanine_cytosine: Option<u64>,
    pub guanine_cytosine_percent: Option<u64>,
    pub genome_coverage: Option<String>,
    pub hybrid: Option<String>,
    pub hybrid_information: Option<String>,
    pub polishing_or_scaffolding_method: Option<String>,
    pub polishing_or_scaffolding_data: Option<String>,
    pub computational_infrastructure: Option<String>,
    pub system_used: Option<String>,
    pub assembly_n50: Option<String>,
    pub contig_n50: Option<u64>,
    pub contig_l50: Option<u64>,
    pub scaffold_n50: Option<u64>,
    pub scaffold_l50: Option<u64>,
    pub longest_contig: Option<u64>,
    pub longest_scaffold: Option<u64>,
    pub total_contig_size: Option<u64>,
    pub total_scaffold_size: Option<u64>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Assembly>, Error> {
    use rdf::Assembly::*;

    let models = dataset.scope(&["assembly"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);

    info!("Resolving data");
    let data: HashMap<Literal, Vec<AssemblyField>> = resolver.resolve(
        &[
            EntityId,
            LibraryId,
            AssemblyId,
            ScientificName,
            TaxonId,
            EventDate,
            Name,
            Type,
            Method,
            MethodVersion,
            MethodLink,
            Size,
            SizeUngapped,
            MinimumGapLength,
            Completeness,
            CompletenessMethod,
            SourceMolecule,
            ReferenceGenomeUsed,
            ReferenceGenomeLink,
            NumberOfScaffolds,
            NumberOfContigs,
            NumberOfChromosomes,
            NumberOfComponentSequences,
            NumberOfOrganelles,
            NumberOfGapsBetweenScaffolds,
            NumberOfATGC,
            NumberOfGuanineCytosine,
            GuanineCytosinePercent,
            GenomeCoverage,
            Hybrid,
            HybridInformation,
            PolishingOrScaffoldingMethod,
            PolishingOrScaffoldingData,
            ComputationalInfrastructure,
            SystemUsed,
            AssemblyN50,
            ContigN50,
            ContigL50,
            ScaffoldN50,
            ScaffoldL50,
            LongestContig,
            LongestScaffold,
            TotalContigSize,
            TotalScaffoldSize,
            CanonicalName,
            ScientificNameAuthorship,
        ],
        &scope,
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
                AssemblyField::SizeUngapped(val) => assembly.size_ungapped = Some(val),
                AssemblyField::MinimumGapLength(val) => assembly.minimum_gap_length = Some(val),
                AssemblyField::Completeness(val) => assembly.completeness = Some(val),
                AssemblyField::CompletenessMethod(val) => assembly.completeness_method = Some(val),
                AssemblyField::SourceMolecule(val) => assembly.source_molecule = Some(val),
                AssemblyField::ReferenceGenomeUsed(val) => assembly.reference_genome_used = Some(val),
                AssemblyField::ReferenceGenomeLink(val) => assembly.reference_genome_link = Some(val),
                AssemblyField::NumberOfScaffolds(val) => assembly.number_of_scaffolds = Some(val),
                AssemblyField::NumberOfContigs(val) => assembly.number_of_contigs = Some(val),
                AssemblyField::NumberOfChromosomes(val) => assembly.number_of_chromosomes = Some(val),
                AssemblyField::NumberOfComponentSequences(val) => assembly.number_of_component_sequences = Some(val),
                AssemblyField::NumberOfOrganelles(val) => assembly.number_of_organelles = Some(val),
                AssemblyField::NumberOfGapsBetweenScaffolds(val) => {
                    assembly.number_of_gaps_between_scaffolds = Some(val)
                }
                AssemblyField::NumberOfATGC(val) => assembly.number_of_atgc = Some(val),
                AssemblyField::NumberOfGuanineCytosine(val) => assembly.number_of_guanine_cytosine = Some(val),
                AssemblyField::GuanineCytosinePercent(val) => assembly.guanine_cytosine_percent = Some(val),
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
                AssemblyField::ContigN50(val) => assembly.contig_n50 = Some(val),
                AssemblyField::ContigL50(val) => assembly.contig_l50 = Some(val),
                AssemblyField::ScaffoldN50(val) => assembly.scaffold_n50 = Some(val),
                AssemblyField::ScaffoldL50(val) => assembly.scaffold_l50 = Some(val),

                AssemblyField::LongestContig(val) => assembly.longest_contig = Some(val),
                AssemblyField::LongestScaffold(val) => assembly.longest_scaffold = Some(val),
                AssemblyField::TotalContigSize(val) => assembly.total_contig_size = Some(val),
                AssemblyField::TotalScaffoldSize(val) => assembly.total_scaffold_size = Some(val),

                AssemblyField::CanonicalName(_) => {}
                AssemblyField::ScientificNameAuthorship(_) => {}
                AssemblyField::TaxonId(_) => {}
            }
        }

        assemblies.push(assembly);
    }

    Ok(assemblies)
}

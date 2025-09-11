pub mod dataset;
pub mod mapped;
pub mod rdf;


use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use dataset::Dataset;
use iref::IriBuf;
use mapped::{Literal, Value};
use rdf::{CollectingField, ExtractionField, OrganismField, SubsampleField, TissueField};
use sophia::api::prelude::*;
use sophia::api::term::SimpleTerm;
use tracing::info;

use crate::errors::{Error, ParseError, TransformError};
use crate::loggers::ProgressStream;
use crate::readers::meta::Meta;


pub mod prefix {
    pub const NAMES: &'static str = "http://arga.org.au/schemas/fields/";
    pub const MAPPING: &'static str = "http://arga.org.au/schemas/mapping/";
    pub const WORMS: &'static str = "http://arga.org.au/schemas/maps/worms/";
    pub const TSI: &'static str = "http://arga.org.au/schemas/maps/tsi/";

    pub const RDF_SYNTAX: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
}


pub fn transform(path: &PathBuf) -> Result<(), Error> {
    let meta = meta(path)?;

    info!(mapping = meta.dataset.schema, "Creating triples database (TriG)");
    let mut dataset = Dataset::new(&meta.dataset.schema);

    // load the mapping definitions
    dataset.load_trig_path("rdf/specimens.ttl")?;
    dataset.load_trig_path("rdf/subsamples.ttl")?;
    dataset.load_trig_path("rdf/extractions.ttl")?;
    dataset.load_trig_path("rdf/arga.ttl")?;

    // transform_test(path)?;

    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);

    for entry in archive.entries_with_seek()? {
        let entry = entry?;
        let header = entry.header();

        let size = header.size()?;
        let path = header.path()?;
        let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        if path.file_name().map(|p| p.to_str().unwrap_or_default()) == Some("meta.toml") {
            continue;
        }

        let message = format!("Loading {}", path.to_str().unwrap_or_default());
        let stream = ProgressStream::new(entry, size as usize, &message);

        dataset.load_csv(stream, &name)?;
    }

    info!("CSV files loaded into TriG dataset");
    export(dataset)

    // let graphs = vec!["http://arga.org.au/schemas/maps/tsi/subsamples"];
    // let graph = dataset.graph(&graphs);
    // dump_graph(&graph);
    // Ok(())
}


fn meta(path: &PathBuf) -> Result<Meta, Error> {
    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);
    let meta_filename = String::from("meta.toml");

    for entry in archive.entries_with_seek()? {
        let mut file = entry?;
        let path = file.header().path()?.to_str().unwrap_or_default().to_string();

        if path == meta_filename {
            let mut s = String::new();
            file.read_to_string(&mut s)?;
            let meta = toml::from_str(&s).map_err(|err| Error::Parsing(ParseError::Toml(err)))?;
            return Ok(meta);
        }
    }

    Err(Error::Parsing(ParseError::FileNotFound(meta_filename)))
}


#[derive(Debug, Default, serde::Serialize)]
pub struct Organism {
    pub entity_id: String,
    pub organism_id: Option<String>,
    pub scientific_name: Option<String>,
    pub sex: Option<String>,
    pub genotypic_sex: Option<String>,
    pub phenotypic_sex: Option<String>,
    pub life_stage: Option<String>,
    pub reproductive_condition: Option<String>,
    pub behavior: Option<String>,
    pub live_state: Option<String>,
    pub remarks: Option<String>,
}


#[derive(Debug, Default, serde::Serialize)]
pub struct Collecting {
    pub entity_id: String,
    pub organism_id: Option<String>,
    pub specimen_id: Option<String>,
    pub field_collecting_id: Option<String>,
    pub scientific_name: Option<String>,
    pub collected_by: Option<String>,
    pub collection_date: Option<String>,
    pub remarks: Option<String>,
    pub preparation: Option<String>,
    pub habitat: Option<String>,
    pub specific_host: Option<String>,
    pub individual_count: Option<String>,
    pub strain: Option<String>,
    pub isolate: Option<String>,
    pub permit: Option<String>,
    pub sampling_protocol: Option<String>,
    pub organism_killed: Option<String>,
    pub organism_kill_method: Option<String>,
    pub field_sample_disposition: Option<String>,
    pub field_notes: Option<String>,
    pub environment_broad_scale: Option<String>,
    pub environment_local_scale: Option<String>,
    pub environment_medium: Option<String>,
    pub locality: Option<String>,
    pub country: Option<String>,
    pub country_code: Option<String>,
    pub state_province: Option<String>,
    pub county: Option<String>,
    pub municipality: Option<String>,
    pub latitude: Option<String>,
    pub longitude: Option<String>,
    pub location_generalisation: Option<String>,
    pub location_source: Option<String>,
    pub elevation: Option<String>,
    pub elevation_accuracy: Option<String>,
    pub depth: Option<String>,
    pub depth_accuracy: Option<String>,
}


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

#[derive(Debug, Default, serde::Serialize)]
pub struct Subsample {
    pub entity_id: String,
    pub specimen_id: Option<String>,
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

#[derive(Debug, Default, serde::Serialize)]
pub struct Extraction {
    pub entity_id: String,
    pub subsample_id: Option<String>,
    pub extract_id: Option<String>,
    pub extracted_by: Option<String>,
    pub extracted_by_orcid: Option<String>,
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
    pub material_extracted_by: Option<String>,
    pub action_extracted: Option<String>,
    pub extraction_method: Option<String>,
    pub number_of_extracts_pooled: Option<String>,
}


fn export(dataset: Dataset) -> Result<(), Error> {
    info!("Exporting TriG dataset as importable CSV files");

    let mapped = mapped::Mapped { dataset };

    export_organisms(&mapped)?;
    export_collections(&mapped)?;
    export_tissues(&mapped)?;
    export_subsamples(&mapped)?;
    export_extractions(&mapped)?;

    Ok(())
}


fn export_organisms(mapped: &mapped::Mapped) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path("organisms.csv")?;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/organisms",
    ];

    let ids = get_entity_ids(&graphs, &mapped)?;

    for id in ids {
        let mut record = Organism {
            entity_id: id.clone(),
            ..Default::default()
        };

        let fields = get_fields(&id, "organism", &graphs, &mapped)?;
        for (field, value) in fields {
            let term: rdf::Organism = field
                .as_iri()
                .try_into()
                .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

            let field: OrganismField = (term, value).try_into()?;
            match field {
                OrganismField::OrganismId(val) => record.organism_id = Some(val),
                OrganismField::ScientificName(val) => record.scientific_name = Some(val),
                OrganismField::Sex(val) => record.sex = Some(val),
                OrganismField::GenotypicSex(val) => record.genotypic_sex = Some(val),
                OrganismField::PhenotypicSex(val) => record.phenotypic_sex = Some(val),
                OrganismField::LifeStage(val) => record.life_stage = Some(val),
                OrganismField::ReproductiveCondition(val) => record.reproductive_condition = Some(val),
                OrganismField::Behavior(val) => record.behavior = Some(val),
                OrganismField::LiveState(val) => record.live_state = Some(val),
                OrganismField::Remarks(val) => record.remarks = Some(val),
            }
        }

        writer.serialize(record)?;
    }

    Ok(())
}

fn export_collections(mapped: &mapped::Mapped) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path("collections.csv")?;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/collecting",
    ];

    let ids = get_entity_ids(&graphs, &mapped)?;

    for id in ids {
        let mut record = Collecting {
            entity_id: id.clone(),
            ..Default::default()
        };

        let fields = get_fields(&id, "collecting_event", &graphs, &mapped)?;
        for (field, value) in fields {
            let term: rdf::Collecting = field
                .as_iri()
                .try_into()
                .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

            let field: CollectingField = (term, value).try_into()?;
            match field {
                CollectingField::OrganismId(val) => record.organism_id = Some(val),
                CollectingField::MaterialSampleId(val) => record.specimen_id = Some(val),
                CollectingField::FieldCollectingId(val) => record.field_collecting_id = Some(val),
                CollectingField::ScientificName(val) => record.scientific_name = Some(val),
                CollectingField::CollectedBy(val) => record.collected_by = Some(val),
                CollectingField::CollectionDate(val) => record.collection_date = Some(val),
                CollectingField::Remarks(val) => record.remarks = Some(val),
                CollectingField::Preparation(val) => record.preparation = Some(val),
                CollectingField::Habitat(val) => record.habitat = Some(val),
                CollectingField::SpecificHost(val) => record.specific_host = Some(val),
                CollectingField::IndividualCount(val) => record.habitat = Some(val),
                CollectingField::Strain(val) => record.strain = Some(val),
                CollectingField::Isolate(val) => record.isolate = Some(val),
                CollectingField::Permit(val) => record.permit = Some(val),
                CollectingField::SamplingProtocol(val) => record.sampling_protocol = Some(val),
                CollectingField::OrganismKilled(val) => record.organism_killed = Some(val),
                CollectingField::OrganismKillMethod(val) => record.organism_kill_method = Some(val),
                CollectingField::FieldSampleDisposition(val) => record.field_sample_disposition = Some(val),
                CollectingField::FieldNotes(val) => record.field_notes = Some(val),
                CollectingField::EnvironmentBroadScale(val) => record.environment_broad_scale = Some(val),
                CollectingField::EnvironmentLocalScale(val) => record.environment_local_scale = Some(val),
                CollectingField::EnvironmentMedium(val) => record.environment_medium = Some(val),
                CollectingField::Locality(val) => record.locality = Some(val),
                CollectingField::Country(val) => record.country = Some(val),
                CollectingField::CountryCode(val) => record.country_code = Some(val),
                CollectingField::StateProvince(val) => record.state_province = Some(val),
                CollectingField::County(val) => record.county = Some(val),
                CollectingField::Municipality(val) => record.municipality = Some(val),
                CollectingField::Latitude(val) => record.latitude = Some(val),
                CollectingField::Longitude(val) => record.longitude = Some(val),
                CollectingField::LocationGeneralisation(val) => record.location_generalisation = Some(val),
                CollectingField::LocationSource(val) => record.location_source = Some(val),
                CollectingField::Elevation(val) => record.elevation = Some(val),
                CollectingField::ElevationAccuracy(val) => record.elevation_accuracy = Some(val),
                CollectingField::Depth(val) => record.depth = Some(val),
                CollectingField::DepthAccuracy(val) => record.depth_accuracy = Some(val),
            }
        }

        writer.serialize(record)?;
    }

    Ok(())
}

fn export_tissues(mapped: &mapped::Mapped) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path("tissues.csv")?;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/tissues",
    ];

    let collecting_graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/collecting",
    ];

    let ids = get_entity_ids(&graphs, &mapped)?;

    for id in ids {
        let mut tissue = Tissue {
            entity_id: id.clone(),
            ..Default::default()
        };

        let fields = get_fields(&id, "tissue", &graphs, &mapped)?;
        for (field, value) in fields {
            let term: rdf::Tissue = field
                .as_iri()
                .try_into()
                .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

            let field: TissueField = (term, value).try_into()?;
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
                TissueField::Source(val) => tissue.source = Some(val),
                TissueField::SourceUrl(val) => tissue.source_url = Some(val),
            }
        }

        if let Some(material_sample_id) = &tissue.material_sample_id {
            let fields = get_fields(&material_sample_id, "collecting_event", &collecting_graphs, &mapped)?;
            for (field, value) in fields {
                let term: rdf::Collecting = field
                    .as_iri()
                    .try_into()
                    .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

                let field: CollectingField = (term, value).try_into()?;
                match field {
                    CollectingField::ScientificName(val) => tissue.scientific_name = Some(val),
                    _ => {}
                }
            }
        }

        writer.serialize(tissue)?;
    }

    Ok(())
}


fn export_subsamples(mapped: &mapped::Mapped) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path("subsamples.csv")?;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/subsamples",
    ];

    let collecting_graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/tissues",
        "http://arga.org.au/schemas/maps/tsi/collecting",
    ];

    let ids = get_entity_ids(&graphs, &mapped)?;

    for id in ids {
        let mut subsample = Subsample {
            entity_id: id.clone(),
            ..Default::default()
        };

        let fields = get_fields(&id, "subsample_event", &graphs, &mapped)?;
        for (field, value) in fields {
            let term: rdf::Subsample = field
                .as_iri()
                .try_into()
                .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

            let field: SubsampleField = (term, value).try_into()?;
            match field {
                SubsampleField::TissueId(val) => subsample.specimen_id = Some(val),
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

        if let Some(specimen_id) = &subsample.specimen_id {
            let name = get_specimen_name(&specimen_id, &collecting_graphs, &mapped)?;
            subsample.scientific_name = name;
        }

        writer.serialize(subsample)?;
    }

    Ok(())
}


fn export_extractions(mapped: &mapped::Mapped) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path("extractions.csv")?;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/extractions",
    ];

    let ids = get_entity_ids(&graphs, &mapped)?;

    for id in ids {
        let mut extraction = Extraction {
            entity_id: id.clone(),
            ..Default::default()
        };

        let fields = get_fields(&id, "extraction_event", &graphs, &mapped)?;
        for (field, value) in fields {
            let term: rdf::Extraction = field
                .as_iri()
                .try_into()
                .map_err(|_| TransformError::InvalidMappingIri(field.to_string()))?;

            let field: ExtractionField = (term, value).try_into()?;
            match field {
                ExtractionField::SubsampleId(val) => extraction.subsample_id = Some(val),
                ExtractionField::ExtractId(val) => extraction.extract_id = Some(val),
                ExtractionField::ExtractedBy(val) => extraction.extracted_by = Some(val),
                ExtractionField::ExtractedByOrcid(val) => extraction.extracted_by_orcid = Some(val),
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
                ExtractionField::MaterialExtractedBy(val) => extraction.material_extracted_by = Some(val),
                ExtractionField::ActionExtracted(val) => extraction.action_extracted = Some(val),
                ExtractionField::ExtractionMethod(val) => extraction.extraction_method = Some(val),
                ExtractionField::NumberOfExtractsPooled(val) => extraction.number_of_extracts_pooled = Some(val),
            }
        }

        writer.serialize(extraction)?;
    }

    Ok(())
}


fn get_entity_ids(graphs: &Vec<&str>, mapped: &mapped::Mapped) -> Result<Vec<String>, Error> {
    let rows = mapped.query(
        r#"
BASE <http://arga.org.au/schemas/model/>
PREFIX : <http://arga.org.au/schemas/rel/>
PREFIX fields: <http://arga.org.au/schemas/fields/>

SELECT ?value WHERE {{
  fields:entity_id ?p ?mapping.
  ?id ?mapping ?value
}}
"#,
        &graphs,
    )?;

    let mut ids = Vec::with_capacity(rows.len());

    for mut row in rows.into_iter() {
        if let Some(mapped::Value::Literal(value)) = row.pop().unwrap_or(None) {
            match value {
                mapped::Literal::String(id) => ids.push(id),
            }
        }
    }

    Ok(ids)
}


fn get_fields(
    id: &str,
    model: &str,
    graphs: &Vec<&str>,
    mapped: &mapped::Mapped,
) -> Result<Vec<(IriBuf, Literal)>, TransformError> {
    let rows = mapped.query(
        &format!(
            r#"
BASE <http://arga.org.au/schemas/model/>
PREFIX : <http://arga.org.au/schemas/rel/>
PREFIX fields: <http://arga.org.au/schemas/fields/>

SELECT ?fields ?value WHERE {{
  <{model}> :has ?fields.
  ?fields ?p ?mapping.
  "{id}" ?mapping ?value
}}
"#
        ),
        &graphs,
    )?;

    let mut results = Vec::with_capacity(rows.len());

    for row in rows.into_iter() {
        let field = row.get(0).unwrap_or(&None).clone();
        let value = row.get(1).unwrap_or(&None).clone();

        match (field, value) {
            (Some(Value::Iri(field)), Some(Value::Literal(value))) => {
                results.push((IriBuf::new(field)?, value));
            }
            (field, value) => return Err(TransformError::Field { field, value }),
        }
    }

    Ok(results)
}

fn get_specimen_name(id: &str, graphs: &Vec<&str>, mapped: &mapped::Mapped) -> Result<Option<String>, TransformError> {
    let query = format!(
        r#"
BASE <http://arga.org.au/schemas/fields/>
PREFIX : <http://arga.org.au/schemas/mapping/>

SELECT ?value WHERE {{
  <material_sample_id> :same ?specimen_map.
  "{id}" ?specimen_map ?specimen_id.

  <scientific_name> :same ?name_map.
  ?specimen_id ?name_map ?value
}}
"#
    );

    let rows = mapped.query(&query, graphs)?;
    for row in rows.into_iter() {
        if let Some(Value::Literal(value)) = row.get(0).unwrap_or(&None) {
            return Ok(match value {
                Literal::String(val) => Some(val.clone()),
            });
        }
    }

    Ok(None)
}


pub fn transform_test(path: &PathBuf) -> Result<(), crate::errors::Error> {
    let mut dataset = Dataset::new(prefix::TSI);
    dataset.load_trig_path("rdf/specimens.ttl")?;
    dataset.load_trig_path("rdf/arga.ttl")?;

    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);

    for entry in archive.entries_with_seek()? {
        let entry = entry?;
        let header = entry.header();

        let size = header.size()?;
        let path = header.path()?;
        let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        if path.file_name().map(|p| p.to_str().unwrap_or_default()) == Some("meta.toml") {
            continue;
        }

        let message = format!("Loading {}", path.to_str().unwrap_or_default());
        let stream = ProgressStream::new(entry, size as usize, &message);

        dataset.load_csv(stream, &name)?;
    }

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/organisms",
        "http://arga.org.au/schemas/maps/tsi/collecting",
        "http://arga.org.au/schemas/maps/tsi/tissues",
    ];

    let mapped = mapped::Mapped { dataset };
    dump_graph(&mapped.dataset.graph(&graphs));


    let rows = mapped.query(
        r#"
BASE <http://arga.org.au/schemas/model/>
PREFIX : <http://arga.org.au/schemas/mapping/>
PREFIX names: <http://arga.org.au/schemas/names/>

SELECT ?entity_id ?storage ?fixation ?tissue_type ?sex WHERE {{
  names:entity_id :hash ?entity_id_map.
  names:organism_id :same ?organism_id_map.
  names:storage :same ?storage_map.
  names:fixation :same ?fixation_map.
  names:tissue_type :same ?tissue_type_map.
  names:sex :same ?sex_map.

  ?entity_id ?entity_id_map ?o.
  ?entity_id ?organism_id_map ?organism_id.
  ?entity_id ?storage_map ?storage.
  ?entity_id ?fixation_map ?fixation.
  ?entity_id ?tissue_type_map ?tissue_type.

  ?organism_id ?sex_map ?sex.

  ?entity_id ?organism_id_map "ANU-ST67".
}}
"#,
        &graphs,
    )?;

    println!("{rows:?}");
    println!("--------------------!!!");


    let rows = mapped.query(
        r#"
BASE <http://arga.org.au/schemas/model/>
PREFIX : <http://arga.org.au/schemas/rel/>
PREFIX fields: <http://arga.org.au/schemas/fields/>

SELECT ?id ?mapping ?value WHERE {{
  <tissue> :references ?links.
  ?links :has ?fields.
  ?fields ?p ?mapping.
  ?id ?mapping ?value.
  FILTER(?id = "ANU-ST67")
}}
"#,
        &graphs,
    )?;

    for row in rows {
        let cols: Vec<String> = row.into_iter().map(|v| format!("{:?}", v.unwrap())).collect();
        println!("{}", cols.join("  "));
    }

    Ok(())
}


fn dump_graph(graph: &dataset::PartialGraph) {
    println!("-----------------------------");
    for triple in graph.triples() {
        dump_triple(triple.unwrap());
    }
    println!("-----------------------------");
}


fn dump_triple(triple: [&SimpleTerm<'_>; 3]) {
    let [s, p, o] = triple;

    let subject = match s {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        SimpleTerm::BlankNode(bnode_id) => bnode_id.as_str(),
        SimpleTerm::LiteralDatatype(lit, _t) => lit,
        _ => unimplemented!(),
    };

    let predicate = match p {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        _ => unimplemented!(),
    };

    let object = match o {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        SimpleTerm::BlankNode(bnode_id) => bnode_id.as_str(),
        SimpleTerm::LiteralDatatype(lit, _t) => lit,
        _ => unimplemented!(),
    };

    println!("{subject} {predicate} {object}")
}

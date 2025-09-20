use std::borrow::Borrow;

use iref_enum::IriEnum;
use sophia::api::term::{SimpleTerm, Term};

use crate::errors::TransformError;


#[derive(Debug, Clone)]
pub enum Value {
    Iri(String),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    String(String),
}


#[derive(Debug, IriEnum)]
#[iri_prefix("mapping" = "http://arga.org.au/schemas/mapping/")]
pub enum Mapping {
    /// The subject and object IRIs reflect the same definition
    /// and can be copied across without transformation.
    #[iri("mapping:same")]
    Same,

    /// The subject is the value of the object after it is
    /// hashed with the xxh3 algorithm to become a content derived hash.
    #[iri("mapping:hash")]
    Hash,

    /// The subject is the value of the first IRI in the object list
    /// that has a value after it is hashed to become a content derived hash.
    #[iri("mapping:hash_first")]
    HashFirst,
}

impl TryFrom<&SimpleTerm<'static>> for Mapping {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        let mapping = try_from_term(&value)?;
        Ok(mapping)
    }
}


#[derive(Debug, Clone)]
pub enum Map {
    Same(iref::IriBuf),
    Hash(iref::IriBuf),
    HashFirst(Vec<iref::IriBuf>),
}


#[derive(Debug, IriEnum)]
#[iri_prefix("rdfs" = "http://www.w3.org/1999/02/22-rdf-syntax-ns")]
pub enum Rdfs {
    #[iri("rdfs:#first")]
    First,
    #[iri("rdfs:#rest")]
    Rest,
    #[iri("rdfs:#nil")]
    Nil,
}

impl TryFrom<&SimpleTerm<'static>> for Rdfs {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        let mapping = try_from_term(&value)?;
        Ok(mapping)
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Publication {
    #[iri("fields:publication_entity_id")]
    EntityId,
    #[iri("fields:title")]
    Title,
    #[iri("fields:authors")]
    Authors,
    #[iri("fields:published_year")]
    PublishedYear,
    #[iri("fields:published_date")]
    PublishedDate,
    #[iri("fields:language")]
    Language,
    #[iri("fields:publisher")]
    Publisher,
    #[iri("fields:doi")]
    Doi,
    #[iri("fields:publication_type")]
    PublicationType,
    #[iri("fields:citation")]
    Citation,
    #[iri("fields:source_url")]
    SourceUrl,
}


#[derive(Debug, Clone)]
pub enum PublicationField {
    EntityId(String),
    Title(String),
    Authors(String),
    PublishedYear(String),
    PublishedDate(String),
    Language(String),
    Publisher(String),
    Doi(String),
    PublicationType(String),
    Citation(String),
    SourceUrl(String),
}

impl From<(Publication, Literal)> for PublicationField {
    fn from(source: (Publication, Literal)) -> Self {
        match source {
            (Publication::EntityId, Literal::String(value)) => Self::EntityId(value),
            (Publication::Title, Literal::String(value)) => Self::Title(value),
            (Publication::Authors, Literal::String(value)) => Self::Authors(value),
            (Publication::PublishedYear, Literal::String(value)) => Self::PublishedYear(value),
            (Publication::PublishedDate, Literal::String(value)) => Self::PublishedDate(value),
            (Publication::Language, Literal::String(value)) => Self::Language(value),
            (Publication::Publisher, Literal::String(value)) => Self::Publisher(value),
            (Publication::Doi, Literal::String(value)) => Self::Doi(value),
            (Publication::PublicationType, Literal::String(value)) => Self::PublicationType(value),
            (Publication::Citation, Literal::String(value)) => Self::Citation(value),
            (Publication::SourceUrl, Literal::String(value)) => Self::SourceUrl(value),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Tissue {
    #[iri("fields:organism_id")]
    OrganismId,
    #[iri("fields:tissue_id")]
    TissueId,
    #[iri("fields:material_sample_id")]
    MaterialSampleId,
    #[iri("fields:original_catalogue_name")]
    OriginalCatalogueName,
    #[iri("fields:current_catalogue_name")]
    CurrentCatalogueName,
    #[iri("fields:identification_verified")]
    IdentificationVerified,
    #[iri("fields:reference_material")]
    ReferenceMaterial,
    #[iri("fields:registered_by")]
    RegisteredBy,
    #[iri("fields:registration_date")]
    RegistrationDate,
    #[iri("fields:custodian")]
    Custodian,
    #[iri("fields:institution")]
    Institution,
    #[iri("fields:institution_code")]
    InstitutionCode,
    #[iri("fields:collection")]
    Collection,
    #[iri("fields:collection_code")]
    CollectionCode,
    #[iri("fields:status")]
    Status,
    #[iri("fields:current_status")]
    CurrentStatus,
    #[iri("fields:sampling_protocol")]
    SamplingProtocol,
    #[iri("fields:tissue_type")]
    TissueType,
    #[iri("fields:disposition")]
    Disposition,
    #[iri("fields:fixation")]
    Fixation,
    #[iri("fields:storage")]
    Storage,
    #[iri("fields:citation")]
    Citation,
    #[iri("fields:source_url")]
    SourceUrl,
}


#[derive(Debug, Clone)]
pub enum TissueField {
    OrganismId(String),
    TissueId(String),
    MaterialSampleId(String),
    OriginalCatalogueName(String),
    CurrentCatalogueName(String),
    IdentificationVerified(String),
    ReferenceMaterial(String),
    RegisteredBy(String),
    RegistrationDate(String),
    Custodian(String),
    Institution(String),
    InstitutionCode(String),
    Collection(String),
    CollectionCode(String),
    Status(String),
    CurrentStatus(String),
    SamplingProtocol(String),
    TissueType(String),
    Disposition(String),
    Fixation(String),
    Storage(String),
    Citation(String),
    SourceUrl(String),
}


impl From<(Tissue, Literal)> for TissueField {
    fn from(source: (Tissue, Literal)) -> Self {
        match source {
            (Tissue::OrganismId, Literal::String(value)) => Self::OrganismId(value),
            (Tissue::TissueId, Literal::String(value)) => Self::TissueId(value),
            (Tissue::MaterialSampleId, Literal::String(value)) => Self::MaterialSampleId(value),
            (Tissue::OriginalCatalogueName, Literal::String(value)) => Self::OriginalCatalogueName(value),
            (Tissue::CurrentCatalogueName, Literal::String(value)) => Self::CurrentCatalogueName(value),
            (Tissue::IdentificationVerified, Literal::String(value)) => Self::IdentificationVerified(value),
            (Tissue::ReferenceMaterial, Literal::String(value)) => Self::ReferenceMaterial(value),
            (Tissue::RegisteredBy, Literal::String(value)) => Self::RegisteredBy(value),
            (Tissue::RegistrationDate, Literal::String(value)) => Self::RegistrationDate(value),
            (Tissue::Custodian, Literal::String(value)) => Self::Custodian(value),
            (Tissue::Institution, Literal::String(value)) => Self::Institution(value),
            (Tissue::InstitutionCode, Literal::String(value)) => Self::InstitutionCode(value),
            (Tissue::Collection, Literal::String(value)) => Self::Collection(value),
            (Tissue::CollectionCode, Literal::String(value)) => Self::CollectionCode(value),
            (Tissue::Status, Literal::String(value)) => Self::Status(value),
            (Tissue::CurrentStatus, Literal::String(value)) => Self::CurrentStatus(value),
            (Tissue::SamplingProtocol, Literal::String(value)) => Self::SamplingProtocol(value),
            (Tissue::TissueType, Literal::String(value)) => Self::TissueType(value),
            (Tissue::Disposition, Literal::String(value)) => Self::Disposition(value),
            (Tissue::Fixation, Literal::String(value)) => Self::Fixation(value),
            (Tissue::Storage, Literal::String(value)) => Self::Storage(value),
            (Tissue::Citation, Literal::String(value)) => Self::Citation(value),
            (Tissue::SourceUrl, Literal::String(value)) => Self::SourceUrl(value),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Collecting {
    #[iri("fields:material_sample_id")]
    MaterialSampleId,
    #[iri("fields:scientific_name")]
    ScientificName,
    #[iri("fields:organism_id")]
    OrganismId,
    #[iri("fields:field_collecting_id")]
    FieldCollectingId,

    #[iri("fields:collected_by")]
    CollectedBy,
    #[iri("fields:collection_date")]
    CollectionDate,
    #[iri("fields:remarks")]
    Remarks,

    #[iri("fields:preparation")]
    Preparation,
    #[iri("fields:habitat")]
    Habitat,
    #[iri("fields:specific_host")]
    SpecificHost,
    #[iri("fields:individual_count")]
    IndividualCount,
    #[iri("fields:strain")]
    Strain,
    #[iri("fields:isolate")]
    Isolate,

    #[iri("fields:permit")]
    Permit,
    #[iri("fields:sampling_protocol")]
    SamplingProtocol,
    #[iri("fields:organism_killed")]
    OrganismKilled,
    #[iri("fields:organism_kill_method")]
    OrganismKillMethod,
    #[iri("fields:field_sample_disposition")]
    FieldSampleDisposition,
    #[iri("fields:field_notes")]
    FieldNotes,

    #[iri("fields:environment_broad_scale")]
    EnvironmentBroadScale,
    #[iri("fields:environment_local_scale")]
    EnvironmentLocalScale,
    #[iri("fields:environment_medium")]
    EnvironmentMedium,

    #[iri("fields:locality")]
    Locality,
    #[iri("fields:country")]
    Country,
    #[iri("fields:country_code")]
    CountryCode,
    #[iri("fields:state_province")]
    StateProvince,
    #[iri("fields:county")]
    County,
    #[iri("fields:municipality")]
    Municipality,
    #[iri("fields:latitude")]
    Latitude,
    #[iri("fields:longitude")]
    Longitude,
    #[iri("fields:location_generalisation")]
    LocationGeneralisation,
    #[iri("fields:location_source")]
    LocationSource,
    #[iri("fields:elevation")]
    Elevation,
    #[iri("fields:elevation_accuracy")]
    ElevationAccuracy,
    #[iri("fields:depth")]
    Depth,
    #[iri("fields:depth_accuracy")]
    DepthAccuracy,
}


#[derive(Debug, Clone)]
pub enum CollectingField {
    OrganismId(String),
    MaterialSampleId(String),
    FieldCollectingId(String),
    ScientificName(String),

    CollectedBy(String),
    CollectionDate(String),
    Remarks(String),

    Preparation(String),
    Habitat(String),
    SpecificHost(String),
    IndividualCount(String),
    Strain(String),
    Isolate(String),

    Permit(String),
    SamplingProtocol(String),
    OrganismKilled(String),
    OrganismKillMethod(String),
    FieldSampleDisposition(String),
    FieldNotes(String),

    EnvironmentBroadScale(String),
    EnvironmentLocalScale(String),
    EnvironmentMedium(String),

    Locality(String),
    Country(String),
    CountryCode(String),
    StateProvince(String),
    County(String),
    Municipality(String),
    Latitude(String),
    Longitude(String),
    LocationGeneralisation(String),
    LocationSource(String),
    Elevation(String),
    ElevationAccuracy(String),
    Depth(String),
    DepthAccuracy(String),
}


impl From<(Collecting, Literal)> for CollectingField {
    fn from(source: (Collecting, Literal)) -> Self {
        match source {
            (Collecting::OrganismId, Literal::String(value)) => Self::OrganismId(value),
            (Collecting::MaterialSampleId, Literal::String(value)) => Self::MaterialSampleId(value),
            (Collecting::FieldCollectingId, Literal::String(value)) => Self::FieldCollectingId(value),
            (Collecting::ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (Collecting::CollectedBy, Literal::String(value)) => Self::CollectedBy(value),
            (Collecting::CollectionDate, Literal::String(value)) => Self::CollectionDate(value),
            (Collecting::Remarks, Literal::String(value)) => Self::Remarks(value),
            (Collecting::Preparation, Literal::String(value)) => Self::Preparation(value),
            (Collecting::Habitat, Literal::String(value)) => Self::Habitat(value),
            (Collecting::SpecificHost, Literal::String(value)) => Self::SpecificHost(value),
            (Collecting::IndividualCount, Literal::String(value)) => Self::IndividualCount(value),
            (Collecting::Strain, Literal::String(value)) => Self::Strain(value),
            (Collecting::Isolate, Literal::String(value)) => Self::Isolate(value),
            (Collecting::Permit, Literal::String(value)) => Self::Permit(value),
            (Collecting::SamplingProtocol, Literal::String(value)) => Self::SamplingProtocol(value),
            (Collecting::OrganismKilled, Literal::String(value)) => Self::OrganismKilled(value),
            (Collecting::OrganismKillMethod, Literal::String(value)) => Self::OrganismKillMethod(value),
            (Collecting::FieldSampleDisposition, Literal::String(value)) => Self::FieldSampleDisposition(value),
            (Collecting::FieldNotes, Literal::String(value)) => Self::FieldNotes(value),
            (Collecting::EnvironmentBroadScale, Literal::String(value)) => Self::EnvironmentBroadScale(value),
            (Collecting::EnvironmentLocalScale, Literal::String(value)) => Self::EnvironmentLocalScale(value),
            (Collecting::EnvironmentMedium, Literal::String(value)) => Self::EnvironmentMedium(value),

            (Collecting::Locality, Literal::String(value)) => Self::Locality(value),
            (Collecting::Country, Literal::String(value)) => Self::Country(value),
            (Collecting::CountryCode, Literal::String(value)) => Self::CountryCode(value),
            (Collecting::StateProvince, Literal::String(value)) => Self::StateProvince(value),
            (Collecting::County, Literal::String(value)) => Self::County(value),
            (Collecting::Municipality, Literal::String(value)) => Self::Municipality(value),
            (Collecting::Latitude, Literal::String(value)) => Self::Latitude(value),
            (Collecting::Longitude, Literal::String(value)) => Self::Longitude(value),
            (Collecting::LocationGeneralisation, Literal::String(value)) => Self::LocationGeneralisation(value),
            (Collecting::LocationSource, Literal::String(value)) => Self::LocationSource(value),
            (Collecting::Elevation, Literal::String(value)) => Self::Elevation(value),
            (Collecting::ElevationAccuracy, Literal::String(value)) => Self::ElevationAccuracy(value),
            (Collecting::Depth, Literal::String(value)) => Self::Depth(value),
            (Collecting::DepthAccuracy, Literal::String(value)) => Self::DepthAccuracy(value),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Organism {
    #[iri("fields:organism_id")]
    OrganismId,
    #[iri("fields:scientific_name")]
    ScientificName,
    #[iri("fields:sex")]
    Sex,
    #[iri("fields:genotypic_sex")]
    GenotypicSex,
    #[iri("fields:phenotypic_sex")]
    PhenotypicSex,
    #[iri("fields:life_stage")]
    LifeStage,
    #[iri("fields:reproductive_condition")]
    ReproductiveCondition,
    #[iri("fields:behavior")]
    Behavior,
    #[iri("fields:live_state")]
    LiveState,
    #[iri("fields:remarks")]
    Remarks,
}


#[derive(Debug, Clone)]
pub enum OrganismField {
    OrganismId(String),
    ScientificName(String),
    Sex(String),
    GenotypicSex(String),
    PhenotypicSex(String),
    LifeStage(String),
    ReproductiveCondition(String),
    Behavior(String),
    LiveState(String),
    Remarks(String),
}


impl From<(Organism, Literal)> for OrganismField {
    fn from(source: (Organism, Literal)) -> Self {
        match source {
            (Organism::OrganismId, Literal::String(value)) => Self::OrganismId(value),
            (Organism::ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (Organism::Sex, Literal::String(value)) => Self::Sex(value),
            (Organism::GenotypicSex, Literal::String(value)) => Self::GenotypicSex(value),
            (Organism::PhenotypicSex, Literal::String(value)) => Self::PhenotypicSex(value),
            (Organism::LifeStage, Literal::String(value)) => Self::LifeStage(value),
            (Organism::ReproductiveCondition, Literal::String(value)) => Self::ReproductiveCondition(value),
            (Organism::Behavior, Literal::String(value)) => Self::Behavior(value),
            (Organism::LiveState, Literal::String(value)) => Self::LiveState(value),
            (Organism::Remarks, Literal::String(value)) => Self::Remarks(value),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Subsample {
    #[iri("fields:tissue_id")]
    TissueId,
    #[iri("fields:subsample_id")]
    SubsampleId,
    #[iri("fields:sample_type")]
    SampleType,
    #[iri("fields:institution")]
    Institution,
    #[iri("fields:institution_code")]
    InstitutionCode,
    #[iri("fields:name")]
    Name,
    #[iri("fields:custodian")]
    Custodian,
    #[iri("fields:description")]
    Description,
    #[iri("fields:notes")]
    Notes,
    #[iri("fields:culture_method")]
    CultureMethod,
    #[iri("fields:culture_media")]
    CultureMedia,
    #[iri("fields:weight_or_vol")]
    WeightOrVolume,
    #[iri("fields:preservation_method")]
    PreservationMethod,
    #[iri("fields:preservation_temperature")]
    PreservationTemperature,
    #[iri("fields:preservation_duration")]
    PreservationDuration,
    #[iri("fields:quality")]
    Quality,
    #[iri("fields:cell_type")]
    CellType,
    #[iri("fields:cell_line")]
    CellLine,
    #[iri("fields:clone_name")]
    CloneName,
    #[iri("fields:lab_host")]
    LabHost,
    #[iri("fields:sample_processing")]
    SampleProcessing,
    #[iri("fields:sample_pooling")]
    SamplePooling,
}


#[derive(Debug, Clone)]
pub enum SubsampleField {
    TissueId(String),
    SubsampleId(String),
    SampleType(String),
    Institution(String),
    InstitutionCode(String),
    Name(String),
    Custodian(String),
    Description(String),
    Notes(String),
    CultureMethod(String),
    CultureMedia(String),
    WeightOrVolume(String),
    PreservationMethod(String),
    PreservationTemperature(String),
    PreservationDuration(String),
    Quality(String),
    CellType(String),
    CellLine(String),
    CloneName(String),
    LabHost(String),
    SampleProcessing(String),
    SamplePooling(String),
}


impl From<(Subsample, Literal)> for SubsampleField {
    fn from(source: (Subsample, Literal)) -> Self {
        match source {
            (Subsample::TissueId, Literal::String(value)) => Self::TissueId(value),
            (Subsample::SubsampleId, Literal::String(value)) => Self::SubsampleId(value),
            (Subsample::SampleType, Literal::String(value)) => Self::SampleType(value),
            (Subsample::Institution, Literal::String(value)) => Self::Institution(value),
            (Subsample::InstitutionCode, Literal::String(value)) => Self::InstitutionCode(value),
            (Subsample::Name, Literal::String(value)) => Self::Name(value),
            (Subsample::Custodian, Literal::String(value)) => Self::Custodian(value),
            (Subsample::Description, Literal::String(value)) => Self::Description(value),
            (Subsample::Notes, Literal::String(value)) => Self::Notes(value),
            (Subsample::CultureMethod, Literal::String(value)) => Self::CultureMethod(value),
            (Subsample::CultureMedia, Literal::String(value)) => Self::CultureMedia(value),
            (Subsample::WeightOrVolume, Literal::String(value)) => Self::WeightOrVolume(value),
            (Subsample::PreservationMethod, Literal::String(value)) => Self::PreservationMethod(value),
            (Subsample::PreservationTemperature, Literal::String(value)) => Self::PreservationTemperature(value),
            (Subsample::PreservationDuration, Literal::String(value)) => Self::PreservationDuration(value),
            (Subsample::Quality, Literal::String(value)) => Self::Quality(value),
            (Subsample::CellType, Literal::String(value)) => Self::CellType(value),
            (Subsample::CellLine, Literal::String(value)) => Self::CellLine(value),
            (Subsample::CloneName, Literal::String(value)) => Self::CloneName(value),
            (Subsample::LabHost, Literal::String(value)) => Self::LabHost(value),
            (Subsample::SampleProcessing, Literal::String(value)) => Self::SampleProcessing(value),
            (Subsample::SamplePooling, Literal::String(value)) => Self::SamplePooling(value),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Extraction {
    #[iri("fields:subsample_id")]
    SubsampleId,
    #[iri("fields:extract_id")]
    ExtractId,
    #[iri("fields:extracted_by")]
    ExtractedBy,
    #[iri("fields:extracted_by_orcid")]
    ExtractedByOrcid,
    #[iri("fields:extraction_date")]
    ExtractionDate,
    #[iri("fields:nucleic_acid_type")]
    NucleicAcidType,
    #[iri("fields:nucleic_acid_conformation")]
    NucleicAcidConformation,
    #[iri("fields:nucleic_acid_preservation_method")]
    NucleicAcidPreservationMethod,
    #[iri("fields:nucleic_acid_concentration")]
    NucleicAcidConcentration,
    #[iri("fields:nucleic_acid_quantification")]
    NucleicAcidQuantification,
    #[iri("fields:concentration_unit")]
    ConcentrationUnit,
    #[iri("fields:absorbance_260_230_ratio")]
    Absorbance260230Ratio,
    #[iri("fields:absorbance_260_280_ratio")]
    Absorbance260280Ratio,
    #[iri("fields:cell_lysis_method")]
    CellLysisMethod,
    #[iri("fields:material_extracted_by")]
    MaterialExtractedBy,
    #[iri("fields:material_extracted_by_orcid")]
    MaterialExtractedByOrcid,
    #[iri("fields:action_extracted")]
    ActionExtracted,
    #[iri("fields:extraction_method")]
    ExtractionMethod,
    #[iri("fields:number_of_extracts_pooled")]
    NumberOfExtractsPooled,

    #[iri("fields:extracted_by_entity_id")]
    ExtractedByEntityId,
    #[iri("fields:material_extracted_by_entity_id")]
    MaterialExtractedByEntityId,
}


#[derive(Debug, Clone)]
pub enum ExtractionField {
    SubsampleId(String),
    ExtractId(String),
    ExtractedBy(String),
    ExtractedByOrcid(String),
    ExtractionDate(String),
    NucleicAcidType(String),
    NucleicAcidConformation(String),
    NucleicAcidPreservationMethod(String),
    NucleicAcidConcentration(String),
    NucleicAcidQuantification(String),
    ConcentrationUnit(String),
    Absorbance260230Ratio(String),
    Absorbance260280Ratio(String),
    CellLysisMethod(String),
    MaterialExtractedBy(String),
    MaterialExtractedByOrcid(String),
    ActionExtracted(String),
    ExtractionMethod(String),
    NumberOfExtractsPooled(String),

    ExtractedByEntityId(String),
    MaterialExtractedByEntityId(String),
}


impl From<(Extraction, Literal)> for ExtractionField {
    fn from(source: (Extraction, Literal)) -> Self {
        use Extraction::*;
        match source {
            (SubsampleId, Literal::String(value)) => Self::SubsampleId(value),
            (ExtractId, Literal::String(value)) => Self::ExtractId(value),
            (ExtractedBy, Literal::String(value)) => Self::ExtractedBy(value),
            (ExtractedByOrcid, Literal::String(value)) => Self::ExtractedByOrcid(value),
            (ExtractionDate, Literal::String(value)) => Self::ExtractionDate(value),
            (NucleicAcidType, Literal::String(value)) => Self::NucleicAcidType(value),
            (NucleicAcidConformation, Literal::String(value)) => Self::NucleicAcidConformation(value),
            (NucleicAcidPreservationMethod, Literal::String(value)) => Self::NucleicAcidPreservationMethod(value),
            (NucleicAcidConcentration, Literal::String(value)) => Self::NucleicAcidConcentration(value),
            (NucleicAcidQuantification, Literal::String(value)) => Self::NucleicAcidQuantification(value),
            (ConcentrationUnit, Literal::String(value)) => Self::ConcentrationUnit(value),
            (Absorbance260230Ratio, Literal::String(value)) => Self::Absorbance260230Ratio(value),
            (Absorbance260280Ratio, Literal::String(value)) => Self::Absorbance260280Ratio(value),
            (CellLysisMethod, Literal::String(value)) => Self::CellLysisMethod(value),
            (MaterialExtractedBy, Literal::String(value)) => Self::MaterialExtractedBy(value),
            (MaterialExtractedByOrcid, Literal::String(value)) => Self::MaterialExtractedByOrcid(value),
            (ActionExtracted, Literal::String(value)) => Self::ActionExtracted(value),
            (ExtractionMethod, Literal::String(value)) => Self::ExtractionMethod(value),
            (NumberOfExtractsPooled, Literal::String(value)) => Self::NumberOfExtractsPooled(value),

            (ExtractedByEntityId, Literal::String(value)) => Self::ExtractedByEntityId(value),
            (MaterialExtractedByEntityId, Literal::String(value)) => Self::MaterialExtractedByEntityId(value),
        }
    }
}


pub fn try_from_term<'a, T>(value: &'a SimpleTerm<'static>) -> Result<T, TransformError>
where
    T: TryFrom<&'a iref::Iri>,
{
    match value {
        SimpleTerm::Iri(iri_ref) => try_from_iri(iri_ref),
        pred => Err(TransformError::InvalidMappingIri(format!("{pred:?}"))),
    }
}


pub trait IntoIriTerm {
    fn into_iri_term(&self) -> Result<SimpleTerm<'_>, TransformError>;
}

impl IntoIriTerm for iref::IriBuf {
    fn into_iri_term(&self) -> Result<SimpleTerm<'_>, TransformError> {
        let iri = sophia::iri::IriRef::new(self.to_string())?;
        Ok(iri.into_term())
    }
}

impl IntoIriTerm for &iref::Iri {
    fn into_iri_term(&self) -> Result<SimpleTerm<'_>, TransformError> {
        let iri = sophia::iri::IriRef::new(self.as_str())?;
        Ok(iri.into_term())
    }
}


pub fn try_from_iri<'a, T, R>(value: &'a T) -> Result<R, TransformError>
where
    T: ToIri,
    R: TryFrom<&'a iref::Iri>,
{
    let iri = value.to_iri()?;
    iri.try_into()
        .map_err(|_| TransformError::InvalidMappingIri(iri.to_string()))
}


pub trait ToIri {
    fn to_iri(&self) -> Result<&iref::Iri, TransformError>;
}

impl<T> ToIri for sophia::iri::IriRef<T>
where
    T: Borrow<str>,
{
    fn to_iri(&self) -> Result<&iref::Iri, TransformError> {
        iref::Iri::new(self).map_err(|_| TransformError::InvalidMappingIri(self.to_string()))
    }
}


pub trait ToIriOwned {
    fn to_iri_owned(&self) -> Result<iref::IriBuf, TransformError>;
}

impl<T> ToIriOwned for sophia::iri::IriRef<T>
where
    T: Borrow<str>,
{
    fn to_iri_owned(&self) -> Result<iref::IriBuf, TransformError> {
        let iri = iref::IriBuf::new(self.to_string())?;
        Ok(iri)
    }
}

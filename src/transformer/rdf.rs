use iref_enum::IriEnum;

use super::mapped::Literal;
use crate::errors::Error;


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
    #[iri("fields:source")]
    Source,
    #[iri("fields:source_url")]
    SourceUrl,
}


#[derive(Debug)]
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
    Source(String),
    SourceUrl(String),
}


impl TryFrom<(Tissue, Literal)> for TissueField {
    type Error = Error;

    fn try_from(source: (Tissue, Literal)) -> Result<Self, Self::Error> {
        let (field, lit) = source;
        let result = match (field, lit) {
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
            (Tissue::Source, Literal::String(value)) => Self::Source(value),
            (Tissue::SourceUrl, Literal::String(value)) => Self::SourceUrl(value),
        };

        Ok(result)
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


#[derive(Debug)]
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


impl TryFrom<(Collecting, Literal)> for CollectingField {
    type Error = Error;

    fn try_from(source: (Collecting, Literal)) -> Result<Self, Self::Error> {
        let (field, lit) = source;
        let result = match (field, lit) {
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
        };

        Ok(result)
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


#[derive(Debug)]
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


impl TryFrom<(Organism, Literal)> for OrganismField {
    type Error = Error;

    fn try_from(source: (Organism, Literal)) -> Result<Self, Self::Error> {
        let (field, lit) = source;
        let result = match (field, lit) {
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
        };

        Ok(result)
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


#[derive(Debug)]
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


impl TryFrom<(Subsample, Literal)> for SubsampleField {
    type Error = Error;

    fn try_from(source: (Subsample, Literal)) -> Result<Self, Self::Error> {
        let (field, lit) = source;
        let result = match (field, lit) {
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
        };

        Ok(result)
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
    #[iri("fields:action_extracted")]
    ActionExtracted,
    #[iri("fields:extraction_method")]
    ExtractionMethod,
    #[iri("fields:number_of_extracts_pooled")]
    NumberOfExtractsPooled,
}


#[derive(Debug)]
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
    ActionExtracted(String),
    ExtractionMethod(String),
    NumberOfExtractsPooled(String),
}


impl TryFrom<(Extraction, Literal)> for ExtractionField {
    type Error = Error;

    fn try_from(source: (Extraction, Literal)) -> Result<Self, Self::Error> {
        let (field, lit) = source;
        let result = match (field, lit) {
            (Extraction::SubsampleId, Literal::String(value)) => Self::SubsampleId(value),
            (Extraction::ExtractId, Literal::String(value)) => Self::ExtractId(value),
            (Extraction::ExtractedBy, Literal::String(value)) => Self::ExtractedBy(value),
            (Extraction::ExtractedByOrcid, Literal::String(value)) => Self::ExtractedByOrcid(value),
            (Extraction::ExtractionDate, Literal::String(value)) => Self::ExtractionDate(value),
            (Extraction::NucleicAcidType, Literal::String(value)) => Self::NucleicAcidType(value),
            (Extraction::NucleicAcidConformation, Literal::String(value)) => Self::NucleicAcidConformation(value),
            (Extraction::NucleicAcidPreservationMethod, Literal::String(value)) => {
                Self::NucleicAcidPreservationMethod(value)
            }
            (Extraction::NucleicAcidConcentration, Literal::String(value)) => Self::NucleicAcidConcentration(value),
            (Extraction::NucleicAcidQuantification, Literal::String(value)) => Self::NucleicAcidQuantification(value),
            (Extraction::ConcentrationUnit, Literal::String(value)) => Self::ConcentrationUnit(value),
            (Extraction::Absorbance260230Ratio, Literal::String(value)) => Self::Absorbance260230Ratio(value),
            (Extraction::Absorbance260280Ratio, Literal::String(value)) => Self::Absorbance260280Ratio(value),
            (Extraction::CellLysisMethod, Literal::String(value)) => Self::CellLysisMethod(value),
            (Extraction::MaterialExtractedBy, Literal::String(value)) => Self::MaterialExtractedBy(value),
            (Extraction::ActionExtracted, Literal::String(value)) => Self::ActionExtracted(value),
            (Extraction::ExtractionMethod, Literal::String(value)) => Self::ExtractionMethod(value),
            (Extraction::NumberOfExtractsPooled, Literal::String(value)) => Self::NumberOfExtractsPooled(value),
        };

        Ok(result)
    }
}

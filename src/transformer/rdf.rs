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

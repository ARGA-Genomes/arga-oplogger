use std::borrow::Borrow;

use iref_enum::IriEnum;
use sophia::api::term::{SimpleTerm, Term};

use crate::errors::TransformError;


#[derive(Debug, IriEnum)]
#[iri_prefix("type" = "http://www.w3.org/2001/XMLSchema#")]
pub enum DataTypes {
    #[iri("type:string")]
    String,
    #[iri("type:boolean")]
    Boolean,
    #[iri("type:decimal")]
    Decimal,
    #[iri("type:integer")]
    Integer,
}


#[derive(Debug, Clone)]
pub enum Value {
    Iri(String),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    String(String),
    UInt64(u64),
}

impl TryFrom<&SimpleTerm<'static>> for Literal {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        match value {
            SimpleTerm::LiteralDatatype(val, type_iri) => match try_from_iri(type_iri)? {
                DataTypes::String => Ok(Literal::String(val.to_string())),
                DataTypes::Boolean => todo!(),
                DataTypes::Decimal => todo!(),
                DataTypes::Integer => todo!(),
            },
            _ => Err(TransformError::MissingEntityId),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("mapping" = "http://arga.org.au/schemas/mapping/")]
pub enum Mapping {
    /// The subject and object IRIs reflect the same definition
    /// and can be copied across without transformation.
    #[iri("mapping:same")]
    Same,

    /// The subject is the combination of the object IRIs separated by a space.
    /// If a value doesn't not exist it will be elided maintaining a single space
    /// between all values referenced by the IRI.
    #[iri("mapping:combines")]
    Combines,

    /// The subject is the value of the object after it is
    /// hashed with the xxh3 algorithm to become a content derived hash.
    #[iri("mapping:hash")]
    Hash,

    /// The subject is the value of the first IRI in the object list
    /// that has a value after it is hashed to become a content derived hash.
    #[iri("mapping:hash_first")]
    HashFirst,

    #[iri("mapping:when")]
    When,

    #[iri("mapping:from")]
    From,
}

impl TryFrom<&SimpleTerm<'static>> for Mapping {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        let mapping = try_from_term(&value)?;
        Ok(mapping)
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("mapping" = "http://arga.org.au/schemas/mapping/")]
pub enum MappingCondition {
    #[iri("mapping:is")]
    Is,
}

impl TryFrom<&SimpleTerm<'static>> for MappingCondition {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        let mapping = try_from_term(&value)?;
        Ok(mapping)
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("mapping" = "http://arga.org.au/schemas/mapping/")]
pub enum FromCondition {
    #[iri("mapping:via")]
    Via,
}

impl TryFrom<&SimpleTerm<'static>> for FromCondition {
    type Error = TransformError;

    fn try_from(value: &SimpleTerm<'static>) -> Result<Self, Self::Error> {
        let mapping = try_from_term(&value)?;
        Ok(mapping)
    }
}


#[derive(Debug, Clone)]
pub enum Map {
    Same(iref::IriBuf),
    Combines(Vec<iref::IriBuf>),
    Hash(iref::IriBuf),
    HashFirst(Vec<iref::IriBuf>),
    When(iref::IriBuf, Condition),
    From { graph: iref::IriBuf, via: iref::IriBuf },
}


#[derive(Debug, Clone)]
pub enum Condition {
    Is(Literal),
}

impl Condition {
    pub fn check(&self, value: &Literal) -> bool {
        match self {
            Condition::Is(literal) => value.eq(literal),
        }
    }
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
pub enum Name {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:canonical_name")]
    CanonicalName,
    #[iri("fields:scientific_name")]
    ScientificName,
    #[iri("fields:scientific_name_authorship")]
    ScientificNameAuthorship,
}


#[derive(Debug, Clone)]
pub enum NameField {
    EntityId(String),
    CanonicalName(String),
    ScientificName(String),
    ScientificNameAuthorship(String),
}

impl From<(Name, Literal)> for NameField {
    fn from(source: (Name, Literal)) -> Self {
        match source {
            (Name::EntityId, Literal::String(value)) => Self::EntityId(value),
            (Name::CanonicalName, Literal::String(value)) => Self::CanonicalName(value),
            (Name::ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (Name::ScientificNameAuthorship, Literal::String(value)) => Self::ScientificNameAuthorship(value),
            _ => unimplemented!(),
        }
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
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Tissue {
    #[iri("fields:entity_id")]
    EntityId,
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
    EntityId(String),
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
            (Tissue::EntityId, Literal::String(value)) => Self::EntityId(value),
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
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Collecting {
    #[iri("fields:entity_id")]
    EntityId,
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

    #[iri("fields:canonical_name")]
    CanonicalName,
    #[iri("fields:scientific_name_authorship")]
    ScientificNameAuthorship,
}


#[derive(Debug, Clone)]
pub enum CollectingField {
    EntityId(String),
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

    CanonicalName(String),
    ScientificNameAuthorship(String),
}


impl From<(Collecting, Literal)> for CollectingField {
    fn from(source: (Collecting, Literal)) -> Self {
        match source {
            (Collecting::EntityId, Literal::String(value)) => Self::EntityId(value),
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

            (Collecting::CanonicalName, Literal::String(value)) => Self::CanonicalName(value),
            (Collecting::ScientificNameAuthorship, Literal::String(value)) => Self::ScientificNameAuthorship(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Organism {
    #[iri("fields:entity_id")]
    EntityId,
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

    #[iri("fields:identified_by")]
    IdentifiedBy,
    #[iri("fields:identification_date")]
    IdentificationDate,
    #[iri("fields:disposition")]
    Disposition,
    #[iri("fields:first_observed_at")]
    FirstObservedAt,
    #[iri("fields:last_known_alive_at")]
    LastKnownAliveAt,

    #[iri("fields:biome")]
    Biome,
    #[iri("fields:habitat")]
    Habitat,
    #[iri("fields:bioregion")]
    Bioregion,
    #[iri("fields:ibra_imcra")]
    IbraImcra,

    #[iri("fields:latitude")]
    Latitude,
    #[iri("fields:longitude")]
    Longitude,
    #[iri("fields:coordinate_system")]
    CoordinateSystem,
    #[iri("fields:location_source")]
    LocationSource,
    #[iri("fields:holding")]
    Holding,
    #[iri("fields:holding_id")]
    HoldingId,
    #[iri("fields:holding_permit")]
    HoldingPermit,

    #[iri("fields:doi")]
    Doi,
    #[iri("fields:citation")]
    Citation,

    #[iri("fields:curator")]
    Curator,
    #[iri("fields:curator_orcid")]
    CuratorOrcid,
    #[iri("fields:created_at")]
    CreatedAt,
    #[iri("fields:updated_at")]
    UpdatedAt,

    #[iri("fields:publication_entity_id")]
    PublicationEntityId,
    #[iri("fields:canonical_name")]
    CanonicalName,
    #[iri("fields:scientific_name_authorship")]
    ScientificNameAuthorship,
}


#[derive(Debug, Clone)]
pub enum OrganismField {
    EntityId(String),
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
    IdentifiedBy(String),
    IdentificationDate(String),
    Disposition(String),
    FirstObservedAt(String),
    LastKnownAliveAt(String),
    Biome(String),
    Habitat(String),
    Bioregion(String),
    IbraImcra(String),
    Latitude(String),
    Longitude(String),
    CoordinateSystem(String),
    LocationSource(String),
    Holding(String),
    HoldingId(String),
    HoldingPermit(String),
    Doi(String),
    Citation(String),
    Curator(String),
    CuratorOrcid(String),
    CreatedAt(String),
    UpdatedAt(String),

    PublicationEntityId(String),
    CanonicalName(String),
    ScientificNameAuthorship(String),
}


impl From<(Organism, Literal)> for OrganismField {
    fn from(source: (Organism, Literal)) -> Self {
        match source {
            (Organism::EntityId, Literal::String(value)) => Self::EntityId(value),
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
            (Organism::IdentifiedBy, Literal::String(value)) => Self::IdentifiedBy(value),
            (Organism::IdentificationDate, Literal::String(value)) => Self::IdentificationDate(value),
            (Organism::Disposition, Literal::String(value)) => Self::Disposition(value),
            (Organism::FirstObservedAt, Literal::String(value)) => Self::FirstObservedAt(value),
            (Organism::LastKnownAliveAt, Literal::String(value)) => Self::LastKnownAliveAt(value),
            (Organism::Biome, Literal::String(value)) => Self::Biome(value),
            (Organism::Habitat, Literal::String(value)) => Self::Habitat(value),
            (Organism::Bioregion, Literal::String(value)) => Self::Bioregion(value),
            (Organism::IbraImcra, Literal::String(value)) => Self::IbraImcra(value),
            (Organism::Latitude, Literal::String(value)) => Self::Latitude(value),
            (Organism::Longitude, Literal::String(value)) => Self::Longitude(value),
            (Organism::CoordinateSystem, Literal::String(value)) => Self::CoordinateSystem(value),
            (Organism::LocationSource, Literal::String(value)) => Self::LocationSource(value),
            (Organism::Holding, Literal::String(value)) => Self::Holding(value),
            (Organism::HoldingId, Literal::String(value)) => Self::HoldingId(value),
            (Organism::HoldingPermit, Literal::String(value)) => Self::HoldingPermit(value),
            (Organism::Doi, Literal::String(value)) => Self::Doi(value),
            (Organism::Citation, Literal::String(value)) => Self::Citation(value),
            (Organism::Curator, Literal::String(value)) => Self::Curator(value),
            (Organism::CuratorOrcid, Literal::String(value)) => Self::CuratorOrcid(value),
            (Organism::CreatedAt, Literal::String(value)) => Self::CreatedAt(value),
            (Organism::UpdatedAt, Literal::String(value)) => Self::UpdatedAt(value),

            (Organism::PublicationEntityId, Literal::String(value)) => Self::PublicationEntityId(value),
            (Organism::CanonicalName, Literal::String(value)) => Self::CanonicalName(value),
            (Organism::ScientificNameAuthorship, Literal::String(value)) => Self::ScientificNameAuthorship(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Subsample {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:specimen_id")]
    SpecimenId,
    #[iri("fields:material_sample_id")]
    MaterialSampleId,
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
    EntityId(String),
    SpecimenId(String),
    MaterialSampleId(String),
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
            (Subsample::EntityId, Literal::String(value)) => Self::EntityId(value),
            (Subsample::SpecimenId, Literal::String(value)) => Self::SpecimenId(value),
            (Subsample::MaterialSampleId, Literal::String(value)) => Self::MaterialSampleId(value),
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
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Extraction {
    #[iri("fields:entity_id")]
    EntityId,
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
    #[iri("fields:doi")]
    Doi,
    #[iri("fields:citation")]
    Citation,

    #[iri("fields:extracted_by_entity_id")]
    ExtractedByEntityId,
    #[iri("fields:material_extracted_by_entity_id")]
    MaterialExtractedByEntityId,
    #[iri("fields:publication_entity_id")]
    PublicationEntityId,
}


#[derive(Debug, Clone)]
pub enum ExtractionField {
    EntityId(String),
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
    Doi(String),
    Citation(String),

    ExtractedByEntityId(String),
    MaterialExtractedByEntityId(String),
    PublicationEntityId(String),
}


impl From<(Extraction, Literal)> for ExtractionField {
    fn from(source: (Extraction, Literal)) -> Self {
        use Extraction::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
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
            (Doi, Literal::String(value)) => Self::Doi(value),
            (Citation, Literal::String(value)) => Self::Citation(value),

            (ExtractedByEntityId, Literal::String(value)) => Self::ExtractedByEntityId(value),
            (MaterialExtractedByEntityId, Literal::String(value)) => Self::MaterialExtractedByEntityId(value),
            (PublicationEntityId, Literal::String(value)) => Self::PublicationEntityId(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Library {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:extract_id")]
    ExtractId,
    #[iri("fields:library_id")]
    LibraryId,
    #[iri("fields:scientific_name")]
    ScientificName,

    #[iri("fields:event_date")]
    EventDate,
    #[iri("fields:concentration")]
    Concentration,
    #[iri("fields:concentration_unit")]
    ConcentrationUnit,
    #[iri("fields:pcr_cycles")]
    PcrCycles,
    #[iri("fields:layout")]
    Layout,
    #[iri("fields:prepared_by")]
    PreparedBy,
    #[iri("fields:selection")]
    Selection,
    #[iri("fields:bait_set_name")]
    BaitSetName,
    #[iri("fields:bait_set_reference")]
    BaitSetReference,
    #[iri("fields:construction_protocol")]
    ConstructionProtocol,
    #[iri("fields:source")]
    Source,
    #[iri("fields:insert_size")]
    InsertSize,
    #[iri("fields:design_description")]
    DesignDescription,
    #[iri("fields:strategy")]
    Strategy,
    #[iri("fields:index_tag")]
    IndexTag,
    #[iri("fields:index_dual_tag")]
    IndexDualTag,
    #[iri("fields:index_oligo")]
    IndexOligo,
    #[iri("fields:index_dual_oligo")]
    IndexDualOligo,
    #[iri("fields:location")]
    Location,
    #[iri("fields:remarks")]
    Remarks,
    #[iri("fields:dna_treatment")]
    DnaTreatment,
    #[iri("fields:number_of_libraries_pooled")]
    NumberOfLibrariesPooled,
    #[iri("fields:pcr_replicates")]
    PcrReplicates,

    #[iri("fields:prepared_by_entity_id")]
    PreparedByEntityId,
    #[iri("fields:canonical_name")]
    CanonicalName,
    #[iri("fields:scientific_name_authorship")]
    ScientificNameAuthorship,
}


#[derive(Debug, Clone)]
pub enum LibraryField {
    EntityId(String),
    ExtractId(String),
    LibraryId(String),
    ScientificName(String),
    EventDate(String),
    Concentration(String),
    ConcentrationUnit(String),
    PcrCycles(String),
    Layout(String),
    PreparedBy(String),
    Selection(String),
    BaitSetName(String),
    BaitSetReference(String),
    ConstructionProtocol(String),
    Source(String),
    InsertSize(String),
    DesignDescription(String),
    Strategy(String),
    IndexTag(String),
    IndexDualTag(String),
    IndexOligo(String),
    IndexDualOligo(String),
    Location(String),
    Remarks(String),
    DnaTreatment(String),
    NumberOfLibrariesPooled(String),
    PcrReplicates(String),

    PreparedByEntityId(String),
    CanonicalName(String),
    ScientificNameAuthorship(String),
}


impl From<(Library, Literal)> for LibraryField {
    fn from(source: (Library, Literal)) -> Self {
        use Library::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (ExtractId, Literal::String(value)) => Self::ExtractId(value),
            (LibraryId, Literal::String(value)) => Self::LibraryId(value),
            (ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (EventDate, Literal::String(value)) => Self::EventDate(value),
            (Concentration, Literal::String(value)) => Self::Concentration(value),
            (ConcentrationUnit, Literal::String(value)) => Self::ConcentrationUnit(value),
            (PcrCycles, Literal::String(value)) => Self::PcrCycles(value),
            (Layout, Literal::String(value)) => Self::Layout(value),
            (PreparedBy, Literal::String(value)) => Self::PreparedBy(value),
            (Selection, Literal::String(value)) => Self::Selection(value),
            (BaitSetName, Literal::String(value)) => Self::BaitSetName(value),
            (BaitSetReference, Literal::String(value)) => Self::BaitSetReference(value),
            (ConstructionProtocol, Literal::String(value)) => Self::ConstructionProtocol(value),
            (Source, Literal::String(value)) => Self::Source(value),
            (InsertSize, Literal::String(value)) => Self::InsertSize(value),
            (DesignDescription, Literal::String(value)) => Self::DesignDescription(value),
            (Strategy, Literal::String(value)) => Self::Strategy(value),
            (IndexTag, Literal::String(value)) => Self::IndexTag(value),
            (IndexDualTag, Literal::String(value)) => Self::IndexDualTag(value),
            (IndexOligo, Literal::String(value)) => Self::IndexOligo(value),
            (IndexDualOligo, Literal::String(value)) => Self::IndexDualOligo(value),
            (Location, Literal::String(value)) => Self::Location(value),
            (Remarks, Literal::String(value)) => Self::Remarks(value),
            (DnaTreatment, Literal::String(value)) => Self::DnaTreatment(value),
            (NumberOfLibrariesPooled, Literal::String(value)) => Self::NumberOfLibrariesPooled(value),
            (PcrReplicates, Literal::String(value)) => Self::PcrReplicates(value),

            (PreparedByEntityId, Literal::String(value)) => Self::PreparedByEntityId(value),
            (CanonicalName, Literal::String(value)) => Self::CanonicalName(value),
            (ScientificNameAuthorship, Literal::String(value)) => Self::ScientificNameAuthorship(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum SequencingRun {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:library_id")]
    LibraryId,
    #[iri("fields:sequence_id")]
    SequenceId,
    #[iri("fields:facility")]
    Facility,
    #[iri("fields:event_date")]
    EventDate,
    #[iri("fields:instrument_or_method")]
    InstrumentOrMethod,
    #[iri("fields:sra_run_accession")]
    SraRunAccession,
    #[iri("fields:platform")]
    Platform,
    #[iri("fields:dataset_file_format")]
    DatasetFileFormat,
    #[iri("fields:kit_chemistry")]
    KitChemistry,
    #[iri("fields:flowcell_type")]
    FlowcellType,
    #[iri("fields:cell_movie_length")]
    CellMovieLength,
    #[iri("fields:base_caller_model")]
    BaseCallerModel,
    #[iri("fields:fast5_compression")]
    Fast5Compression,
    #[iri("fields:analysis_software")]
    AnalysisSoftware,
    #[iri("fields:analysis_software_version")]
    AnalysisSoftwareVersion,
    #[iri("fields:target_gene")]
    TargetGene,
}


#[derive(Debug, Clone)]
pub enum SequencingRunField {
    EntityId(String),
    LibraryId(String),
    SequenceId(String),
    Facility(String),
    EventDate(String),
    InstrumentOrMethod(String),
    SraRunAccession(String),
    Platform(String),
    DatasetFileFormat(String),
    KitChemistry(String),
    FlowcellType(String),
    CellMovieLength(String),
    BaseCallerModel(String),
    Fast5Compression(String),
    AnalysisSoftware(String),
    AnalysisSoftwareVersion(String),
    TargetGene(String),
}


impl From<(SequencingRun, Literal)> for SequencingRunField {
    fn from(source: (SequencingRun, Literal)) -> Self {
        use SequencingRun::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (LibraryId, Literal::String(value)) => Self::LibraryId(value),
            (SequenceId, Literal::String(value)) => Self::SequenceId(value),
            (Facility, Literal::String(value)) => Self::Facility(value),
            (EventDate, Literal::String(value)) => Self::EventDate(value),
            (InstrumentOrMethod, Literal::String(value)) => Self::InstrumentOrMethod(value),
            (SraRunAccession, Literal::String(value)) => Self::SraRunAccession(value),
            (Platform, Literal::String(value)) => Self::Platform(value),
            (DatasetFileFormat, Literal::String(value)) => Self::DatasetFileFormat(value),
            (KitChemistry, Literal::String(value)) => Self::KitChemistry(value),
            (FlowcellType, Literal::String(value)) => Self::FlowcellType(value),
            (CellMovieLength, Literal::String(value)) => Self::CellMovieLength(value),
            (BaseCallerModel, Literal::String(value)) => Self::BaseCallerModel(value),
            (Fast5Compression, Literal::String(value)) => Self::Fast5Compression(value),
            (AnalysisSoftware, Literal::String(value)) => Self::AnalysisSoftware(value),
            (AnalysisSoftwareVersion, Literal::String(value)) => Self::AnalysisSoftwareVersion(value),
            (TargetGene, Literal::String(value)) => Self::TargetGene(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Assembly {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:library_id")]
    LibraryId,
    #[iri("fields:assembly_id")]
    AssemblyId,
    #[iri("fields:scientific_name")]
    ScientificName,
    #[iri("fields:event_date")]
    EventDate,
    #[iri("fields:name")]
    Name,
    #[iri("fields:type")]
    Type,
    #[iri("fields:method")]
    Method,
    #[iri("fields:method_version")]
    MethodVersion,
    #[iri("fields:method_link")]
    MethodLink,
    #[iri("fields:size")]
    Size,
    #[iri("fields:size_ungapped")]
    SizeUngapped,
    #[iri("fields:minimum_gap_length")]
    MinimumGapLength,
    #[iri("fields:completeness")]
    Completeness,
    #[iri("fields:completeness_method")]
    CompletenessMethod,
    #[iri("fields:source_molecule")]
    SourceMolecule,
    #[iri("fields:reference_genome_used")]
    ReferenceGenomeUsed,
    #[iri("fields:reference_genome_link")]
    ReferenceGenomeLink,
    #[iri("fields:number_of_scaffolds")]
    NumberOfScaffolds,
    #[iri("fields:number_of_contigs")]
    NumberOfContigs,
    #[iri("fields:number_of_chromosomes")]
    NumberOfChromosomes,
    #[iri("fields:number_of_component_sequences")]
    NumberOfComponentSequences,
    #[iri("fields:number_of_organelles")]
    NumberOfOrganelles,
    #[iri("fields:number_of_gaps_between_scaffolds")]
    NumberOfGapsBetweenScaffolds,
    #[iri("fields:number_of_atgc")]
    NumberOfATGC,
    #[iri("fields:number_of_guanine_cytosine")]
    NumberOfGuanineCytosine,
    #[iri("fields:guanine_cytosine_percent")]
    GuanineCytosinePercent,
    #[iri("fields:genome_coverage")]
    GenomeCoverage,
    #[iri("fields:hybrid")]
    Hybrid,
    #[iri("fields:hybrid_information")]
    HybridInformation,
    #[iri("fields:polishing_or_scaffolding_method")]
    PolishingOrScaffoldingMethod,
    #[iri("fields:polishing_or_scaffolding_data")]
    PolishingOrScaffoldingData,
    #[iri("fields:computational_infrastructure")]
    ComputationalInfrastructure,
    #[iri("fields:system_used")]
    SystemUsed,

    #[iri("fields:level")]
    Level,
    #[iri("fields:representation")]
    Representation,

    #[iri("fields:assembly_n50")]
    AssemblyN50,
    #[iri("fields:contig_n50")]
    ContigN50,
    #[iri("fields:contig_l50")]
    ContigL50,
    #[iri("fields:scaffold_n50")]
    ScaffoldN50,
    #[iri("fields:scaffold_l50")]
    ScaffoldL50,

    #[iri("fields:longest_contig")]
    LongestContig,
    #[iri("fields:longest_scaffold")]
    LongestScaffold,
    #[iri("fields:total_contig_size")]
    TotalContigSize,
    #[iri("fields:total_scaffold_size")]
    TotalScaffoldSize,

    #[iri("fields:canonical_name")]
    CanonicalName,
    #[iri("fields:scientific_name_authorship")]
    ScientificNameAuthorship,

    #[iri("fields:taxon_id")]
    TaxonId,
}


#[derive(Debug, Clone)]
pub enum AssemblyField {
    EntityId(String),
    LibraryId(String),
    AssemblyId(String),
    ScientificName(String),
    EventDate(String),
    Name(String),
    Type(String),
    Method(String),
    MethodVersion(String),
    MethodLink(String),
    Size(u64),
    SizeUngapped(u64),
    MinimumGapLength(String),
    Completeness(String),
    CompletenessMethod(String),
    SourceMolecule(String),
    ReferenceGenomeUsed(String),
    ReferenceGenomeLink(String),
    Hybrid(String),
    HybridInformation(String),
    PolishingOrScaffoldingMethod(String),
    PolishingOrScaffoldingData(String),
    ComputationalInfrastructure(String),
    SystemUsed(String),
    Level(String),
    Representation(String),

    NumberOfScaffolds(u64),
    NumberOfContigs(u64),
    NumberOfChromosomes(u64),
    NumberOfComponentSequences(u64),
    NumberOfOrganelles(u64),
    NumberOfGapsBetweenScaffolds(u64),
    NumberOfATGC(u64),
    NumberOfGuanineCytosine(u64),
    GuanineCytosinePercent(u64),
    GenomeCoverage(String),
    AssemblyN50(String),
    ContigN50(u64),
    ContigL50(u64),
    ScaffoldN50(u64),
    ScaffoldL50(u64),

    LongestContig(u64),
    LongestScaffold(u64),
    TotalContigSize(u64),
    TotalScaffoldSize(u64),

    CanonicalName(String),
    ScientificNameAuthorship(String),
    TaxonId(String),
}


impl From<(Assembly, Literal)> for AssemblyField {
    fn from(source: (Assembly, Literal)) -> Self {
        use Assembly::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (LibraryId, Literal::String(value)) => Self::LibraryId(value),
            (AssemblyId, Literal::String(value)) => Self::AssemblyId(value),
            (ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (EventDate, Literal::String(value)) => Self::EventDate(value),
            (Name, Literal::String(value)) => Self::Name(value),
            (Type, Literal::String(value)) => Self::Type(value),
            (Method, Literal::String(value)) => Self::Method(value),
            (MethodVersion, Literal::String(value)) => Self::MethodVersion(value),
            (MethodLink, Literal::String(value)) => Self::MethodLink(value),
            (Size, Literal::UInt64(value)) => Self::Size(value),
            (Size, Literal::String(value)) => Self::Size(str_to_u64(&value).unwrap()),
            (SizeUngapped, Literal::UInt64(value)) => Self::SizeUngapped(value),
            (SizeUngapped, Literal::String(value)) => Self::SizeUngapped(str_to_u64(&value).unwrap()),
            (MinimumGapLength, Literal::String(value)) => Self::MinimumGapLength(value),
            (Completeness, Literal::String(value)) => Self::Completeness(value),
            (CompletenessMethod, Literal::String(value)) => Self::CompletenessMethod(value),
            (SourceMolecule, Literal::String(value)) => Self::SourceMolecule(value),
            (ReferenceGenomeUsed, Literal::String(value)) => Self::ReferenceGenomeUsed(value),
            (ReferenceGenomeLink, Literal::String(value)) => Self::ReferenceGenomeLink(value),
            (NumberOfScaffolds, Literal::UInt64(value)) => Self::NumberOfScaffolds(value),
            (NumberOfScaffolds, Literal::String(value)) => Self::NumberOfScaffolds(str_to_u64(&value).unwrap()),
            (NumberOfContigs, Literal::UInt64(value)) => Self::NumberOfContigs(value),
            (NumberOfContigs, Literal::String(value)) => Self::NumberOfContigs(str_to_u64(&value).unwrap()),
            (NumberOfChromosomes, Literal::UInt64(value)) => Self::NumberOfChromosomes(value),
            (NumberOfChromosomes, Literal::String(value)) => Self::NumberOfChromosomes(str_to_u64(&value).unwrap()),
            (NumberOfComponentSequences, Literal::UInt64(value)) => Self::NumberOfComponentSequences(value),
            (NumberOfComponentSequences, Literal::String(value)) => {
                Self::NumberOfComponentSequences(str_to_u64(&value).unwrap())
            }
            (NumberOfOrganelles, Literal::UInt64(value)) => Self::NumberOfOrganelles(value),
            (NumberOfOrganelles, Literal::String(value)) => Self::NumberOfOrganelles(str_to_u64(&value).unwrap()),
            (NumberOfGapsBetweenScaffolds, Literal::UInt64(value)) => Self::NumberOfGapsBetweenScaffolds(value),
            (NumberOfGapsBetweenScaffolds, Literal::String(value)) => {
                Self::NumberOfGapsBetweenScaffolds(str_to_u64(&value).unwrap())
            }
            (NumberOfATGC, Literal::UInt64(value)) => Self::NumberOfATGC(value),
            (NumberOfATGC, Literal::String(value)) => Self::NumberOfATGC(str_to_u64(&value).unwrap()),
            (NumberOfGuanineCytosine, Literal::UInt64(value)) => Self::NumberOfGuanineCytosine(value),
            (NumberOfGuanineCytosine, Literal::String(value)) => {
                Self::NumberOfGuanineCytosine(str_to_u64(&value).unwrap())
            }
            (GuanineCytosinePercent, Literal::UInt64(value)) => Self::GuanineCytosinePercent(value),
            (GuanineCytosinePercent, Literal::String(value)) => match str_to_f32(&value) {
                Ok(val) => Self::GuanineCytosinePercent(val.round() as u64),
                Err(_) => Self::GuanineCytosinePercent(str_to_u64(&value).unwrap()),
            },
            (GenomeCoverage, Literal::String(value)) => Self::GenomeCoverage(value),
            (Hybrid, Literal::String(value)) => Self::Hybrid(value),
            (HybridInformation, Literal::String(value)) => Self::HybridInformation(value),
            (PolishingOrScaffoldingMethod, Literal::String(value)) => Self::PolishingOrScaffoldingMethod(value),
            (PolishingOrScaffoldingData, Literal::String(value)) => Self::PolishingOrScaffoldingData(value),
            (ComputationalInfrastructure, Literal::String(value)) => Self::ComputationalInfrastructure(value),
            (SystemUsed, Literal::String(value)) => Self::SystemUsed(value),
            (Level, Literal::String(value)) => Self::Level(value),
            (Representation, Literal::String(value)) => Self::Representation(value),

            (AssemblyN50, Literal::String(value)) => Self::AssemblyN50(value),
            (ContigN50, Literal::UInt64(value)) => Self::ContigN50(value),
            (ContigN50, Literal::String(value)) => Self::ContigN50(str_to_u64(&value).unwrap()),
            (ContigL50, Literal::UInt64(value)) => Self::ContigL50(value),
            (ContigL50, Literal::String(value)) => Self::ContigL50(str_to_u64(&value).unwrap()),
            (ScaffoldN50, Literal::UInt64(value)) => Self::ScaffoldN50(value),
            (ScaffoldN50, Literal::String(value)) => Self::ScaffoldN50(str_to_u64(&value).unwrap()),
            (ScaffoldL50, Literal::UInt64(value)) => Self::ScaffoldL50(value),
            (ScaffoldL50, Literal::String(value)) => Self::ScaffoldL50(str_to_u64(&value).unwrap()),

            (LongestContig, Literal::UInt64(value)) => Self::LongestContig(value),
            (LongestContig, Literal::String(value)) => Self::LongestContig(str_to_u64(&value).unwrap()),
            (LongestScaffold, Literal::UInt64(value)) => Self::LongestScaffold(value),
            (LongestScaffold, Literal::String(value)) => Self::LongestScaffold(str_to_u64(&value).unwrap()),
            (TotalContigSize, Literal::UInt64(value)) => Self::TotalContigSize(value),
            (TotalContigSize, Literal::String(value)) => Self::TotalContigSize(str_to_u64(&value).unwrap()),
            (TotalScaffoldSize, Literal::UInt64(value)) => Self::TotalScaffoldSize(value),
            (TotalScaffoldSize, Literal::String(value)) => Self::TotalScaffoldSize(str_to_u64(&value).unwrap()),

            (CanonicalName, Literal::String(value)) => Self::CanonicalName(value),
            (ScientificNameAuthorship, Literal::String(value)) => Self::ScientificNameAuthorship(value),
            (TaxonId, Literal::String(value)) => Self::TaxonId(value),
            (field, val) => {
                tracing::error!(?field, ?val, "unsupported field format");
                unimplemented!()
            }
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum DataProduct {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:organism_id")]
    OrganismId,
    #[iri("fields:extract_id")]
    ExtractId,
    #[iri("fields:sequence_run_id")]
    SequenceRunId,

    #[iri("fields:sequence_sample_id")]
    SequenceSampleId,
    #[iri("fields:sequence_analysis_id")]
    SequenceAnalysisId,
    #[iri("fields:notes")]
    Notes,
    #[iri("fields:context")]
    Context,
    #[iri("fields:type")]
    Type,
    #[iri("fields:file_type")]
    FileType,
    #[iri("fields:url")]
    Url,
    #[iri("fields:licence")]
    Licence,
    #[iri("fields:access")]
    Access,
    #[iri("fields:custodian")]
    Custodian,
    #[iri("fields:custodian_orcid")]
    CustodianOrcid,
    #[iri("fields:citation")]
    Citation,
    #[iri("fields:source_url")]
    SourceUrl,

    #[iri("fields:custodian_entity_id")]
    CustodianEntityId,
    #[iri("fields:publication_entity_id")]
    PublicationEntityId,
}


#[derive(Debug, Clone)]
pub enum DataProductField {
    EntityId(String),
    OrganismId(String),
    ExtractId(String),
    SequenceRunId(String),

    SequenceSampleId(String),
    SequenceAnalysisId(String),
    Notes(String),
    Context(String),
    Type(String),
    FileType(String),
    Url(String),
    Licence(String),
    Access(String),
    Custodian(String),
    CustodianOrcid(String),
    Citation(String),
    SourceUrl(String),

    CustodianEntityId(String),
    PublicationEntityId(String),
}


impl From<(DataProduct, Literal)> for DataProductField {
    fn from(source: (DataProduct, Literal)) -> Self {
        use DataProduct::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (OrganismId, Literal::String(value)) => Self::OrganismId(value),
            (ExtractId, Literal::String(value)) => Self::ExtractId(value),
            (SequenceRunId, Literal::String(value)) => Self::SequenceRunId(value),
            (SequenceSampleId, Literal::String(value)) => Self::SequenceSampleId(value),
            (SequenceAnalysisId, Literal::String(value)) => Self::SequenceAnalysisId(value),
            (Notes, Literal::String(value)) => Self::Notes(value),
            (Context, Literal::String(value)) => Self::Context(value),
            (Type, Literal::String(value)) => Self::Type(value),
            (FileType, Literal::String(value)) => Self::FileType(value),
            (Url, Literal::String(value)) => Self::Url(value),
            (Licence, Literal::String(value)) => Self::Licence(value),
            (Access, Literal::String(value)) => Self::Access(value),
            (Custodian, Literal::String(value)) => Self::Custodian(value),
            (CustodianOrcid, Literal::String(value)) => Self::CustodianOrcid(value),
            (Citation, Literal::String(value)) => Self::Citation(value),
            (SourceUrl, Literal::String(value)) => Self::SourceUrl(value),
            (CustodianEntityId, Literal::String(value)) => Self::CustodianEntityId(value),
            (PublicationEntityId, Literal::String(value)) => Self::PublicationEntityId(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Annotation {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:assembly_id")]
    AssemblyId,

    #[iri("fields:name")]
    Name,
    #[iri("fields:provider")]
    Provider,
    #[iri("fields:method")]
    Method,
    #[iri("fields:type")]
    Type,
    #[iri("fields:version")]
    Version,
    #[iri("fields:software")]
    Software,
    #[iri("fields:software_version")]
    SoftwareVersion,
    #[iri("fields:event_date")]
    EventDate,

    #[iri("fields:number_of_genes")]
    NumberOfGenes,
    #[iri("fields:number_of_coding_proteins")]
    NumberOfCodingProteins,
    #[iri("fields:number_of_non_coding_proteins")]
    NumberOfNonCodingProteins,
    #[iri("fields:number_of_pseudogenes")]
    NumberOfPseudogenes,
    #[iri("fields:number_of_other_genes")]
    NumberOfOtherGenes,
}


#[derive(Debug, Clone)]
pub enum AnnotationField {
    EntityId(String),
    AssemblyId(String),

    Name(String),
    Provider(String),
    Method(String),
    Type(String),
    Version(String),
    Software(String),
    SoftwareVersion(String),
    EventDate(String),

    NumberOfGenes(u64),
    NumberOfCodingProteins(u64),
    NumberOfNonCodingProteins(u64),
    NumberOfPseudogenes(u64),
    NumberOfOtherGenes(u64),
}


impl From<(Annotation, Literal)> for AnnotationField {
    fn from(source: (Annotation, Literal)) -> Self {
        use Annotation::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (AssemblyId, Literal::String(value)) => Self::AssemblyId(value),
            (Name, Literal::String(value)) => Self::Name(value),
            (Provider, Literal::String(value)) => Self::Provider(value),
            (Method, Literal::String(value)) => Self::Method(value),
            (Type, Literal::String(value)) => Self::Type(value),
            (Version, Literal::String(value)) => Self::Version(value),
            (Software, Literal::String(value)) => Self::Software(value),
            (SoftwareVersion, Literal::String(value)) => Self::SoftwareVersion(value),
            (EventDate, Literal::String(value)) => Self::EventDate(value),
            (NumberOfGenes, Literal::UInt64(value)) => Self::NumberOfGenes(value),
            (NumberOfGenes, Literal::String(value)) => Self::NumberOfGenes(str_to_u64(&value).unwrap_or_default()),
            (NumberOfCodingProteins, Literal::UInt64(value)) => Self::NumberOfCodingProteins(value),
            (NumberOfCodingProteins, Literal::String(value)) => {
                Self::NumberOfCodingProteins(str_to_u64(&value).unwrap_or_default())
            }
            (NumberOfNonCodingProteins, Literal::UInt64(value)) => Self::NumberOfNonCodingProteins(value),
            (NumberOfNonCodingProteins, Literal::String(value)) => {
                Self::NumberOfNonCodingProteins(str_to_u64(&value).unwrap_or_default())
            }
            (NumberOfPseudogenes, Literal::UInt64(value)) => Self::NumberOfPseudogenes(value),
            (NumberOfPseudogenes, Literal::String(value)) => {
                Self::NumberOfPseudogenes(str_to_u64(&value).unwrap_or_default())
            }
            (NumberOfOtherGenes, Literal::UInt64(value)) => Self::NumberOfOtherGenes(value),
            (NumberOfOtherGenes, Literal::String(value)) => {
                Self::NumberOfOtherGenes(str_to_u64(&value).unwrap_or_default())
            }
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Deposition {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:assembly_id")]
    AssemblyId,

    #[iri("fields:event_date")]
    EventDate,
    #[iri("fields:url")]
    Url,
    #[iri("fields:institution")]
    Institution,
}


#[derive(Debug, Clone)]
pub enum DepositionField {
    EntityId(String),
    AssemblyId(String),

    EventDate(String),
    Url(String),
    Institution(String),
}


impl From<(Deposition, Literal)> for DepositionField {
    fn from(source: (Deposition, Literal)) -> Self {
        use Deposition::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (AssemblyId, Literal::String(value)) => Self::AssemblyId(value),
            (EventDate, Literal::String(value)) => Self::EventDate(value),
            (Url, Literal::String(value)) => Self::Url(value),
            (Institution, Literal::String(value)) => Self::Institution(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum Project {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:project_id")]
    ProjectId,

    #[iri("fields:scientific_name")]
    ScientificName,
    #[iri("fields:initiative")]
    Initiative,
    #[iri("fields:initiative_theme")]
    InitiativeTheme,
    #[iri("fields:title")]
    Title,
    #[iri("fields:description")]
    Description,
    #[iri("fields:data_context")]
    DataContext,
    #[iri("fields:data_types")]
    DataTypes,
    #[iri("fields:data_assay_types")]
    DataAssayTypes,
    #[iri("fields:partners")]
    Partners,

    #[iri("fields:curator")]
    Curator,
    #[iri("fields:curator_orcid")]
    CuratorOrcid,
}


#[derive(Debug, Clone)]
pub enum ProjectField {
    EntityId(String),
    ProjectId(String),

    ScientificName(String),
    Initiative(String),
    InitiativeTheme(String),
    Title(String),
    Description(String),
    DataContext(String),
    DataTypes(String),
    DataAssayTypes(String),
    Partners(String),

    Curator(String),
    CuratorOrcid(String),
}


impl From<(Project, Literal)> for ProjectField {
    fn from(source: (Project, Literal)) -> Self {
        use Project::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (ProjectId, Literal::String(value)) => Self::ProjectId(value),
            (ScientificName, Literal::String(value)) => Self::ScientificName(value),
            (Initiative, Literal::String(value)) => Self::Initiative(value),
            (InitiativeTheme, Literal::String(value)) => Self::InitiativeTheme(value),
            (Title, Literal::String(value)) => Self::Title(value),
            (Description, Literal::String(value)) => Self::Description(value),
            (DataContext, Literal::String(value)) => Self::DataContext(value),
            (DataTypes, Literal::String(value)) => Self::DataTypes(value),
            (DataAssayTypes, Literal::String(value)) => Self::DataAssayTypes(value),
            (Partners, Literal::String(value)) => Self::Partners(value),
            (Curator, Literal::String(value)) => Self::Curator(value),
            (CuratorOrcid, Literal::String(value)) => Self::CuratorOrcid(value),
            _ => unimplemented!(),
        }
    }
}


#[derive(Debug, IriEnum)]
#[iri_prefix("fields" = "http://arga.org.au/schemas/fields/")]
pub enum ProjectMember {
    #[iri("fields:entity_id")]
    EntityId,
    #[iri("fields:project_id")]
    ProjectId,
    #[iri("fields:name")]
    Name,
    #[iri("fields:orcid")]
    Orcid,
    #[iri("fields:organisation")]
    Organisation,
}


#[derive(Debug, Clone)]
pub enum ProjectMemberField {
    EntityId(String),
    ProjectId(String),
    Name(String),
    Orcid(String),
    Organisation(String),
}


impl From<(ProjectMember, Literal)> for ProjectMemberField {
    fn from(source: (ProjectMember, Literal)) -> Self {
        use ProjectMember::*;
        match source {
            (EntityId, Literal::String(value)) => Self::EntityId(value),
            (ProjectId, Literal::String(value)) => Self::ProjectId(value),
            (Name, Literal::String(value)) => Self::Name(value),
            (Orcid, Literal::String(value)) => Self::Orcid(value),
            (Organisation, Literal::String(value)) => Self::Organisation(value),
            _ => unimplemented!(),
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


fn str_to_u64(value: &str) -> Result<u64, TransformError> {
    let scrubbed = value.replace(",", "");
    Ok(scrubbed.parse::<u64>()?)
}

fn str_to_f32(value: &str) -> Result<f32, TransformError> {
    let scrubbed = value.replace(",", "");
    Ok(scrubbed.parse::<f32>()?)
}

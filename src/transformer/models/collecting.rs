use std::collections::HashMap;

use tracing::instrument;

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, CollectingField, Literal};
use crate::transformer::resolver::Resolver;


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


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Collecting>, Error> {
    use rdf::Collecting::*;

    let models = dataset.scope(&["collecting"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let data: HashMap<Literal, Vec<CollectingField>> = resolver.resolve(
        &[
            EntityId,
            OrganismId,
            MaterialSampleId,
            FieldCollectingId,
            ScientificName,
            CollectedBy,
            Remarks,
            Preparation,
            Habitat,
            SpecificHost,
            IndividualCount,
            Strain,
            Isolate,
            Permit,
            SamplingProtocol,
            OrganismKilled,
            OrganismKillMethod,
            FieldSampleDisposition,
            FieldNotes,
            EnvironmentBroadScale,
            EnvironmentLocalScale,
            EnvironmentMedium,
            Locality,
            Country,
            CountryCode,
            StateProvince,
            County,
            Municipality,
            Latitude,
            Longitude,
            LocationGeneralisation,
            LocationSource,
            Elevation,
            ElevationAccuracy,
            Depth,
            DepthAccuracy,
            CanonicalName,
            ScientificNameAuthorship,
        ],
        &scope,
    )?;


    let mut records = Vec::new();

    for (_idx, fields) in data {
        let mut record = Collecting::default();

        for field in fields {
            match field {
                CollectingField::EntityId(val) => record.entity_id = val,
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

                CollectingField::CanonicalName(_) => {}
                CollectingField::ScientificNameAuthorship(_) => {}
            }
        }

        records.push(record);
    }

    Ok(records)
}


/// Get all scientific names.
#[instrument(skip_all)]
pub fn get_scientific_names(dataset: &Dataset) -> Result<HashMap<String, String>, Error> {
    let models = dataset.scope(&["collecting"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);


    let mut names = HashMap::new();

    let data: HashMap<Literal, Vec<CollectingField>> = resolver.resolve(
        &[
            rdf::Collecting::EntityId,
            rdf::Collecting::ScientificName,
            rdf::Collecting::CanonicalName,
            rdf::Collecting::ScientificNameAuthorship,
        ],
        &scope,
    )?;

    for (_idx, fields) in data.into_iter() {
        let mut entity_id = None;
        let mut scientific_name = None;

        for field in fields {
            match field {
                CollectingField::EntityId(val) => entity_id = Some(val),
                CollectingField::ScientificName(val) => scientific_name = Some(val),
                _ => {}
            }
        }

        if let (Some(entity_id), Some(scientific_name)) = (entity_id, scientific_name) {
            names.insert(entity_id, scientific_name);
        }
    }

    Ok(names)
}

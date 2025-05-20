use std::io::Read;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{self, CollectionEventAtom, CollectionEventOperation};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::{error, info, trace};

use crate::database::{name_lookup, organism_lookup, specimen_lookup, FrameLoader, StringMap};
use crate::errors::{Error, LookupError, ReduceError};
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type CollectionEventFrame = DataFrame<CollectionEventAtom>;


impl OperationLoader for FrameLoader<CollectionEventOperation> {
    type Operation = CollectionEventOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<CollectionEventOperation>, Error> {
        use schema::collection_event_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let ops = collection_event_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<CollectionEventOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[CollectionEventOperation]) -> Result<usize, Error> {
        use schema::collection_event_logs::dsl::*;
        let mut conn = self.pool.get()?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(collection_event_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        Ok(inserted)
    }
}


// A single row in a supported CSV file.
//
// For specimens this is conflated with a collection event, so we deserialize both
// in order to split them up into different operation logs down the line without having
// to reprocess the CSV file.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    entity_id: String,
    field_collecting_id: String,
    scientific_name: String,
    organism_id: String,
    specimen_id: Option<String>,

    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,

    collected_by: Option<String>,
    collection_remarks: Option<String>,

    identified_by: Option<String>,
    identified_date: Option<chrono::NaiveDate>,
    identification_remarks: Option<String>,

    // location block
    locality: Option<String>,
    country: Option<String>,
    country_code: Option<String>,
    state_province: Option<String>,
    county: Option<String>,
    municipality: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    elevation: Option<f64>,
    depth: Option<f64>,
    elevation_accuracy: Option<f64>,
    depth_accuracy: Option<f64>,
    location_source: Option<String>,

    preparation: Option<String>,
    environment_broad_scale: Option<String>,
    environment_local_scale: Option<String>,
    environment_medium: Option<String>,

    habitat: Option<String>,
    specific_host: Option<String>,

    individual_count: Option<String>,
    organism_quantity: Option<String>,
    organism_quantity_type: Option<String>,

    strain: Option<String>,
    isolate: Option<String>,
    field_notes: Option<String>,
}

impl IntoFrame for Record {
    type Atom = CollectionEventAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: CollectionEventFrame) -> CollectionEventFrame {
        use CollectionEventAtom::*;

        // identity
        frame.push(FieldCollectingId(self.field_collecting_id));
        frame.push(ScientificName(self.scientific_name));
        frame.push(OrganismId(self.organism_id));
        frame_push_opt!(frame, SpecimenId, self.specimen_id);

        // location
        frame_push_opt!(frame, Locality, self.locality);
        frame_push_opt!(frame, Country, self.country);
        frame_push_opt!(frame, CountryCode, self.country_code);
        frame_push_opt!(frame, StateProvince, self.state_province);
        frame_push_opt!(frame, County, self.county);
        frame_push_opt!(frame, Municipality, self.municipality);
        frame_push_opt!(frame, Latitude, self.latitude);
        frame_push_opt!(frame, Longitude, self.longitude);
        frame_push_opt!(frame, Elevation, self.elevation);
        frame_push_opt!(frame, Depth, self.depth);
        frame_push_opt!(frame, ElevationAccuracy, self.elevation_accuracy);
        frame_push_opt!(frame, DepthAccuracy, self.depth_accuracy);
        frame_push_opt!(frame, LocationSource, self.location_source);

        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, CollectedBy, self.collected_by);
        frame_push_opt!(frame, CollectionRemarks, self.collection_remarks);
        frame_push_opt!(frame, IdentifiedBy, self.identified_by);
        frame_push_opt!(frame, IdentifiedDate, self.identified_date);
        frame_push_opt!(frame, IdentificationRemarks, self.identification_remarks);

        frame_push_opt!(frame, Preparation, self.preparation);
        frame_push_opt!(frame, EnvironmentBroadScale, self.environment_broad_scale);
        frame_push_opt!(frame, EnvironmentLocalScale, self.environment_local_scale);
        frame_push_opt!(frame, EnvironmentMedium, self.environment_medium);

        frame_push_opt!(frame, Habitat, self.habitat);
        frame_push_opt!(frame, SpecificHost, self.specific_host);
        frame_push_opt!(frame, IndividualCount, self.individual_count);
        frame_push_opt!(frame, OrganismQuantity, self.organism_quantity);
        frame_push_opt!(frame, OrganismQuantityType, self.organism_quantity_type);

        frame_push_opt!(frame, Strain, self.strain);
        frame_push_opt!(frame, Isolate, self.isolate);
        frame_push_opt!(frame, FieldNotes, self.field_notes);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, CollectionEventOperation>(stream, dataset)
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let lookups = Lookups {
        names: name_lookup(&mut pool)?,
        organisms: organism_lookup(&mut pool)?,
        specimens: specimen_lookup(&mut pool)?,
    };

    let pager: FrameLoader<CollectionEventOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating collection events");

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total_entities = pager.total()?;
    info!(total_entities, "Reducing collection events");

    let reducer: DatabaseReducer<models::CollectionEvent, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::collection_events::dsl::*;

            let mut valid_records = Vec::new();
            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(collection_events)
                .values(valid_records)
                .on_conflict(id)
                .do_update()
                .set((
                    field_collecting_id.eq(excluded(field_collecting_id)),
                    name_id.eq(excluded(name_id)),
                    organism_id.eq(excluded(organism_id)),
                    specimen_id.eq(excluded(specimen_id)),
                    collected_by.eq(excluded(collected_by)),
                    collection_remarks.eq(excluded(collection_remarks)),
                    identified_by.eq(excluded(identified_by)),
                    identified_date.eq(excluded(identified_date)),
                    identification_remarks.eq(excluded(identification_remarks)),
                    locality.eq(excluded(locality)),
                    country.eq(excluded(country)),
                    country_code.eq(excluded(country_code)),
                    state_province.eq(excluded(state_province)),
                    county.eq(excluded(county)),
                    municipality.eq(excluded(municipality)),
                    latitude.eq(excluded(latitude)),
                    longitude.eq(excluded(longitude)),
                    elevation.eq(excluded(elevation)),
                    depth.eq(excluded(depth)),
                    elevation_accuracy.eq(excluded(elevation_accuracy)),
                    depth_accuracy.eq(excluded(depth_accuracy)),
                    location_source.eq(excluded(location_source)),
                    preparation.eq(excluded(preparation)),
                    environment_broad_scale.eq(excluded(environment_broad_scale)),
                    environment_local_scale.eq(excluded(environment_local_scale)),
                    environment_medium.eq(excluded(environment_medium)),
                    habitat.eq(excluded(habitat)),
                    specific_host.eq(excluded(specific_host)),
                    individual_count.eq(excluded(individual_count)),
                    organism_quantity.eq(excluded(organism_quantity)),
                    organism_quantity_type.eq(excluded(organism_quantity_type)),
                    strain.eq(excluded(strain)),
                    isolate.eq(excluded(isolate)),
                    field_notes.eq(excluded(field_notes)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}


struct Lookups {
    names: StringMap,
    organisms: StringMap,
    specimens: StringMap,
}


impl Reducer<Lookups> for models::CollectionEvent {
    type Atom = CollectionEventAtom;

    fn reduce(frame: Map<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use CollectionEventAtom::*;

        let mut field_collecting_id = None;
        let mut specimen_id = None;
        let mut organism_id = None;
        let mut scientific_name = None;

        let mut event_date = None;
        let mut event_time = None;
        let mut collected_by = None;
        let mut collection_remarks = None;
        let mut identified_by = None;
        let mut identified_date = None;
        let mut identification_remarks = None;

        let mut locality = None;
        let mut country = None;
        let mut country_code = None;
        let mut state_province = None;
        let mut county = None;
        let mut municipality = None;
        let mut latitude = None;
        let mut longitude = None;
        let mut elevation = None;
        let mut depth = None;
        let mut elevation_accuracy = None;
        let mut depth_accuracy = None;
        let mut location_source = None;

        let mut preparation = None;
        let mut environment_broad_scale = None;
        let mut environment_local_scale = None;
        let mut environment_medium = None;
        let mut habitat = None;
        let mut specific_host = None;
        let mut individual_count = None;
        let mut organism_quantity = None;
        let mut organism_quantity_type = None;
        let mut strain = None;
        let mut isolate = None;
        let mut field_notes = None;

        for atom in frame.atoms.into_values() {
            match atom {
                Empty => {}
                FieldCollectingId(value) => field_collecting_id = Some(value),
                SpecimenId(value) => specimen_id = Some(value),
                OrganismId(value) => organism_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),

                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                CollectedBy(value) => collected_by = Some(value),
                CollectionRemarks(value) => collection_remarks = Some(value),
                IdentifiedBy(value) => identified_by = Some(value),
                IdentifiedDate(value) => identified_date = Some(value),
                IdentificationRemarks(value) => identification_remarks = Some(value),

                Locality(value) => locality = Some(value),
                Country(value) => country = Some(value),
                CountryCode(value) => country_code = Some(value),
                StateProvince(value) => state_province = Some(value),
                County(value) => county = Some(value),
                Municipality(value) => municipality = Some(value),
                Latitude(value) => latitude = Some(value),
                Longitude(value) => longitude = Some(value),
                Elevation(value) => elevation = Some(value),
                Depth(value) => depth = Some(value),
                ElevationAccuracy(value) => elevation_accuracy = Some(value),
                DepthAccuracy(value) => depth_accuracy = Some(value),
                LocationSource(value) => location_source = Some(value),

                Preparation(value) => preparation = Some(value),
                EnvironmentBroadScale(value) => environment_broad_scale = Some(value),
                EnvironmentLocalScale(value) => environment_local_scale = Some(value),
                EnvironmentMedium(value) => environment_medium = Some(value),
                Habitat(value) => habitat = Some(value),
                SpecificHost(value) => specific_host = Some(value),
                IndividualCount(value) => individual_count = Some(value),
                OrganismQuantity(value) => organism_quantity = Some(value),
                OrganismQuantityType(value) => organism_quantity_type = Some(value),
                Strain(value) => strain = Some(value),
                Isolate(value) => isolate = Some(value),
                FieldNotes(value) => field_notes = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let field_collecting_id = field_collecting_id
            .ok_or(ReduceError::MissingAtom(frame.entity_id.to_string(), "FieldCollectingId".to_string()))?;
        let scientific_name = scientific_name
            .ok_or(ReduceError::MissingAtom(frame.entity_id.to_string(), "ScientificName".to_string()))?;
        let organism_id =
            organism_id.ok_or(ReduceError::MissingAtom(frame.entity_id.to_string(), "OrganismId".to_string()))?;


        let record = models::CollectionEvent {
            id: uuid::Uuid::new_v4(),
            entity_id: frame.entity_id,
            field_collecting_id,

            // everything in our database basically links to a name. we never should get an error
            // here as all names _should_ be imported with every dataset. however that is outside
            // the control of the oplogger so if you can't match a name make a loud noise
            name_id: lookups
                .names
                .get(&scientific_name)
                .ok_or(LookupError::Name(scientific_name))?
                .clone(),

            // every collection must have an organism. we will stub it out if the dataset doesn't
            // actually include any organism data
            organism_id: lookups
                .organisms
                .get(&organism_id)
                .ok_or(LookupError::Name(organism_id))?
                .clone(),

            // we can have collected specimens before they get registered into an official repository
            // so if we don't find a specimen then leave it as null
            specimen_id: specimen_id.and_then(|id| lookups.specimens.get(&id).copied()),

            event_date,
            event_time,
            collected_by,
            collection_remarks,
            identified_by,
            identified_date,
            identification_remarks,
            locality,
            country,
            country_code,
            state_province,
            county,
            municipality,
            latitude,
            longitude,
            elevation,
            depth,
            elevation_accuracy,
            depth_accuracy,
            location_source,

            preparation,
            environment_broad_scale,
            environment_local_scale,
            environment_medium,
            habitat,
            specific_host,
            individual_count,
            organism_quantity,
            organism_quantity_type,

            strain,
            isolate,
            field_notes,
        };

        Ok(record)
    }
}


impl EntityPager for FrameLoader<CollectionEventOperation> {
    type Operation = models::CollectionEventOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::collection_event_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;

        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        trace!(page, "loading entity operations");

        use schema::collection_event_logs::dsl::*;
        use schema_gnl::collection_event_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = collection_event_entities::table
            .select(collection_event_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = collection_event_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<CollectionEventOperation>(&mut conn)?;

        trace!(total = operations.len(), "operations loaded");
        Ok(operations)
    }
}

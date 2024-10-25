use std::io::Read;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{self, LogOperation, SpecimenAtom, SpecimenOperation};
use arga_core::schema;
use diesel::*;
use rayon::prelude::*;
use serde::Deserialize;
use tracing::{error, info};

use crate::database::{dataset_lookup, name_lookup, FrameLoader, PgPool, StringMap};
use crate::errors::Error;
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::{new_progress_bar, titleize_first_word};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type SpecimenFrame = DataFrame<SpecimenAtom>;


impl OperationLoader for FrameLoader<SpecimenOperation> {
    type Operation = SpecimenOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<SpecimenOperation>, Error> {
        use schema::specimen_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = specimen_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<SpecimenOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[SpecimenOperation]) -> Result<usize, Error> {
        use schema::specimen_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(specimen_logs)
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
    record_id: String,
    scientific_name: String,
    canonical_name: String,
    scientific_name_authority: Option<String>,

    type_status: Option<String>,
    institution_name: Option<String>,
    institution_code: Option<String>,
    // collection_code: Option<String>,
    // catalog_number: Option<String>,
    // collected_by: Option<String>,
    // identified_by: Option<String>,
    // identified_date: Option<String>,
    // organism_id: Option<String>,
    // material_sample_id: Option<String>,
    // details: Option<String>,
    // remarks: Option<String>,
    // identification_remarks: Option<String>,

    // // location block
    // locality: Option<String>,
    // country: Option<String>,
    // country_code: Option<String>,
    // state_province: Option<String>,
    // county: Option<String>,
    // municipality: Option<String>,
    // latitude: Option<f64>,
    // longitude: Option<f64>,
    // // verbatim_lat_long: Option<String>,
    // elevation: Option<f64>,
    // depth: Option<f64>,
    // elevation_accuracy: Option<f64>,
    // depth_accuracy: Option<f64>,
    // location_source: Option<String>,

    // // collection event block
    // event_date: Option<String>,
    // event_time: Option<String>,
    // field_number: Option<String>,
    // field_notes: Option<String>,
    // record_number: Option<String>,
    // individual_count: Option<String>,
    // organism_quantity: Option<String>,
    // organism_quantity_type: Option<String>,
    // sex: Option<String>,
    // genotypic_sex: Option<String>,
    // phenotypic_sex: Option<String>,
    // life_stage: Option<String>,
    // reproductive_condition: Option<String>,
    // behavior: Option<String>,
    // establishment_means: Option<String>,
    // degree_of_establishment: Option<String>,
    // pathway: Option<String>,
    // occurrence_status: Option<String>,
    // preparation: Option<String>,
    // other_catalog_numbers: Option<String>,
    // env_broad_scale: Option<String>,
    // env_local_scale: Option<String>,
    // env_medium: Option<String>,
    // habitat: Option<String>,
    // ref_biomaterial: Option<String>,
    // source_mat_id: Option<String>,
    // specific_host: Option<String>,
    // strain: Option<String>,
    // isolate: Option<String>,
}

impl IntoFrame for Record {
    type Atom = SpecimenAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: SpecimenFrame) -> SpecimenFrame {
        use SpecimenAtom::*;
        frame.push(EntityId(self.entity_id));
        frame.push(RecordId(self.record_id));
        frame.push(ScientificName(titleize_first_word(&self.scientific_name)));
        frame.push(CanonicalName(titleize_first_word(&self.canonical_name)));
        frame_push_opt!(frame, Authorship, self.scientific_name_authority);
        frame_push_opt!(frame, TypeStatus, self.type_status);
        frame_push_opt!(frame, InstitutionName, self.institution_name);
        frame_push_opt!(frame, InstitutionCode, self.institution_code);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, SpecimenOperation>(stream, dataset)
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let lookups = Lookups {
        names: name_lookup(&mut pool)?,
        datasets: dataset_lookup(&mut pool)?,
    };

    let pager: FrameLoader<SpecimenOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating specimens");

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total_entities = pager.total()?;
    info!(total_entities, "Reducing specimens");

    let reducer: DatabaseReducer<models::Specimen, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::specimens::dsl::*;

            let mut valid_records = Vec::new();
            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(specimens)
                .values(valid_records)
                .on_conflict(id)
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    name_id.eq(excluded(name_id)),
                    record_id.eq(excluded(record_id)),
                    material_sample_id.eq(excluded(material_sample_id)),
                    organism_id.eq(excluded(organism_id)),
                    institution_name.eq(excluded(institution_name)),
                    institution_code.eq(excluded(institution_code)),
                    collection_code.eq(excluded(collection_code)),
                    recorded_by.eq(excluded(recorded_by)),
                    identified_by.eq(excluded(identified_by)),
                    identified_date.eq(excluded(identified_date)),
                    type_status.eq(excluded(type_status)),
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
                    details.eq(excluded(details)),
                    remarks.eq(excluded(remarks)),
                    identification_remarks.eq(excluded(identification_remarks)),
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
    datasets: StringMap,
}


impl Reducer<Lookups> for models::Specimen {
    type Atom = SpecimenAtom;

    fn reduce(frame: Map<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use SpecimenAtom::*;

        let mut record_id = None;
        let mut material_sample_id = None;
        let mut organism_id = None;
        let mut scientific_name = None;
        let mut canonical_name = None;
        let mut authorship = None;
        let mut institution_name = None;
        let mut institution_code = None;
        let mut collection_code = None;
        let mut recorded_by = None;
        let mut identified_by = None;
        let mut identified_date = None;
        let mut type_status = None;
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
        let mut details = None;
        let mut remarks = None;
        let mut identification_remarks = None;

        for atom in frame.atoms.into_values() {
            match atom {
                Empty => {}
                EntityId(_) => {}
                RecordId(value) => record_id = Some(value),
                MaterialSampleId(value) => material_sample_id = Some(value),
                OrganismId(value) => organism_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                CanonicalName(value) => canonical_name = Some(value),
                Authorship(value) => authorship = Some(value),
                InstitutionName(value) => institution_name = Some(value),
                InstitutionCode(value) => institution_code = Some(value),
                CollectionCode(value) => collection_code = Some(value),
                RecordedBy(value) => recorded_by = Some(value),
                IdentifiedBy(value) => identified_by = Some(value),
                IdentifiedDate(value) => identified_date = Some(value),
                TypeStatus(value) => type_status = Some(value),
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
                Details(value) => details = Some(value),
                Remarks(value) => remarks = Some(value),
                IdentificationRemarks(value) => identification_remarks = Some(value),
            }
        }

        let record = models::Specimen {
            id: uuid::Uuid::new_v4(),
            entity_id: Some(frame.entity_id),
            dataset_id: lookups
                .datasets
                .get("ARGA:TL:0001000")
                .expect("dataset not found")
                .clone(),
            name_id: lookups
                .names
                .get(&scientific_name.expect("scientific_name not found"))
                .expect("name not found")
                .clone(),
            record_id: record_id.expect("record_id not found"),
            material_sample_id,
            organism_id,
            institution_name,
            institution_code,
            collection_code,
            recorded_by,
            identified_by,
            identified_date,
            type_status,
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
            details,
            remarks,
            identification_remarks,
        };

        Ok(record)
    }
}


impl EntityPager for FrameLoader<SpecimenOperation> {
    type Operation = models::SpecimenOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;

        let total = {
            use diesel::dsl::count_distinct;
            use schema::specimen_logs::dsl::*;
            specimen_logs
                .select(count_distinct(entity_id))
                .get_result::<i64>(&mut conn)?
        };

        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::specimen_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let limit = 10_000;
        let offset = page as i64 * limit;

        let entity_ids = specimen_logs
            .select(entity_id)
            .group_by(entity_id)
            .order_by(entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = specimen_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by((entity_id, operation_id))
            .load::<SpecimenOperation>(&mut conn)?;

        Ok(operations)
    }
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{AccessionEventAtom, AccessionEventOperation};
use arga_core::models::{self, DatasetVersion};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;
use xxhash_rust::xxh3::xxh3_64;

use crate::database::{name_lookup, FrameLoader, StringMap};
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type AccessionEventFrame = DataFrame<AccessionEventAtom>;


impl OperationLoader for FrameLoader<AccessionEventOperation> {
    type Operation = AccessionEventOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::accession_event_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = accession_event_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(accession_event_logs::all_columns())
            .order(operation_id.asc())
            .load::<AccessionEventOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::accession_event_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = accession_event_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(accession_event_logs::all_columns())
            .order(operation_id.asc())
            .load::<AccessionEventOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[AccessionEventOperation]) -> Result<usize, Error> {
        use schema::accession_event_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(accession_event_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

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
    specimen_id: String,
    scientific_name: String,

    type_status: Option<String>,
    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,

    collection_repository_id: Option<String>,
    collection_repository_code: Option<String>,
    institution_name: Option<String>,
    institution_code: Option<String>,

    disposition: Option<String>,
    preparation: Option<String>,

    accessioned_by: Option<String>,
    prepared_by: Option<String>,
    identified_by: Option<String>,
    identified_date: Option<chrono::NaiveDate>,
    identification_remarks: Option<String>,

    other_catalog_numbers: Option<String>,
}

impl IntoFrame for Record {
    type Atom = AccessionEventAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: AccessionEventFrame) -> AccessionEventFrame {
        use AccessionEventAtom::*;

        frame.push(SpecimenId(self.specimen_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, TypeStatus, self.type_status);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, CollectionRepositoryId, self.collection_repository_id);
        frame_push_opt!(frame, CollectionRepositoryCode, self.collection_repository_code);
        frame_push_opt!(frame, InstitutionName, self.institution_name);
        frame_push_opt!(frame, InstitutionCode, self.institution_code);
        frame_push_opt!(frame, Disposition, self.disposition);
        frame_push_opt!(frame, Preparation, self.preparation);
        frame_push_opt!(frame, AccessionedBy, self.accessioned_by);
        frame_push_opt!(frame, PreparedBy, self.prepared_by);
        frame_push_opt!(frame, IdentifiedBy, self.identified_by);
        frame_push_opt!(frame, IdentifiedDate, self.identified_date);
        frame_push_opt!(frame, IdentificationRemarks, self.identification_remarks);
        frame_push_opt!(frame, OtherCatalogNumbers, self.other_catalog_numbers);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, AccessionEventOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<AccessionEventOperation> {
    type Operation = models::AccessionEventOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::accession_event_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::accession_event_logs::dsl::*;
        use schema_gnl::accession_event_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = accession_event_entities::table
            .select(accession_event_entities::entity_id)
            .order_by(accession_event_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = accession_event_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<AccessionEventOperation>(&mut conn)?;

        Ok(operations)
    }
}


struct Lookups {
    names: StringMap,
}

impl Reducer<Lookups> for models::AccessionEvent {
    type Atom = AccessionEventAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use AccessionEventAtom::*;

        let mut specimen_id = None;
        let mut scientific_name = None;
        let mut type_status = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut collection_repository_id = None;
        let mut collection_repository_code = None;
        let mut institution_name = None;
        let mut institution_code = None;
        let mut disposition = None;
        let mut preparation = None;
        let mut accessioned_by = None;
        let mut prepared_by = None;
        let mut identified_by = None;
        let mut identified_date = None;
        let mut identification_remarks = None;
        let mut other_catalog_numbers = None;


        for atom in atoms {
            match atom {
                Empty => {}
                SpecimenId(value) => specimen_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                TypeStatus(value) => type_status = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                CollectionRepositoryId(value) => collection_repository_id = Some(value),
                CollectionRepositoryCode(value) => collection_repository_code = Some(value),
                InstitutionName(value) => institution_name = Some(value),
                InstitutionCode(value) => institution_code = Some(value),
                Disposition(value) => disposition = Some(value),
                Preparation(value) => preparation = Some(value),
                AccessionedBy(value) => accessioned_by = Some(value),
                PreparedBy(value) => prepared_by = Some(value),
                IdentifiedBy(value) => identified_by = Some(value),
                IdentifiedDate(value) => identified_date = Some(value),
                IdentificationRemarks(value) => identification_remarks = Some(value),
                OtherCatalogNumbers(value) => other_catalog_numbers = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;
        let specimen_id = specimen_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "SpecimenId".to_string()))?;

        let specimen_entity_id = xxh3_64(specimen_id.as_bytes());


        let record = models::AccessionEvent {
            entity_id,
            specimen_id: specimen_entity_id.to_string(),

            // everything in our database basically links to a name. we never should get an error
            // here as all names _should_ be imported with every dataset. however that is outside
            // the control of the oplogger so if you can't match a name make a loud noise
            name_id: lookups
                .names
                .get(&scientific_name)
                .ok_or(LookupError::Name(scientific_name))?
                .clone(),

            type_status,
            event_date,
            event_time,
            collection_repository_id,
            collection_repository_code,
            institution_name,
            institution_code,
            disposition,
            preparation,
            accessioned_by,
            prepared_by,
            identified_by,
            identified_date,
            identification_remarks,
            other_catalog_numbers,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let lookups = Lookups {
        names: name_lookup(&mut pool)?,
    };

    let pager: FrameLoader<AccessionEventOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating accession events");

    let reducer: DatabaseReducer<models::AccessionEvent, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::accession_events::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(accession_events)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    name_id.eq(excluded(name_id)),
                    specimen_id.eq(excluded(specimen_id)),
                    type_status.eq(excluded(type_status)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    collection_repository_id.eq(excluded(collection_repository_id)),
                    collection_repository_code.eq(excluded(collection_repository_code)),
                    institution_name.eq(excluded(institution_name)),
                    institution_code.eq(excluded(institution_code)),
                    disposition.eq(excluded(disposition)),
                    preparation.eq(excluded(preparation)),
                    accessioned_by.eq(excluded(accessioned_by)),
                    prepared_by.eq(excluded(prepared_by)),
                    identified_by.eq(excluded(identified_by)),
                    identified_date.eq(excluded(identified_date)),
                    identification_remarks.eq(excluded(identification_remarks)),
                    other_catalog_numbers.eq(excluded(other_catalog_numbers)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

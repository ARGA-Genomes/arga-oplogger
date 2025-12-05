use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{DepositionAtom, DepositionOperation};
use arga_core::models::{self, DatasetVersion};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;
use xxhash_rust::xxh3::xxh3_64;

use crate::database::FrameLoader;
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{OperationLoader, meta};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::{new_progress_bar, parse_string_opt};
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type DepositionFrame = DataFrame<DepositionAtom>;


impl OperationLoader for FrameLoader<DepositionOperation> {
    type Operation = DepositionOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::deposition_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = deposition_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(deposition_logs::all_columns())
            .order(operation_id.asc())
            .load::<DepositionOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::deposition_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = deposition_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(deposition_logs::all_columns())
            .order(operation_id.asc())
            .load::<DepositionOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[DepositionOperation]) -> Result<usize, Error> {
        use schema::deposition_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(deposition_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

        Ok(inserted)
    }
}


// A single row in a supported CSV file.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    entity_id: String,
    assembly_id: String,

    event_date: Option<chrono::NaiveDate>,
    url: Option<String>,
    institution: Option<String>,
}

impl IntoFrame for Record {
    type Atom = DepositionAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: DepositionFrame) -> DepositionFrame {
        use DepositionAtom::*;

        frame.push(AssemblyId(self.assembly_id));
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, Url, self.url);
        frame_push_opt!(frame, Institution, self.institution);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, DepositionOperation>(stream, dataset)?;

    // let pool = get_pool()?;
    // refresh_materialized_view(get_pool()?, MaterializedView::DepositionEntities)
    Ok(())
}


impl EntityPager for FrameLoader<DepositionOperation> {
    type Operation = models::DepositionOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::deposition_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::deposition_logs::dsl::*;
        use schema_gnl::deposition_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = deposition_entities::table
            .select(deposition_entities::entity_id)
            .order_by(deposition_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = deposition_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<DepositionOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::Deposition {
    type Atom = DepositionAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use DepositionAtom::*;

        let mut assembly_id = None;
        let mut event_date = None;
        let mut url = None;
        let mut institution = None;


        for atom in atoms {
            match atom {
                Empty => {}
                AssemblyId(value) => assembly_id = Some(value),
                EventDate(value) => event_date = Some(value),
                Url(value) => url = Some(value),
                Institution(value) => institution = Some(value),
            }
        }

        let assembly_id = assembly_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "AssemblyId".to_string()))?;
        let assembly_entity_id = xxh3_64(assembly_id.as_bytes());

        let record = models::Deposition {
            entity_id,
            assembly_id: assembly_entity_id.to_string(),
            event_date,
            url,
            institution,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<DepositionOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating depositions");
    let reducer: DatabaseReducer<models::Deposition, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::depositions::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(depositions)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    assembly_id.eq(excluded(assembly_id)),
                    event_date.eq(excluded(event_date)),
                    url.eq(excluded(url)),
                    institution.eq(excluded(institution)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

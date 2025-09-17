use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{AgentAtom, AgentOperation};
use arga_core::models::{self, DatasetVersion};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;

use crate::database::FrameLoader;
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{OperationLoader, meta};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type AgentFrame = DataFrame<AgentAtom>;


impl OperationLoader for FrameLoader<AgentOperation> {
    type Operation = AgentOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::agent_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = agent_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(agent_logs::all_columns())
            .order(operation_id.asc())
            .load::<AgentOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::agent_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = agent_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(agent_logs::all_columns())
            .order(operation_id.asc())
            .load::<AgentOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[AgentOperation]) -> Result<usize, Error> {
        use schema::agent_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(agent_logs)
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
    full_name: String,
    orcid: Option<String>,
}

impl IntoFrame for Record {
    type Atom = AgentAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: AgentFrame) -> AgentFrame {
        use AgentAtom::*;

        frame.push(FullName(self.full_name));
        frame_push_opt!(frame, Orcid, self.orcid);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, AgentOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<AgentOperation> {
    type Operation = models::AgentOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::agent_entities::table.count().get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::agent_logs::dsl::*;
        use schema_gnl::agent_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = agent_entities::table
            .select(agent_entities::entity_id)
            .order_by(agent_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = agent_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<AgentOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::Agent {
    type Atom = AgentAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use AgentAtom::*;

        let mut full_name = None;
        let mut orcid = None;

        for atom in atoms {
            match atom {
                Empty => {}
                FullName(value) => full_name = Some(value),
                Orcid(value) => orcid = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let full_name = full_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "FullName".to_string()))?;

        let record = models::Agent {
            entity_id,
            full_name,
            orcid,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<AgentOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating agents");
    let reducer: DatabaseReducer<models::Agent, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::agents::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(agents)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((full_name.eq(excluded(full_name)), orcid.eq(excluded(orcid))))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

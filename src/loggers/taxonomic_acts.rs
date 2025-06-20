use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{
    self,
    DatasetVersion,
    TaxonomicActAtom,
    TaxonomicActOperation,
    TaxonomicActOperationWithDataset,
    TaxonomicStatus,
};
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::*;
use indicatif::ProgressIterator;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::database::{dataset_lookup, get_pool, taxon_lookup, FrameLoader, PgPool, StringMap, UuidStringMap};
use crate::errors::{Error, LookupError, ReduceError};
use crate::frames::IntoFrame;
use crate::operations::group_operations;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::{
    date_time_from_str_opt,
    new_progress_bar,
    new_spinner,
    taxonomic_status_from_str,
    titleize_first_word,
    UpdateBars,
};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type TaxonomicActFrame = DataFrame<TaxonomicActAtom>;


impl OperationLoader for FrameLoader<TaxonomicActOperation> {
    type Operation = TaxonomicActOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::taxonomic_act_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = taxonomic_act_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(taxonomic_act_logs::all_columns())
            .order(operation_id.asc())
            .load::<TaxonomicActOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::taxonomic_act_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = taxonomic_act_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(taxonomic_act_logs::all_columns())
            .order(operation_id.asc())
            .load::<TaxonomicActOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[TaxonomicActOperation]) -> Result<usize, Error> {
        use schema::taxonomic_act_logs::dsl::*;
        let mut conn = self.pool.get()?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(taxonomic_act_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        Ok(inserted)
    }
}

// impl OperationReducer for FrameLoader<TaxonomicActOperationWithDataset> {
//     type Operation = TaxonomicActOperationWithDataset;
//     type ReducedRecord = TaxonomicAct;

//     fn total_entities(&self) -> Result<i64, Error> {
//         use diesel::dsl::count_distinct;
//         use schema::taxonomic_act_logs::dsl::*;

//         let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

//         // get the total amount of distinct entities in the log table. this allows
//         // us to split up the reduction into many threads without loading all operations
//         // into memory
//         let total = taxonomic_act_logs
//             .select(count_distinct(entity_id))
//             .get_result::<i64>(&mut conn)?;

//         Ok(total)
//     }

//     fn load_entity_operations(&self, offset: i64, limit: i64) -> Result<Vec<Self::Operation>, Error> {
//         use schema::taxonomic_act_logs::dsl::*;
//         use schema::{dataset_versions, datasets};

//         let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

//         let entity_ids = taxonomic_act_logs
//             .select(entity_id)
//             .group_by(entity_id)
//             .order_by(entity_id)
//             .offset(offset)
//             .limit(limit)
//             .into_boxed();

//         let operations = taxonomic_act_logs
//             .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
//             .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
//             .filter(entity_id.eq_any(entity_ids))
//             .order_by((entity_id, operation_id))
//             .load::<TaxonomicActOperationWithDataset>(&mut conn)?;

//         Ok(operations)
//     }

//     fn reduce(&self, operations: Vec<Self::Operation>) -> Result<Vec<Self::ReducedRecord>, Error> {
//         let entities = group_operations(operations, vec![]);
//         let mut records = Vec::new();

//         for (key, ops) in entities.into_iter() {
//             let mut map = Map::new(key);
//             map.reduce(&ops);

//             let mut record = TaxonomicAct::from(map);
//             if let Some(op) = ops.first() {
//                 record.dataset_uuid.clone_from(&op.dataset.id);
//                 records.push(record);
//             }
//         }

//         Ok(records)
//     }

//     fn upsert(&self, reduced_records: Vec<TaxonomicAct>) -> Result<(), Error> {
//         let mut pool = self.pool.clone();

//         let mut dataset_ids = HashSet::new();
//         for record in &reduced_records {
//             dataset_ids.insert(record.dataset_uuid);
//         }

//         let dataset_ids = Vec::from_iter(dataset_ids);
//         let taxa = taxon_lookup(&mut pool, &dataset_ids)?;

//         let mut records = Vec::new();
//         for record in reduced_records {
//             let taxon = taxa.get(&(record.dataset_uuid, record.taxon));
//             let accepted_taxon = taxa.get(&(record.dataset_uuid, record.accepted_taxon.unwrap_or_default()));

//             if let (Some(taxonomic_act), Some(taxon)) = (record.act, taxon) {
//                 records.push(models::TaxonomicAct {
//                     id: Uuid::new_v4(),
//                     entity_id: record.entity_id,
//                     taxon_id: *taxon,
//                     accepted_taxon_id: accepted_taxon.cloned(),
//                     act: taxonomic_act,
//                     source_url: record.source_url,
//                     created_at: chrono::Utc::now(),
//                     updated_at: chrono::Utc::now(),
//                     data_created_at: record.data_created_at,
//                     data_updated_at: record.data_updated_at,
//                 })
//             }
//         }

//         // postgres always creates a new row version so we cant get
//         // an actual figure of the amount of records changed
//         {
//             use diesel::upsert::excluded;
//             use schema::taxonomic_acts::dsl::*;

//             diesel::insert_into(taxonomic_acts)
//                 .values(records)
//                 .on_conflict(entity_id)
//                 .do_update()
//                 .set((
//                     entity_id.eq(excluded(entity_id)),
//                     taxon_id.eq(excluded(taxon_id)),
//                     accepted_taxon_id.eq(excluded(accepted_taxon_id)),
//                     act.eq(excluded(act)),
//                     source_url.eq(excluded(source_url)),
//                     updated_at.eq(excluded(updated_at)),
//                     data_created_at.eq(excluded(data_created_at)),
//                     data_updated_at.eq(excluded(data_updated_at)),
//                 ))
//                 .execute(&mut conn)?;
//         }

//         Ok(())
//     }
// }


/// The CSV record to decompose into operation logs.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize, Default)]
struct Record {
    /// Any value that uniquely identifies this record through its lifetime.
    /// This is a kind of global permanent identifier
    entity_id: String,

    /// The dataset id used to isolate the taxa from other systems
    dataset_id: String,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The name of the taxon currently accepted. Should include author when possible
    accepted_usage_taxon: Option<String>,

    /// The timestamp of when the record was created at the data source
    #[serde(deserialize_with = "date_time_from_str_opt")]
    created_at: Option<DateTime<Utc>>,
    /// The timestamp of when the record was update at the data source
    #[serde(deserialize_with = "date_time_from_str_opt")]
    updated_at: Option<DateTime<Utc>>,

    references: Option<String>,
}

impl IntoFrame for Record {
    type Atom = TaxonomicActAtom;

    fn entity_hashable(&self) -> &[u8] {
        // the nomenclatural act id should be an externally unique value that all datasets
        // reference if they are describing this particular datum
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: TaxonomicActFrame) -> TaxonomicActFrame {
        use TaxonomicActAtom::*;

        let accepted_taxon_usage = self.accepted_usage_taxon.map(|val| titleize_first_word(&val));

        frame.push(EntityId(self.entity_id));
        frame.push(Taxon(titleize_first_word(&self.scientific_name)));
        frame.push(DatasetId(self.dataset_id));
        frame_push_opt!(frame, AcceptedTaxon, accepted_taxon_usage);
        frame_push_opt!(frame, SourceUrl, self.references);
        frame_push_opt!(frame, CreatedAt, self.created_at);
        frame_push_opt!(frame, UpdatedAt, self.updated_at);
        frame
    }
}


/// The ARGA taxonomic act CSV record output
/// This is the record in a CSV after reducing the taxonomic act logs
/// from multiple datasets.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TaxonomicAct {
    /// The id of this record entity in the taxonomic act logs
    entity_id: String,
    /// The external identifier of the source dataset as determined by ARGA
    dataset_id: String,

    /// The internal identifier of the source dataset as determined by ARGA
    dataset_uuid: Uuid,

    /// The name of the taxon. Should include author when possible
    taxon: String,
    /// The name of the taxon currently accepted. Should include author when possible
    accepted_taxon: Option<String>,

    /// The timestamp of when the data was created in the dataset
    data_created_at: Option<DateTime<Utc>>,
    /// The timestamp of when the data was updated in the dataset
    data_updated_at: Option<DateTime<Utc>>,

    publication: Option<String>,
    publication_date: Option<String>,
    source_url: Option<String>,
}


pub fn import<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, TaxonomicActOperation>(stream, dataset)
}

pub fn update2() -> Result<(), Error> {
    let pool = get_pool()?;
    let mut conn = pool.get()?;

    // let loader: FrameLoader<TaxonomicActOperationWithDataset> = FrameLoader::new(pool);

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total = {
        use diesel::dsl::count_distinct;
        use schema::taxonomic_act_logs::dsl::*;

        taxonomic_act_logs
            .select(count_distinct(entity_id))
            .get_result::<i64>(&mut conn)?
    };

    let limit = 10_000;
    let offsets: Vec<i64> = (0..total).step_by(limit as usize).collect();

    offsets
        .into_par_iter()
        .try_for_each(|offset| reduce_and_update(pool.clone(), offset, limit))?;

    Ok(())
}

pub fn reduce_and_update(mut pool: PgPool, offset: i64, limit: i64) -> Result<(), Error> {
    let mut conn = pool.get()?;

    let operations = {
        use schema::taxonomic_act_logs::dsl::*;
        use schema::{dataset_versions, datasets};

        // we first get all the entity ids within a specified range. this means that the
        // query here has to return the same amount of results as a COUNT DISTINCT query otherwise
        // some entities will be missed during the update. in particular make sure to always order
        // the query results otherwise random entities might get pulled in since postgres doesnt sort by default
        let entity_ids = taxonomic_act_logs
            .select(entity_id)
            .group_by(entity_id)
            .order_by(entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        // get the operations for the entities making sure to order by operation id so that
        // the CRDT structs can do their thing
        taxonomic_act_logs
            .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .filter(entity_id.eq_any(entity_ids))
            .order_by((entity_id, operation_id))
            .load::<TaxonomicActOperationWithDataset>(&mut conn)?
    };

    // group the entity operations up and preparing it for use in the LWW map
    let entities = group_operations(operations, vec![]);
    let mut reduced_records = Vec::new();

    // reduce all the operations by applying them to an empty record
    // as per the last write wins policy
    for (key, ops) in entities.into_iter() {
        let mut map = Map::new(key);
        map.reduce(&ops);
        let map = Map::new("".to_string());

        let mut record = TaxonomicAct::from(map);
        if let Some(op) = ops.first() {
            record.dataset_uuid.clone_from(&op.dataset.id);
            reduced_records.push(record);
        }
    }

    // get all datasets involved so that we can scope the taxon lookup
    let mut dataset_ids = HashSet::new();
    for record in &reduced_records {
        dataset_ids.insert(record.dataset_uuid);
    }

    let dataset_ids = Vec::from_iter(dataset_ids);
    let taxa = taxon_lookup(&mut pool, &dataset_ids)?;

    let mut records = Vec::new();
    for record in reduced_records {
        // get the taxa that match by name and that are also from the same dataset as the record. this ensures
        // that relationships aren't formed across taxonomic datasets
        let taxon = taxa.get(&(record.dataset_uuid, record.taxon));
        let accepted_taxon = taxa.get(&(record.dataset_uuid, record.accepted_taxon.unwrap_or_default()));

        // taxonomic act and the taxon are mandatory so we print a warning when something
        // wont end up inserted in the database
        match taxon {
            None => warn!(record.entity_id, "Cannot find the taxon in the existing database"),
            Some(taxon) => records.push(models::TaxonomicAct {
                id: Uuid::new_v4(),
                entity_id: record.entity_id,
                taxon_id: *taxon,
                accepted_taxon_id: accepted_taxon.cloned(),
                source_url: record.source_url,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                data_created_at: record.data_created_at,
                data_updated_at: record.data_updated_at,
            }),
        }
    }

    // postgres always creates a new row version so we cant get
    // an actual figure of the amount of records changed
    {
        use diesel::upsert::excluded;
        use schema::taxonomic_acts::dsl::*;

        for chunk in records.chunks(1000) {
            diesel::insert_into(taxonomic_acts)
                .values(chunk)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    taxon_id.eq(excluded(taxon_id)),
                    accepted_taxon_id.eq(excluded(accepted_taxon_id)),
                    source_url.eq(excluded(source_url)),
                    updated_at.eq(excluded(updated_at)),
                    data_created_at.eq(excluded(data_created_at)),
                    data_updated_at.eq(excluded(data_updated_at)),
                ))
                .execute(&mut conn)?;
        }
    }

    Ok(())
}


pub struct TaxonomicActs {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl TaxonomicActs {
    /// Import the CSV file as taxonomic act operations into the taxonomic_act_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing taxonomic act logs
    /// and then insert them into the database, effectively updating taxonomic_act_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        crate::import_csv_as_logs::<Record, TaxonomicActOperation>(&self.path, &self.dataset_version_id)?;
        info!("Taxonomic act logs imported");
        Ok(())
    }

    /// Reduce the entire taxonomic_act_logs table into an ARGA CSV file.
    ///
    /// This will generate a snapshot of every taxonomic act built from all datasets
    /// using the last-write-win CRDT map. The snapshot output is a reproducible
    /// dataset that should be imported into the ARGA database and used by the application.
    pub fn reduce() -> Result<Vec<TaxonomicAct>, Error> {
        use schema::taxonomic_act_logs::dsl::*;
        use schema::{dataset_versions, datasets};

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let spinner = new_spinner("Loading taxonomic act logs");
        let ops = taxonomic_act_logs
            .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .order(operation_id.asc())
            .load::<TaxonomicActOperationWithDataset>(&mut conn)?;
        spinner.finish();

        let spinner = new_spinner("Grouping taxonomic act logs");
        let entities = group_operations(ops, vec![]);
        spinner.finish();

        let mut records = Vec::new();

        let bar = new_progress_bar(entities.len(), "Reducing operations");
        for (key, ops) in entities.into_iter().progress_with(bar) {
            let mut map = Map::new(key);
            map.reduce(&ops);

            // include the dataset global id in the reduced output to
            // allow for multiple taxonomic systems
            let mut record = TaxonomicAct::from(map);
            if let Some(op) = ops.first() {
                record.dataset_id.clone_from(&op.dataset.global_id);
                records.push(record);
            }
        }

        Ok(records)
    }

    pub fn update() -> Result<(), Error> {
        use diesel::upsert::excluded;
        use schema::taxonomic_acts::dsl::*;

        let mut pool = get_pool()?;
        let mut conn = pool.get()?;

        // reduce the logs and convert the record to the model equivalent. because taxa
        // are unique per dataset we need to have a dataset lookup and scope the taxa
        // lookup to the appropriate dataset, this ensures that taxonomic acts are applied
        // to the correct taxon for that system, rather than attaching an act across systems
        let reduced = Self::reduce()?;

        // get all the dataset uuids in the record list first to scope on
        let datasets = dataset_lookup(&mut pool)?;
        let mut dataset_ids = HashSet::new();
        for record in &reduced {
            if let Some(dataset_id) = datasets.get(&record.dataset_id) {
                dataset_ids.insert(*dataset_id);
            }
        }

        let dataset_ids = Vec::from_iter(dataset_ids);
        let taxa = taxon_lookup(&mut pool, &dataset_ids)?;

        let mut records = Vec::new();
        for record in reduced {
            let dataset_uuid = *datasets.get(&record.dataset_id).expect("Cannot find dataset");

            let taxon = taxa.get(&(dataset_uuid, record.taxon));
            let accepted_taxon = taxa.get(&(dataset_uuid, record.accepted_taxon.unwrap_or_default()));

            if let Some(taxon) = taxon {
                records.push(models::TaxonomicAct {
                    id: Uuid::new_v4(),
                    entity_id: record.entity_id,
                    taxon_id: *taxon,
                    accepted_taxon_id: accepted_taxon.cloned(),
                    source_url: record.source_url,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                    data_created_at: record.data_created_at,
                    data_updated_at: record.data_updated_at,
                })
            }
        }

        // finally import the operations. if there is a conflict based on the operation_id
        // then it is a duplicate operation so do nothing with it
        let bar = new_progress_bar(records.len(), "Importing taxonomic acts");
        for chunk in records.chunks(1000) {
            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(taxonomic_acts)
                .values(chunk)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    taxon_id.eq(excluded(taxon_id)),
                    accepted_taxon_id.eq(excluded(accepted_taxon_id)),
                    source_url.eq(excluded(source_url)),
                    updated_at.eq(excluded(updated_at)),
                    data_created_at.eq(excluded(data_created_at)),
                    data_updated_at.eq(excluded(data_updated_at)),
                ))
                .execute(&mut conn)?;

            bar.inc(1000);
        }

        bar.finish();
        info!(total = records.len(), "Taxonomic acts import finished");

        Ok(())
    }
}

/// Converts a LWW CRDT map of taxonomic act atoms to a TaxonomicAct record for serialisation
impl From<Map<TaxonomicActAtom>> for TaxonomicAct {
    fn from(value: Map<TaxonomicActAtom>) -> Self {
        use TaxonomicActAtom::*;

        let mut act = TaxonomicAct {
            entity_id: value.entity_id,
            ..Default::default()
        };

        for val in value.atoms.into_values() {
            match val {
                Empty => {}
                Publication(value) => act.publication = Some(value),
                PublicationDate(value) => act.publication_date = Some(value),
                Taxon(value) => act.taxon = value,
                AcceptedTaxon(value) => act.accepted_taxon = Some(value),
                SourceUrl(value) => act.source_url = Some(value),
                CreatedAt(value) => act.data_created_at = Some(value),
                UpdatedAt(value) => act.data_updated_at = Some(value),

                // we want this atom for provenance and reproduction with the hash
                // generation but we don't need to actually use it
                EntityId(_value) => {}
                DatasetId(_value) => {}
            }
        }

        act
    }
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let datasets = dataset_lookup(&mut pool)?;
    let dataset_ids: Vec<Uuid> = datasets.values().map(|id| id.clone()).collect();

    let lookups = Lookups {
        datasets,
        taxa: taxon_lookup(&mut pool, &dataset_ids)?,
    };

    let pager: FrameLoader<TaxonomicActOperation> = FrameLoader::new(pool.clone());

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total_entities = pager.total()? as usize;
    let bars = UpdateBars::new(total_entities);

    info!(total_entities, "Reducing taxonomic acts");

    let reducer: DatabaseReducer<models::TaxonomicAct, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::taxonomic_acts::dsl::*;

            let mut valid_records = Vec::new();
            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(taxonomic_acts)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    taxon_id.eq(excluded(taxon_id)),
                    accepted_taxon_id.eq(excluded(accepted_taxon_id)),
                    source_url.eq(excluded(source_url)),
                    updated_at.eq(excluded(updated_at)),
                    data_created_at.eq(excluded(data_created_at)),
                    data_updated_at.eq(excluded(data_updated_at)),
                ))
                .execute(&mut conn)?;

            bars.records.inc(chunk.len() as u64);
        }
    }

    bars.finish();
    info!("Finished reducing and updating taxonomic acts");

    Ok(())
}


impl EntityPager for FrameLoader<TaxonomicActOperation> {
    type Operation = models::TaxonomicActOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;

        let total = {
            use diesel::dsl::count_distinct;
            use schema::taxonomic_act_logs::dsl::*;
            taxonomic_act_logs
                .select(count_distinct(entity_id))
                .get_result::<i64>(&mut conn)?
        };

        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::taxonomic_act_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let limit = 10_000;
        let offset = page as i64 * limit;

        let entity_ids = taxonomic_act_logs
            .select(entity_id)
            .group_by(entity_id)
            .order_by(entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = taxonomic_act_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by((entity_id, operation_id))
            .load::<TaxonomicActOperation>(&mut conn)?;

        Ok(operations)
    }
}


struct Lookups {
    datasets: StringMap,
    taxa: UuidStringMap,
}

impl Reducer<Lookups> for models::TaxonomicAct {
    type Atom = TaxonomicActAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use TaxonomicActAtom::*;

        let mut dataset_id = None;
        let mut taxon = None;
        let mut accepted_taxon = None;
        let mut source_url = None;
        let mut data_created_at = None;
        let mut data_updated_at = None;

        for atom in atoms {
            match atom {
                DatasetId(value) => dataset_id = Some(value),
                Taxon(value) => taxon = Some(value),
                AcceptedTaxon(value) => accepted_taxon = Some(value),
                SourceUrl(value) => source_url = Some(value),
                CreatedAt(value) => data_created_at = Some(value),
                UpdatedAt(value) => data_updated_at = Some(value),
                _ => {}
            }
        }

        let dataset_id = dataset_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "DatasetId".to_string()))?;
        let dataset_id = lookups
            .datasets
            .get(&dataset_id)
            .ok_or(LookupError::Dataset(dataset_id))?
            .clone();

        let taxon = taxon.ok_or(ReduceError::MissingAtom(entity_id.clone(), "Taxon".to_string()))?;
        let accepted_taxon =
            accepted_taxon.ok_or(ReduceError::MissingAtom(entity_id.clone(), "AcceptedTaxon".to_string()))?;

        let taxon_key = (dataset_id, taxon.clone());
        let taxon_id = lookups
            .taxa
            .get(&taxon_key)
            .ok_or(LookupError::Name(taxon.clone()))?
            .clone();

        let accepted_taxon_key = (dataset_id, accepted_taxon.clone());
        let accepted_taxon_id = lookups
            .taxa
            .get(&accepted_taxon_key)
            .ok_or(LookupError::Name(accepted_taxon.clone()))?
            .clone();

        let record = models::TaxonomicAct {
            id: Uuid::new_v4(),
            entity_id,
            taxon_id,
            accepted_taxon_id: Some(accepted_taxon_id),
            source_url,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            data_created_at,
            data_updated_at,
        };
        Ok(record)
    }
}

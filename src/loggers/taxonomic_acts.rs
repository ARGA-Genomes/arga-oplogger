use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{
    self,
    TaxonomicActAtom,
    TaxonomicActOperation,
    TaxonomicActOperationWithDataset,
    TaxonomicActType,
    TaxonomicStatus,
};
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::*;
use indicatif::ProgressIterator;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::database::{dataset_lookup, get_pool, taxon_lookup, FrameLoader};
use crate::errors::Error;
use crate::operations::group_operations;
use crate::readers::csv::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::utils::{date_time_from_str_opt, new_progress_bar, new_spinner, taxonomic_status_from_str};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type TaxonomicActFrame = DataFrame<TaxonomicActAtom>;


impl OperationLoader for FrameLoader<TaxonomicActOperation> {
    type Operation = TaxonomicActOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<TaxonomicActOperation>, Error> {
        use schema::taxonomic_act_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = taxonomic_act_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<TaxonomicActOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[TaxonomicActOperation]) -> Result<usize, Error> {
        use schema::taxonomic_act_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(taxonomic_act_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        Ok(inserted)
    }
}


/// The CSV record to decompose into operation logs.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    /// Any value that uniquely identifies this record through its lifetime.
    /// This is a kind of global permanent identifier
    entity_id: String,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The name of the taxon currently accepted. Should include author when possible
    accepted_usage_taxon: Option<String>,

    /// The status of the taxon. Refer to TaxonomicStatus for all options
    #[serde(deserialize_with = "taxonomic_status_from_str")]
    taxonomic_status: TaxonomicStatus,

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

        // derive the act from the taxonomic status
        let act = match self.taxonomic_status {
            TaxonomicStatus::Accepted => Some(TaxonomicActType::Accepted),
            TaxonomicStatus::Synonym => Some(TaxonomicActType::Synonym),
            TaxonomicStatus::Homonym => Some(TaxonomicActType::Homonym),
            TaxonomicStatus::Unaccepted => Some(TaxonomicActType::Unaccepted),
            TaxonomicStatus::NomenclaturalSynonym => Some(TaxonomicActType::NomenclaturalSynonym),
            TaxonomicStatus::TaxonomicSynonym => Some(TaxonomicActType::TaxonomicSynonym),
            TaxonomicStatus::ReplacedSynonym => Some(TaxonomicActType::ReplacedSynonym),
            _ => None,
        };

        frame.push(EntityId(self.entity_id));
        frame.push(Taxon(self.scientific_name));
        frame_push_opt!(frame, Act, act);
        frame_push_opt!(frame, AcceptedTaxon, self.accepted_usage_taxon);
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

    /// The name of the taxon. Should include author when possible
    taxon: String,
    /// The name of the taxon currently accepted. Should include author when possible
    accepted_taxon: Option<String>,

    /// The taxonomic act of this record
    act: Option<TaxonomicActType>,

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

            if let (Some(taxonomic_act), Some(taxon)) = (record.act, taxon) {
                records.push(models::TaxonomicAct {
                    id: Uuid::new_v4(),
                    entity_id: record.entity_id,
                    taxon_id: *taxon,
                    accepted_taxon_id: accepted_taxon.cloned(),
                    act: taxonomic_act,
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
                    act.eq(excluded(act)),
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
                Act(value) => act.act = Some(value),
                SourceUrl(value) => act.source_url = Some(value),
                CreatedAt(value) => act.data_created_at = Some(value),
                UpdatedAt(value) => act.data_updated_at = Some(value),

                // we want this atom for provenance and reproduction with the hash
                // generation but we don't need to actually use it
                EntityId(_value) => {}
            }
        }

        act
    }
}

use std::path::PathBuf;

use arga_core::crdt::lww::Map;
use arga_core::crdt::{DataFrame, Version};
use arga_core::models::{self, NomenclaturalActAtom, NomenclaturalActOperation, NomenclaturalActType};
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::*;
use indicatif::ProgressIterator;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::database::{get_pool, name_lookup, name_publication_lookup};
use crate::errors::Error;
use crate::operations::{group_operations, merge_operations};
use crate::utils::{date_time_from_str_opt, new_progress_bar, new_spinner, nomenclatural_act_from_str};

type NomenclaturalActFrame = DataFrame<NomenclaturalActAtom>;

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
    acted_on: Option<String>,

    /// The status of the taxon. Refer to TaxonomicStatus for all options
    #[serde(deserialize_with = "nomenclatural_act_from_str")]
    act: NomenclaturalActType,

    publication: String,
    publication_date: Option<String>,

    source_url: String,
    _citation: Option<String>,

    /// The timestamp of when the record was created at the data source
    #[serde(deserialize_with = "date_time_from_str_opt")]
    _created_at: Option<DateTime<Utc>>,
    /// The timestamp of when the record was update at the data source
    #[serde(deserialize_with = "date_time_from_str_opt")]
    _updated_at: Option<DateTime<Utc>>,
}

/// The ARGA taxonomic act CSV record output
/// This is the record in a CSV after reducing the taxonomic act logs
/// from multiple datasets.
#[derive(Clone, Debug, Default, Serialize)]
pub struct NomenclaturalAct {
    /// The id of this record entity in the taxonomic act logs
    entity_id: String,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The name of the taxon currently accepted. Should include author when possible
    acted_on: String,

    /// The taxonomic act of this record
    act: Option<NomenclaturalActType>,

    publication: String,
    publication_date: Option<String>,
    source_url: String,
    citation: Option<String>,
}

pub struct NomenclaturalActs {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl NomenclaturalActs {
    pub fn acts(&self) -> Result<Vec<NomenclaturalActOperation>, Error> {
        use NomenclaturalActAtom::*;

        let spinner = new_spinner("Parsing taxonomy CSV file");
        let mut records: Vec<Record> = Vec::new();
        for row in csv::Reader::from_path(&self.path)?.deserialize() {
            records.push(row?);
        }
        spinner.finish();

        // an operation represents one field and a record is a single entity so we have a logical grouping
        // that would be ideal to represent as a frame (like a database transaction). this allows us to
        // leverage the logical clock in the operation_id to closely associate these fields and make it
        // obvious that they occur within the same record, a kind of co-locating of operations.
        let mut last_version = Version::new();
        let mut operations: Vec<NomenclaturalActOperation> = Vec::new();

        let bar = new_progress_bar(records.len(), "Decomposing records into operation logs");
        for record in records.into_iter().progress_with(bar) {
            // because arga supports multiple taxonomic systems we use the taxon_id
            // from the system as the unique entity id. if we used the scientific name
            // instead then we would combine and reduce changes from all systems which
            // is not desireable for our purposes
            let mut hasher = Xxh3::new();
            hasher.update(record.entity_id.as_bytes());
            let hash = hasher.digest();

            let mut frame = NomenclaturalActFrame::create(hash.to_string(), self.dataset_version_id, last_version);
            // frame.push(EntityId(record.entity_id));
            frame.push(ScientificName(record.scientific_name));
            frame.push(Act(record.act));
            frame.push(SourceUrl(record.source_url));
            frame.push(Publication(record.publication));

            if let Some(value) = record.acted_on {
                frame.push(ActedOn(value));
            }
            if let Some(value) = record.publication_date {
                frame.push(PublicationDate(value));
            }

            last_version = frame.last_version();
            operations.extend(frame.collect());
        }

        Ok(operations)
    }

    /// Import the CSV file as taxonomic act operations into the taxonomic_act_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing taxonomic act logs
    /// and then insert them into the database, effectively updating taxonomic_act_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        use schema::nomenclatural_act_logs::dsl::*;

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        // load the existing operations. this can be quite large it includes
        // all operations ever, and is a grow-only table.
        // a future memory optimised operation could instead group by entity id
        // first and then query large chunks of logs in parallel
        let spinner = new_spinner("Loading existing nomenclatural act operations");
        let existing = nomenclatural_act_logs
            .order(operation_id.asc())
            .load::<NomenclaturalActOperation>(&mut conn)?;
        spinner.finish();

        // parse and decompose the input file into taxon operations
        let records = self.acts()?;

        // merge the new operations with the existing ones in the database
        // to deduplicate all ops
        let spinner = new_spinner("Merging existing and new operations");
        let records = merge_operations(existing, records);
        spinner.finish();

        let mut total_imported = 0;
        let bar = new_progress_bar(records.len(), "Importing nomenclatural act operations");

        // finally import the operations. if there is a conflict based on the operation_id
        // then it is a duplicate operation so do nothing with it
        for chunk in records.chunks(1000) {
            let inserted = diesel::insert_into(nomenclatural_act_logs)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;

            total_imported += inserted;
            bar.inc(1000);
        }

        bar.finish();
        info!(total_imported, "Nomenclatural act logs imported");
        Ok(())
    }

    /// Reduce the entire taxonomic_act_logs table into an ARGA CSV file.
    ///
    /// This will generate a snapshot of every taxonomic act built from all datasets
    /// using the last-write-win CRDT map. The snapshot output is a reproducible
    /// dataset that should be imported into the ARGA database and used by the application.
    pub fn reduce() -> Result<Vec<NomenclaturalAct>, Error> {
        use schema::nomenclatural_act_logs::dsl::*;

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let spinner = new_spinner("Loading nomenclatural act logs");
        let ops = nomenclatural_act_logs
            // .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            // .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .order(operation_id.asc())
            .load::<NomenclaturalActOperation>(&mut conn)?;
        spinner.finish();

        let spinner = new_spinner("Grouping nomenclatural act logs");
        let entities = group_operations(ops, vec![]);
        spinner.finish();

        let mut records = Vec::new();

        let bar = new_progress_bar(entities.len(), "Reducing operations");
        for (key, ops) in entities.into_iter().progress_with(bar) {
            let mut map = Map::new(key);
            map.reduce(&ops);

            // include the dataset global id in the reduced output to
            // allow for multiple taxonomic systems
            let record = NomenclaturalAct::from(map);
            if let Some(_op) = ops.first() {
                records.push(record);
            }
        }

        Ok(records)
    }

    pub fn update() -> Result<(), Error> {
        use diesel::upsert::excluded;
        use schema::nomenclatural_acts::dsl::*;

        let mut pool = get_pool()?;
        let mut conn = pool.get()?;

        // reduce the logs and convert the record to the model equivalent. because taxa
        // are unique per dataset we need to have a dataset lookup and scope the taxa
        // lookup to the appropriate dataset, this ensures that taxonomic acts are applied
        // to the correct taxon for that system, rather than attaching an act across systems
        let reduced = Self::reduce()?;

        let names = name_lookup(&mut pool)?;
        let publications = name_publication_lookup(&mut pool)?;

        let mut records = Vec::new();
        for record in reduced {
            let name_uuid = names.get(&record.scientific_name);
            let acted_on_uuid = names.get(&record.acted_on);
            let name_publication_id = publications.get(&record.publication);

            if let (Some(name_uuid), Some(acted_on_uuid), Some(nomen_act), Some(name_publication_id)) =
                (name_uuid, acted_on_uuid, record.act, name_publication_id)
            {
                records.push(models::NomenclaturalAct {
                    id: Uuid::new_v4(),
                    entity_id: record.entity_id,
                    publication_id: *name_publication_id,
                    name_id: *name_uuid,
                    acted_on_id: *acted_on_uuid,
                    act: nomen_act,
                    source_url: record.source_url,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                })
            }
        }

        // finally import the operations. if there is a conflict based on the operation_id
        // then it is a duplicate operation so do nothing with it
        let bar = new_progress_bar(records.len(), "Importing nomenclatural acts");
        for chunk in records.chunks(1000) {
            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(nomenclatural_acts)
                .values(chunk)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    publication_id.eq(excluded(publication_id)),
                    name_id.eq(excluded(name_id)),
                    acted_on_id.eq(excluded(acted_on_id)),
                    act.eq(excluded(act)),
                    source_url.eq(excluded(source_url)),
                    updated_at.eq(excluded(updated_at)),
                ))
                .execute(&mut conn)?;

            bar.inc(1000);
        }

        bar.finish();
        info!(total = records.len(), "Nomenclatural acts import finished");

        Ok(())
    }
}

/// Converts a LWW CRDT map of taxonomic act atoms to a TaxonomicAct record for serialisation
impl From<Map<NomenclaturalActAtom>> for NomenclaturalAct {
    fn from(value: Map<NomenclaturalActAtom>) -> Self {
        use NomenclaturalActAtom::*;

        let mut act = NomenclaturalAct {
            entity_id: value.entity_id,
            ..Default::default()
        };

        for val in value.atoms.into_values() {
            match val {
                Empty => {}
                Publication(value) => act.publication = value,
                PublicationDate(value) => act.publication_date = Some(value),
                ScientificName(value) => act.scientific_name = value,
                ActedOn(value) => act.acted_on = value,
                Act(value) => act.act = Some(value),
                SourceUrl(value) => act.source_url = value,
                // we want this atom for provenance and reproduction with the hash
                // generation but we don't need to actually use it
                // EntityId(_value) => {}
                _ => {}
            }
        }

        act
    }
}

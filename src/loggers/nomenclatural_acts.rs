use std::path::PathBuf;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{self, NomenclaturalActAtom, NomenclaturalActOperation, NomenclaturalActType};
use arga_core::schema;
use diesel::*;
use indicatif::ProgressIterator;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::database::{get_pool, name_lookup, publication_lookup, FrameLoader, PgPool};
use crate::errors::Error;
use crate::frames::{FrameReader, IntoFrame};
use crate::operations::group_operations;
use crate::readers::OperationLoader;
use crate::utils::{new_progress_bar, new_spinner, nomenclatural_act_from_str};
use crate::{frame_push_opt, import_frames_from_stream, FrameProgress};

type NomenclaturalActFrame = DataFrame<NomenclaturalActAtom>;


impl OperationLoader for FrameLoader<NomenclaturalActOperation> {
    type Operation = NomenclaturalActOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<NomenclaturalActOperation>, Error> {
        use schema::nomenclatural_act_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = nomenclatural_act_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<NomenclaturalActOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[NomenclaturalActOperation]) -> Result<usize, Error> {
        use schema::nomenclatural_act_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(nomenclatural_act_logs)
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
pub struct Record {
    /// Any value that uniquely identifies this record through its lifetime.
    /// This is a kind of global permanent identifier
    pub entity_id: String,

    /// The name of the taxon. Should include author when possible
    pub scientific_name: String,
    /// The name of the taxon without the author
    pub canonical_name: String,
    /// The authorship of the name
    pub scientific_name_authorship: Option<String>,

    pub authority_name: Option<String>,
    pub authority_year: Option<String>,

    pub base_authority_name: Option<String>,
    pub base_authority_year: Option<String>,

    /// The name of the taxon currently accepted. Should include author when possible
    pub acted_on: Option<String>,

    /// The status of the taxon. Refer to TaxonomicStatus for all options
    #[serde(deserialize_with = "nomenclatural_act_from_str")]
    pub act: NomenclaturalActType,

    pub publication: String,
    pub publication_date: Option<String>,

    pub source_url: String,
    // citation: Option<String>,

    // /// The timestamp of when the record was created at the data source
    // #[serde(deserialize_with = "date_time_from_str_opt")]
    // created_at: Option<DateTime<Utc>>,
    // /// The timestamp of when the record was update at the data source
    // #[serde(deserialize_with = "date_time_from_str_opt")]
    // updated_at: Option<DateTime<Utc>>,
}

impl IntoFrame for Record {
    type Atom = NomenclaturalActAtom;

    fn entity_hashable(&self) -> &[u8] {
        // the nomenclatural act id should be an externally unique value that all datasets
        // reference if they are describing this particular datum
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: NomenclaturalActFrame) -> NomenclaturalActFrame {
        use NomenclaturalActAtom::*;
        frame.push(EntityId(self.entity_id.clone()));
        frame.push(ScientificName(self.scientific_name));
        frame.push(CanonicalName(self.canonical_name));
        frame.push(Act(self.act));
        frame.push(SourceUrl(self.source_url));
        frame.push(Publication(self.publication));
        frame_push_opt!(frame, Authorship, self.scientific_name_authorship);
        frame_push_opt!(frame, AuthorityName, self.authority_name);
        frame_push_opt!(frame, AuthorityYear, self.authority_year);
        frame_push_opt!(frame, BasionymAuthorityName, self.base_authority_name);
        frame_push_opt!(frame, BasionymAuthorityYear, self.base_authority_year);
        frame_push_opt!(frame, ActedOn, self.acted_on);
        frame_push_opt!(frame, PublicationDate, self.publication_date);
        frame
    }
}


/// Import frames of nomenclatural acts from the stream
pub fn import<R>(reader: R, pool: PgPool) -> Result<(), Error>
where
    R: FrameReader<Atom = models::NomenclaturalActAtom> + FrameProgress,
    R: Iterator<Item = Result<DataFrame<R::Atom>, Error>>,
{
    import_frames_from_stream::<models::NomenclaturalActOperation, R>(reader, pool)
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
    /// The authorship of the name
    scientific_name_authorship: Option<String>,
    /// The name of the taxon without the author
    canonical_name: String,

    authority_name: Option<String>,
    authority_year: Option<String>,

    base_authority_name: Option<String>,
    base_authority_year: Option<String>,

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
    /// Import the CSV file as taxonomic act operations into the taxonomic_act_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing taxonomic act logs
    /// and then insert them into the database, effectively updating taxonomic_act_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        crate::import_csv_as_logs::<Record, NomenclaturalActOperation>(&self.path, &self.dataset_version_id)?;
        info!("Nomenclatural act logs imported");
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

        // import all the names in case they don't already exist. we use names to
        // hang data on including the names that a nomenclatural act describes or acts on
        let mut names = Vec::new();
        for record in &reduced {
            names.push(models::Name {
                id: Uuid::new_v4(),
                scientific_name: record.scientific_name.clone(),
                canonical_name: record.canonical_name.clone(),
                authorship: record.scientific_name_authorship.clone(),
            });
        }
        names.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
        names.dedup_by(|a, b| a.scientific_name.eq(&b.scientific_name));
        super::names::import(&names)?;

        let names = name_lookup(&mut pool)?;
        let publications = publication_lookup(&mut pool)?;

        let mut records = Vec::new();
        for record in reduced {
            let name_uuid = names.get(&record.scientific_name);
            let acted_on_uuid = names.get(&record.acted_on);
            let pub_id = publications.get(&record.publication);

            // default to the root of names
            let acted_on_uuid = acted_on_uuid.or_else(|| names.get("Eukaryota"));

            if let (Some(name_uuid), Some(acted_on_uuid), Some(nomen_act), Some(pub_id)) =
                (name_uuid, acted_on_uuid, record.act, pub_id)
            {
                records.push(models::NomenclaturalAct {
                    id: Uuid::new_v4(),
                    entity_id: record.entity_id,
                    publication_id: *pub_id,
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
                Authorship(value) => act.scientific_name_authorship = Some(value),
                CanonicalName(value) => act.canonical_name = value,
                AuthorityName(value) => act.authority_name = Some(value),
                AuthorityYear(value) => act.authority_year = Some(value),
                BasionymAuthorityName(value) => act.base_authority_name = Some(value),
                BasionymAuthorityYear(value) => act.base_authority_year = Some(value),
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

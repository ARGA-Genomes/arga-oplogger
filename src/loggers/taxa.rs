use std::path::PathBuf;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{self, TaxonAtom, TaxonOperation, TaxonOperationWithDataset, TaxonomicRank, TaxonomicStatus};
use arga_core::schema;
use diesel::*;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::database::{
    dataset_lookup,
    get_pool,
    name_lookup,
    refresh_materialized_view,
    taxon_lookup,
    FrameLoader,
    MaterializedView,
};
use crate::errors::Error;
use crate::frame_push_opt;
use crate::operations::group_operations;
use crate::readers::csv::IntoFrame;
use crate::readers::OperationLoader;
use crate::utils::{
    new_progress_bar,
    new_spinner,
    taxonomic_rank_from_str,
    taxonomic_status_from_str,
    titleize_first_word,
};

type TaxonFrame = DataFrame<TaxonAtom>;


impl OperationLoader for FrameLoader<TaxonOperation> {
    type Operation = TaxonOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<TaxonOperation>, Error> {
        use schema::taxa_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = taxa_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<TaxonOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[Self::Operation]) -> Result<usize, Error> {
        use schema::taxa_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(taxa_logs)
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

    /// The record id assigned by the dataset
    taxon_id: String,
    /// The scientific name of the parent taxon to link up in a tree
    parent_taxon: Option<String>,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The authorship of the taxon
    scientific_name_authorship: Option<String>,

    /// The name of the taxon without the author
    canonical_name: String,

    /// The rank of the taxon. Refer to TaxonomicRank for all options
    #[serde(deserialize_with = "taxonomic_rank_from_str")]
    taxon_rank: TaxonomicRank,
    /// The status of the taxon. Refer to TaxonomicStatus for all options
    #[serde(deserialize_with = "taxonomic_status_from_str")]
    taxonomic_status: TaxonomicStatus,

    /// The code used to define the taxon. Eg. ICZN
    nomenclatural_code: String,

    citation: Option<String>,
    references: Option<String>,
    last_updated: Option<String>,
}

impl IntoFrame for Record {
    type Atom = TaxonAtom;

    fn entity_hashable(&self) -> &[u8] {
        // because arga supports multiple taxonomic systems we use the entity_id
        // field which should be salted with a unique dataset_id to ensure that
        // matching scientific names remain unqiue within the dataset only
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: TaxonFrame) -> TaxonFrame {
        use TaxonAtom::*;
        frame.push(EntityId(self.entity_id));
        frame.push(TaxonId(self.taxon_id));
        frame.push(ScientificName(titleize_first_word(&self.scientific_name)));
        frame.push(CanonicalName(titleize_first_word(&self.canonical_name)));
        frame.push(TaxonomicRank(self.taxon_rank));
        frame.push(TaxonomicStatus(self.taxonomic_status));
        frame.push(NomenclaturalCode(self.nomenclatural_code));
        frame_push_opt!(frame, Authorship, self.scientific_name_authorship);
        frame_push_opt!(frame, Citation, self.citation);
        frame_push_opt!(frame, References, self.references);
        frame_push_opt!(frame, LastUpdated, self.last_updated);
        if let Some(value) = self.parent_taxon {
            frame.push(ParentTaxon(titleize_first_word(&value)));
        }
        frame
    }
}


/// The ARGA taxon CSV record output
/// This is the record in a CSV after reducing the taxa logs
/// from multiple datasets.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Taxon {
    /// The id of this record entity in the taxa logs
    entity_id: String,
    /// The id of the taxon as determined by the source dataset
    taxon_id: String,
    /// The scientific name of the parent taxon. Useful for taxonomy trees
    parent_taxon: Option<String>,
    /// The external identifier of the source dataset as determined by ARGA
    dataset_id: String,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The authorship of the taxon
    scientific_name_authorship: Option<String>,
    /// The name of the taxon without the author
    canonical_name: String,
    /// The code used to define the taxon. Eg. ICZN
    nomenclatural_code: String,

    /// The rank of the taxon. Refer to TaxonomicRank for all options
    taxon_rank: TaxonomicRank,
    /// The status of the taxon. Refer to TaxonomicStatus for all options
    taxonomic_status: TaxonomicStatus,

    citation: Option<String>,
    references: Option<String>,
    last_updated: Option<String>,
}


pub struct Taxa {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl Taxa {
    /// Import the CSV file as taxon operations into the taxa_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing taxa logs
    /// and then insert them into the database, effectively updating taxa_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        crate::import_csv_as_logs::<Record, TaxonOperation>(&self.path, &self.dataset_version_id)?;
        info!("Taxa operations import finished");
        Ok(())
    }

    /// Reduce the entire taxa_logs table into a list of Taxon.
    ///
    /// This will generate a snapshot of every taxon built from all datasets
    /// using the last-write-win CRDT map. The snapshot output is a reproducible
    /// dataset that should be imported into the ARGA database and used by the application.
    pub fn reduce() -> Result<Vec<Taxon>, Error> {
        use schema::taxa_logs::dsl::*;
        use schema::{dataset_versions, datasets};

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let spinner = new_spinner("Loading taxa logs");
        let ops = taxa_logs
            .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .order(operation_id.asc())
            .load::<TaxonOperationWithDataset>(&mut conn)?;
        spinner.finish();

        let spinner = new_spinner("Grouping taxa logs");
        let entities = group_operations(ops, vec![]);
        spinner.finish();

        let mut taxa = Vec::new();

        let bar = new_progress_bar(entities.len(), "Reducing operations");
        for (key, ops) in entities.into_iter().progress_with(bar) {
            let mut map = Map::new(key);
            map.reduce(&ops);

            // include the dataset global id in the reduced output to
            // allow for multiple taxonomic systems
            let mut taxon = Taxon::from(map);
            if let Some(op) = ops.first() {
                taxon.dataset_id.clone_from(&op.dataset.global_id);
                taxa.push(taxon);
            }
        }

        // our taxa table has a unique constraint on scientific name and dataset id.
        // some data sources will duplicate a taxon (separate record id) to record
        // multiple accepted names. we sort and deduplicate it here since the relationship
        // between taxa isn't of concern here, just the name itself.
        let spinner = new_spinner("Deduplicating taxa");

        taxa.sort_by(|a, b| {
            a.dataset_id
                .cmp(&b.dataset_id)
                .then_with(|| a.scientific_name.cmp(&b.scientific_name))
        });
        taxa.dedup_by(|a, b| a.scientific_name == b.scientific_name && a.dataset_id == b.dataset_id);

        spinner.finish();
        Ok(taxa)
    }

    pub fn update() -> Result<(), Error> {
        use diesel::upsert::excluded;
        use schema::taxa::dsl::*;

        let mut pool = get_pool()?;
        let mut conn = pool.get()?;

        // reduce the logs and convert the record to the model equivalent. this means
        // linking up the records to the database ids based on a lookup
        let datasets = dataset_lookup(&mut pool)?;

        let mut names = Vec::new();
        let mut records = Vec::new();

        for record in Self::reduce()? {
            let dataset_uuid = datasets.get(&record.dataset_id).expect("Cannot find dataset");

            names.push(models::Name {
                id: Uuid::new_v4(),
                scientific_name: record.scientific_name.clone(),
                canonical_name: record.canonical_name.clone(),
                authorship: record.scientific_name_authorship.clone(),
            });

            records.push(models::Taxon {
                id: Uuid::new_v4(),
                dataset_id: *dataset_uuid,
                parent_id: None,
                entity_id: Some(record.entity_id),
                status: record.taxonomic_status,
                rank: record.taxon_rank,
                scientific_name: record.scientific_name,
                canonical_name: record.canonical_name,
                authorship: record.scientific_name_authorship,
                nomenclatural_code: record.nomenclatural_code,
                citation: record.citation,
                vernacular_names: None,
                description: None,
                remarks: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
        }

        // import all the names in case they don't already exist. we use names to
        // hang data on including taxonomy and is a key method of data discovery
        names.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
        names.dedup_by(|a, b| a.scientific_name.eq(&b.scientific_name));
        super::names::import(&names)?;

        // finally import the operations. if there is a conflict based on the operation_id
        // then it is a duplicate operation so do nothing with it
        let bar = new_progress_bar(records.len(), "Importing taxa");
        for chunk in records.chunks(1000) {
            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(taxa)
                .values(chunk)
                .on_conflict((scientific_name, dataset_id))
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    status.eq(excluded(status)),
                    rank.eq(excluded(rank)),
                    canonical_name.eq(excluded(canonical_name)),
                    authorship.eq(excluded(authorship)),
                    nomenclatural_code.eq(excluded(nomenclatural_code)),
                    citation.eq(excluded(citation)),
                    vernacular_names.eq(excluded(vernacular_names)),
                    description.eq(excluded(description)),
                    remarks.eq(excluded(remarks)),
                    updated_at.eq(excluded(updated_at)),
                ))
                .execute(&mut conn)?;

            bar.inc(1000);
        }

        bar.finish();
        info!(total = records.len(), "Taxa import finished");
        Ok(())
    }

    /// Link all taxa to their parent taxon.
    /// Due to bulk upserts we don't want to set the parent_id for taxa as it is self-referential.
    /// It is important to link them up after import to enable the taxonomy DAG within the database.
    /// This will also refresh the necessary materialized views in the database which may take a while.
    pub fn link() -> Result<(), Error> {
        use schema::taxa::dsl::*;
        use schema::taxon_names;

        let mut pool = get_pool()?;
        let mut conn = pool.get()?;

        // we want to link all taxa datasets so include all of them
        let datasets = dataset_lookup(&mut pool)?;
        let dataset_ids = datasets.values().cloned().collect();
        let names = name_lookup(&mut pool)?;
        let all_taxa = taxon_lookup(&mut pool, &dataset_ids)?;

        let mut links: Vec<(Uuid, Uuid)> = Vec::new();
        let mut name_links: Vec<(Uuid, Uuid)> = Vec::new();

        for record in Self::reduce()? {
            if let Some(dataset_uuid) = datasets.get(&record.dataset_id) {
                let taxon_lookup = (*dataset_uuid, record.scientific_name.clone());
                if let Some(taxon_uuid) = all_taxa.get(&taxon_lookup) {
                    // add a name link if the name can be found in the database
                    if let Some(name_uuid) = names.get(&record.scientific_name) {
                        name_links.push((*taxon_uuid, *name_uuid));
                    }

                    // add a parent link if the taxon can be found in the database
                    if let Some(parent) = record.parent_taxon {
                        if let Some(parent_uuid) = all_taxa.get(&(*dataset_uuid, parent)) {
                            links.push((*taxon_uuid, *parent_uuid));
                        }
                    }
                }
            }
        }

        // this closure allows us to get a new connection per worker thread
        // that rayon spawns with the parallel iterator.
        let get_conn = || pool.get().unwrap();

        // we cant do a bulk update without resorting to upserts so instead
        // we use rayon to parallelize to greatly increase the speed
        let bar = new_progress_bar(links.len(), "Updating parent links");
        links
            .par_iter()
            .progress_with(bar)
            .for_each_init(get_conn, |conn, (taxon_uuid, parent_uuid)| {
                diesel::update(taxa.filter(id.eq(taxon_uuid)))
                    .set(parent_id.eq(parent_uuid))
                    .execute(conn)
                    .expect("Failed to update");
            });

        // all data links to a 'name' so that we can use different taxonomic systems represent
        // the same 'concept' that other data refers to. the taxon_names table provides this
        // and at a minimum every taxon should link to one name via this through table.
        let bar = new_progress_bar(name_links.len(), "Importing taxon name links");
        for chunk in name_links.chunks(10_000) {
            let mut values = Vec::with_capacity(chunk.len());
            for (taxon_uuid, name_uuid) in chunk {
                values.push((taxon_names::taxon_id.eq(taxon_uuid), taxon_names::name_id.eq(name_uuid)))
            }

            diesel::insert_into(taxon_names::table)
                .values(values)
                .on_conflict((taxon_names::taxon_id, taxon_names::name_id))
                .do_nothing()
                .execute(&mut conn)?;

            bar.inc(10_000);
        }
        bar.finish();

        // refresh the views that cache taxa data
        refresh_materialized_view(&mut pool, MaterializedView::TaxaDag)?;
        refresh_materialized_view(&mut pool, MaterializedView::TaxaDagDown)?;
        refresh_materialized_view(&mut pool, MaterializedView::TaxaTree)?;
        refresh_materialized_view(&mut pool, MaterializedView::TaxaTreeStats)?;
        refresh_materialized_view(&mut pool, MaterializedView::Species)?;

        Ok(())
    }
}

/// Converts a LWW CRDT map of taxon atoms to a Taxon record for serialisation
impl From<Map<TaxonAtom>> for Taxon {
    fn from(value: Map<TaxonAtom>) -> Self {
        use TaxonAtom::*;

        let mut taxon = Taxon {
            entity_id: value.entity_id,
            ..Default::default()
        };

        for val in value.atoms.into_values() {
            match val {
                Empty => {}
                TaxonId(value) => taxon.taxon_id = value,
                ParentTaxon(value) => taxon.parent_taxon = Some(value),
                ScientificName(value) => taxon.scientific_name = value,
                Authorship(value) => taxon.scientific_name_authorship = Some(value),
                CanonicalName(value) => taxon.canonical_name = value,
                NomenclaturalCode(value) => taxon.nomenclatural_code = value,
                TaxonomicRank(value) => taxon.taxon_rank = value,
                TaxonomicStatus(value) => taxon.taxonomic_status = value,
                Citation(value) => taxon.citation = Some(value),
                References(value) => taxon.references = Some(value),
                LastUpdated(value) => taxon.last_updated = Some(value),

                // we want this atom for provenance and reproduction with the hash
                // generation but we don't need to actually use it
                EntityId(_value) => {}

                // fields currently not supported
                AcceptedNameUsageId(_value) => {}
                ParentNameUsageId(_value) => {}
                AcceptedNameUsage(_value) => {}
                ParentNameUsage(_value) => {}
                NomenclaturalStatus(_value) => {}
                NamePublishedIn(_value) => {}
                NamePublishedInYear(_value) => {}
                NamePublishedInUrl(_value) => {}
            }
        }

        taxon
    }
}

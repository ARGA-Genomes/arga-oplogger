use std::io::Read;

use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{
    self,
    DatasetVersion,
    TaxonAtom,
    TaxonOperation,
    TaxonOperationWithDataset,
    TaxonomicRank,
    TaxonomicStatus,
};
use arga_core::schema;
use diesel::*;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::database::{
    dataset_lookup,
    get_pool,
    name_lookup,
    refresh_materialized_view,
    taxon_lookup,
    FrameLoader,
    MaterializedView,
    PgPool,
    StringMap,
    UuidStringMap,
};
use crate::errors::{Error, LookupError, ReduceError};
use crate::frames::IntoFrame;
use crate::operations::group_operations;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::{taxonomic_rank_from_str, taxonomic_status_from_str, titleize_first_word, UpdateBars};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type TaxonFrame = DataFrame<TaxonAtom>;


impl OperationLoader for FrameLoader<TaxonOperation> {
    type Operation = TaxonOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::taxa_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = taxa_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(taxa_logs::all_columns())
            .order(operation_id.asc())
            .load::<TaxonOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::taxa_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = taxa_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(taxa_logs::all_columns())
            .order(operation_id.asc())
            .load::<TaxonOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[Self::Operation]) -> Result<usize, Error> {
        use schema::taxa_logs::dsl::*;
        let mut conn = self.pool.get()?;

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

    /// The dataset id used to isolate the taxa from other systems
    dataset_id: String,

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
        frame.push(DatasetId(self.dataset_id));
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

    /// The internal identifier of the source dataset as determined by ARGA
    dataset_uuid: Uuid,

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

pub struct TaxonLink {
    dataset_id: Uuid,
    name_id: Uuid,
    taxon_id: Uuid,
    parent_id: Option<Uuid>,
}


pub fn import<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, TaxonOperation>(stream, dataset)
}


pub fn update2() -> Result<(), Error> {
    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let total = {
        use diesel::dsl::count_distinct;
        use schema::taxa::dsl::*;

        taxa.select(count_distinct(entity_id)).get_result::<i64>(&mut conn)?
    };

    let limit = 10_000;
    let offsets: Vec<i64> = (0..total).step_by(limit as usize).collect();

    offsets
        .into_par_iter()
        .try_for_each(|offset| reduce_and_update(pool.clone(), offset, limit))?;

    Ok(())
}

fn reduce_chunk(pool: PgPool, offset: i64, limit: i64) -> Result<Vec<Taxon>, Error> {
    let mut conn = pool.get()?;

    let operations = {
        use schema::taxa_logs::dsl::*;
        use schema::{dataset_versions, datasets};

        // we first get all the entity ids within a specified range. this means that the
        // query here has to return the same amount of results as a COUNT DISTINCT query otherwise
        // some entities will be missed during the update. in particular make sure to always order
        // the query results otherwise random entities might get pulled in since postgres doesnt sort by default
        let entity_ids = taxa_logs
            .select(entity_id)
            .group_by(entity_id)
            .order_by(entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        // get the operations for the entities making sure to order by operation id so that
        // the CRDT structs can do their thing
        taxa_logs
            .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .filter(entity_id.eq_any(entity_ids))
            .order_by((entity_id, operation_id))
            .load::<TaxonOperationWithDataset>(&mut conn)?
    };

    // group the entity operations up and preparing it for use in the LWW map
    let entities = group_operations(operations, vec![]);
    let mut reduced_records = Vec::new();

    // reduce all the operations by applying them to an empty record
    // as per the last write wins policy
    for (key, ops) in entities.into_iter() {
        let mut map = Map::new(key);
        map.reduce(&ops);

        // include the dataset uuid in the reduced output to
        // allow for multiple taxonomic systems
        let mut record = Taxon::from(map);
        if let Some(op) = ops.first() {
            record.dataset_uuid.clone_from(&op.dataset.id);
            reduced_records.push(record);
        }
    }

    // our taxa table has a unique constraint on scientific name and dataset id.
    // some data sources will duplicate a taxon (separate record id) to record
    // multiple accepted names. we sort and deduplicate it here since the relationship
    // between taxa isn't of concern here, just the name itself.
    reduced_records.sort_by(|a, b| {
        a.dataset_id
            .cmp(&b.dataset_id)
            .then_with(|| a.scientific_name.cmp(&b.scientific_name))
    });
    reduced_records.dedup_by(|a, b| a.scientific_name == b.scientific_name && a.dataset_id == b.dataset_id);

    Ok(reduced_records)
}

pub fn reduce_and_update(pool: PgPool, offset: i64, limit: i64) -> Result<(), Error> {
    let reduced_records = reduce_chunk(pool.clone(), offset, limit)?;

    let mut names = Vec::new();
    let mut records = Vec::new();

    for record in reduced_records {
        names.push(models::Name {
            id: Uuid::new_v4(),
            scientific_name: record.scientific_name.clone(),
            canonical_name: record.canonical_name.clone(),
            authorship: record.scientific_name_authorship.clone(),
        });

        records.push(models::Taxon {
            id: Uuid::new_v4(),
            dataset_id: record.dataset_uuid,
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
    super::names::import(pool.clone(), &names)?;

    // postgres always creates a new row version so we cant get
    // an actual figure of the amount of records changed
    {
        use diesel::upsert::excluded;
        use schema::taxa::dsl::*;
        let mut conn = pool.get()?;

        for chunk in records.chunks(1000) {
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
        }
    }

    Ok(())
}


pub fn link2() -> Result<(), Error> {
    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let total = {
        use diesel::dsl::count_distinct;
        use schema::taxa::dsl::*;

        taxa.select(count_distinct(entity_id)).get_result::<i64>(&mut conn)?
    };

    let limit = 10_000;
    let offsets: Vec<i64> = (0..total).step_by(limit as usize).collect();

    offsets
        .into_par_iter()
        .try_for_each(|offset| link_and_update(pool.clone(), offset, limit))?;


    // refresh the views that cache taxa data
    // refresh_materialized_view(&mut pool, MaterializedView::TaxaDag)?;
    // refresh_materialized_view(&mut pool, MaterializedView::TaxaDagDown)?;
    // refresh_materialized_view(&mut pool, MaterializedView::TaxaTree)?;
    // refresh_materialized_view(&mut pool, MaterializedView::TaxaTreeStats)?;
    // refresh_materialized_view(&mut pool, MaterializedView::Species)?;

    Ok(())
}

pub fn link_and_update(mut pool: PgPool, offset: i64, limit: i64) -> Result<(), Error> {
    let reduced_records = reduce_chunk(pool.clone(), offset, limit)?;

    let mut dataset_ids: Vec<Uuid> = reduced_records.iter().map(|r| r.dataset_uuid).collect();
    dataset_ids.sort();
    dataset_ids.dedup();

    let names = name_lookup(&mut pool)?;
    let all_taxa = taxon_lookup(&mut pool, &dataset_ids)?;

    let mut links: Vec<(Uuid, Uuid)> = Vec::new();
    let mut name_links: Vec<(Uuid, Uuid)> = Vec::new();

    for record in reduced_records {
        let taxon_key = (record.dataset_uuid, record.scientific_name.clone());
        let taxon_match = all_taxa.get(&taxon_key);
        let name_match = names.get(&record.scientific_name);

        match (taxon_match, name_match) {
            (Some(taxon_uuid), Some(name_uuid)) => {
                // add a name link if the name can be found in the database
                name_links.push((*taxon_uuid, *name_uuid));

                // add a parent link if the taxon can be found in the database
                if let Some(parent) = record.parent_taxon {
                    if let Some(parent_uuid) = all_taxa.get(&(record.dataset_uuid, parent)) {
                        links.push((*taxon_uuid, *parent_uuid));
                    }
                }
            }

            (None, None) => warn!("link failed. neither taxon nor name found in database"),
            (Some(_), None) => warn!(record.scientific_name, "link failed. taxon found but not the name"),
            (None, Some(_)) => warn!(record.scientific_name, "link failed. name found but not the taxon"),
        };
    }

    // this closure allows us to get a new connection per worker thread
    // that rayon spawns with the parallel iterator.
    let get_conn = || pool.get_timeout(std::time::Duration::from_secs(1)).unwrap();

    // we cant do a bulk update without resorting to upserts so instead
    // we use rayon to parallelize to greatly increase the speed
    links
        .par_iter()
        .for_each_init(get_conn, |conn, (taxon_uuid, parent_uuid)| {
            use schema::taxa::dsl::*;

            diesel::update(taxa.filter(id.eq(taxon_uuid)))
                .set(parent_id.eq(parent_uuid))
                .execute(conn)
                .expect("Failed to update");
        });

    // all data links to a 'name' so that we can use different taxonomic systems represent
    // the same 'concept' that other data refers to. the taxon_names table provides this
    // and at a minimum every taxon should link to one name via this through table.
    for chunk in name_links.chunks(10_000) {
        use schema::taxon_names::dsl::*;
        let mut conn = pool.get()?;

        let mut values = Vec::with_capacity(chunk.len());
        for (taxon_uuid, name_uuid) in chunk {
            values.push((taxon_id.eq(taxon_uuid), name_id.eq(name_uuid)))
        }

        diesel::insert_into(taxon_names)
            .values(values)
            .on_conflict((taxon_id, name_id))
            .do_nothing()
            .execute(&mut conn)?;
    }

    Ok(())
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
                DatasetId(_value) => {}
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


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let lookups = Lookups {
        datasets: dataset_lookup(&mut pool)?,
    };

    let pager: FrameLoader<TaxonOperation> = FrameLoader::new(pool.clone());

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total_entities = pager.total()? as usize;
    let mut bars = UpdateBars::new(total_entities);
    let name_bar = bars.add_progress_bar(total_entities, "Inserting names");

    info!(total_entities, "Reducing taxa");

    let reducer: DatabaseReducer<models::Taxon, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::names;
            use schema::taxa::dsl::*;

            let mut valid_records = Vec::new();
            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record.clone()),
                    Err(err) => error!(?err),
                }
            }

            // insert the names as well as they'll need to be used for linking later
            let mut names: Vec<models::Name> = valid_records.iter().map(|r| models::Name::from(r.clone())).collect();
            names.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
            names.dedup_by(|a, b| a.scientific_name.eq(&b.scientific_name));

            diesel::insert_into(names::table)
                .values(names)
                .on_conflict(names::scientific_name)
                .do_update()
                .set((
                    names::canonical_name.eq(excluded(names::canonical_name)),
                    names::authorship.eq(excluded(names::authorship)),
                ))
                .execute(&mut conn)?;

            name_bar.inc(chunk.len() as u64);

            valid_records.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
            valid_records.dedup_by(|a, b| a.dataset_id.eq(&b.dataset_id) && a.scientific_name.eq(&b.scientific_name));

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(taxa)
                .values(valid_records)
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

            bars.records.inc(chunk.len() as u64);
        }
    }

    bars.finish();
    info!("Finished reducing and updating taxa");

    Ok(())
}


pub fn link() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let datasets = dataset_lookup(&mut pool)?;
    let dataset_ids: Vec<Uuid> = datasets.values().map(|id| id.clone()).collect();

    let lookups = LinkLookups {
        datasets,
        names: name_lookup(&mut pool)?,
        taxa: taxon_lookup(&mut pool, &dataset_ids)?,
    };

    let pager: FrameLoader<TaxonOperation> = FrameLoader::new(pool.clone());

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total_entities = pager.total()? as usize;
    info!(total_entities, "Linking taxa");

    let mut bars = UpdateBars::new(total_entities);
    bars.records.enable_steady_tick(std::time::Duration::from_millis(500));

    let reducer: DatabaseReducer<TaxonLink, _, _> = DatabaseReducer::new(pager, lookups);

    let mut links: Vec<(Uuid, Uuid)> = Vec::new();
    let mut name_links: Vec<(Uuid, Uuid)> = Vec::new();

    for chunk in reducer.into_iter() {
        let mut records = Vec::new();
        for record in chunk {
            match record {
                Ok(record) => records.push(record),
                Err(err) => error!(?err),
            }
        }

        for record in records {
            name_links.push((record.taxon_id, record.name_id));

            if let Some(parent_id) = record.parent_id {
                links.push((record.taxon_id, parent_id));
            }

            bars.records.inc(1);
        }
    }

    let name_bar = bars.add_progress_bar(total_entities, "Updating name links");
    let parent_bar = bars.add_progress_bar(links.len(), "Updating parent links");

    // this closure allows us to get a new connection per worker thread
    // that rayon spawns with the parallel iterator.
    let get_conn = || pool.get_timeout(std::time::Duration::from_secs(1)).unwrap();

    // we cant do a bulk update without resorting to upserts so instead
    // we use rayon to parallelize to greatly increase the speed
    links
        .par_iter()
        .progress_with(parent_bar)
        .for_each_init(get_conn, |conn, (taxon_uuid, parent_uuid)| {
            use schema::taxa::dsl::*;

            diesel::update(taxa.filter(id.eq(taxon_uuid)))
                .set(parent_id.eq(parent_uuid))
                .execute(conn)
                .expect("Failed to update");
        });


    let mut conn = pool.get()?;

    // all data links to a 'name' so that we can use different taxonomic systems represent
    // the same 'concept' that other data refers to. the taxon_names table provides this
    // and at a minimum every taxon should link to one name via this through table.
    for chunk in name_links.chunks(10_000) {
        use schema::taxon_names::dsl::*;

        let mut values = Vec::with_capacity(chunk.len());
        for (taxon_uuid, name_uuid) in chunk {
            values.push((taxon_id.eq(taxon_uuid), name_id.eq(name_uuid)))
        }

        diesel::insert_into(taxon_names)
            .values(values)
            .on_conflict((taxon_id, name_id))
            .do_nothing()
            .execute(&mut conn)?;

        name_bar.inc(chunk.len() as u64);
    }

    bars.finish();
    Ok(())
}


struct Lookups {
    datasets: StringMap,
}

struct LinkLookups {
    datasets: StringMap,
    taxa: UuidStringMap,
    names: StringMap,
}

impl Reducer<LinkLookups> for TaxonLink {
    type Atom = TaxonAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &LinkLookups) -> Result<Self, Error> {
        use TaxonAtom::*;

        let mut dataset_id = None;
        let mut parent_taxon = None;
        let mut scientific_name = None;

        for atom in atoms {
            match atom {
                DatasetId(value) => dataset_id = Some(value),
                ParentTaxon(value) => parent_taxon = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                _ => {}
            }
        }

        let dataset_id = dataset_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "DatasetId".to_string()))?;
        let dataset_id = lookups
            .datasets
            .get(&dataset_id)
            .ok_or(LookupError::Dataset(dataset_id))?
            .clone();

        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;
        let name_id = lookups
            .names
            .get(&scientific_name)
            .ok_or(LookupError::Name(scientific_name.clone()))?
            .clone();

        let taxon_key = (dataset_id, scientific_name.clone());
        let taxon_id = lookups
            .taxa
            .get(&taxon_key)
            .ok_or(LookupError::Name(scientific_name.clone()))?
            .clone();

        let parent_id = match parent_taxon {
            Some(parent) => {
                let parent_key = (dataset_id, parent);
                match lookups.taxa.get(&parent_key) {
                    Some(parent_id) => Some(parent_id.clone()),
                    None => {
                        warn!(?parent_key, "Cannot find parent taxon. Skipping");
                        None
                    }
                }
            }
            None => None,
        };

        let record = TaxonLink {
            dataset_id,
            taxon_id,
            name_id,
            parent_id,
        };
        Ok(record)
    }
}

impl Reducer<Lookups> for models::Taxon {
    type Atom = TaxonAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use TaxonAtom::*;

        let mut dataset_id = None;
        let mut taxon_id = None;
        let mut parent_taxon = None;
        let mut scientific_name = None;
        let mut canonical_name = None;
        let mut authorship = None;
        let mut nomenclatural_code = None;
        let mut taxonomic_rank = None;
        let mut taxonomic_status = None;
        let mut citation = None;
        let mut references = None;
        let mut last_updated = None;

        for atom in atoms {
            match atom {
                Empty => {}
                DatasetId(value) => dataset_id = Some(value),
                TaxonId(value) => taxon_id = Some(value),
                ParentTaxon(value) => parent_taxon = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                CanonicalName(value) => canonical_name = Some(value),
                Authorship(value) => authorship = Some(value),
                NomenclaturalCode(value) => nomenclatural_code = Some(value),
                TaxonomicRank(value) => taxonomic_rank = Some(value),
                TaxonomicStatus(value) => taxonomic_status = Some(value),
                Citation(value) => citation = Some(value),
                References(value) => references = Some(value),
                LastUpdated(value) => last_updated = Some(value),

                // we want this atom for provenance and reproduction with the hash
                // generation but we don't need to actually use it
                EntityId(_value) => {}

                // fields currently not supported
                AcceptedNameUsageId(_) => {}
                ParentNameUsageId(_) => {}
                AcceptedNameUsage(_) => {}
                ParentNameUsage(_) => {}
                NomenclaturalStatus(_) => {}
                NamePublishedIn(_) => {}
                NamePublishedInYear(_) => {}
                NamePublishedInUrl(_) => {}
            }
        }

        let dataset_id = dataset_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "DatasetId".to_string()))?;
        let dataset_id = lookups
            .datasets
            .get(&dataset_id)
            .ok_or(LookupError::Dataset(dataset_id))?;

        let record = models::Taxon {
            id: uuid::Uuid::new_v4(),
            entity_id: Some(entity_id),
            dataset_id: dataset_id.clone(),
            parent_id: None,
            status: taxonomic_status.expect("taxonomic status not found"),
            rank: taxonomic_rank.expect("taxonomic rank not found"),
            scientific_name: scientific_name.expect("scientific name not found"),
            canonical_name: canonical_name.expect("canonical name not found"),
            authorship,
            nomenclatural_code: nomenclatural_code.expect("nomenclatural code not found"),
            citation,
            vernacular_names: None,
            description: None,
            remarks: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        Ok(record)
    }
}


impl EntityPager for FrameLoader<TaxonOperation> {
    type Operation = models::TaxonOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;

        let total = {
            use diesel::dsl::count_distinct;
            use schema::taxa_logs::dsl::*;
            taxa_logs
                .select(count_distinct(entity_id))
                .get_result::<i64>(&mut conn)?
        };

        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::taxa_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let limit = 10_000;
        let offset = page as i64 * limit;

        let entity_ids = taxa_logs
            .select(entity_id)
            .group_by(entity_id)
            .order_by(entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = taxa_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by((entity_id, operation_id))
            .load::<TaxonOperation>(&mut conn)?;

        Ok(operations)
    }
}

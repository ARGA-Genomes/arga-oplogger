use std::collections::HashMap;
use std::time::Duration;

use arga_core::models::DatasetVersion;
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::*;
use tracing::info;
use uuid::Uuid;

use crate::errors::Error;
use crate::utils::new_spinner;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

/// A String map. The value is a Uuid associated with the string. For example, a
/// name of a dataset stored in this map will return the dataset id when queried.
pub type StringMap = HashMap<String, Uuid>;

/// A Uuid + String map. The key is a tuple of a uuid and string to allow
/// for scoping such as all strings from a specific dataset
pub type UuidStringMap = HashMap<(Uuid, String), Uuid>;

/// A refreshable materialized view
pub enum MaterializedView {
    TaxaDag,
    TaxaDagDown,
    TaxaTree,
    TaxaTreeStats,
    Species,
}

impl std::fmt::Display for MaterializedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MaterializedView::TaxaDag => "taxa_dag",
            MaterializedView::TaxaDagDown => "taxa_dag_down",
            MaterializedView::TaxaTree => "taxa_tree",
            MaterializedView::TaxaTreeStats => "taxa_tree_stats",
            MaterializedView::Species => "species",
        })
    }
}

pub fn get_pool() -> Result<PgPool, Error> {
    let url = arga_core::get_database_url();
    let manager = ConnectionManager::<PgConnection>::new(url);
    let pool = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .max_size(30)
        .build(manager)?;
    Ok(pool)
}

fn find_dataset_id(dataset_id: &str) -> Result<Uuid, Error> {
    use schema::datasets::dsl::*;

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let uuid = datasets
        .filter(global_id.eq(dataset_id))
        .select(id)
        .get_result::<Uuid>(&mut conn)?;
    Ok(uuid)
}

pub fn create_dataset_version(dataset_id: &str, version: &str, created_at: &str) -> Result<DatasetVersion, Error> {
    use schema::dataset_versions;

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let dataset_version = diesel::insert_into(dataset_versions::table)
        .values(DatasetVersion {
            id: Uuid::new_v4(),
            dataset_id: find_dataset_id(dataset_id)?,
            version: version.to_string(),
            created_at: DateTime::parse_from_rfc3339(created_at).unwrap().to_utc(),
            imported_at: Utc::now(),
        })
        .returning(DatasetVersion::as_select())
        .get_result(&mut conn)?;

    Ok(dataset_version)
}

/// Refreshes a materialized view.
/// This can be a costly operation depending on the view being refreshed.
/// Because we cant use bound parameters on this query we instead use an enum to
/// ensure that user generated content never gets injected.
pub fn refresh_materialized_view(pool: &mut PgPool, name: MaterializedView) -> Result<(), Error> {
    let mut conn = pool.get()?;
    let spinner = new_spinner(&format!("Refreshing {name}"));
    sql_query(format!("REFRESH MATERIALIZED VIEW {name}")).execute(&mut conn)?;
    spinner.finish();
    Ok(())
}

pub fn dataset_lookup(pool: &mut PgPool) -> Result<StringMap, Error> {
    use schema::datasets::dsl::*;
    info!("Creating dataset map");

    let mut conn = pool.get()?;

    let results: Vec<(Uuid, String)> = datasets.select((id, global_id)).load::<(Uuid, String)>(&mut conn)?;

    let mut map = StringMap::new();
    for (uuid, lookup) in results {
        map.insert(lookup, uuid);
    }

    info!(total = map.len(), "Creating dataset map finished");
    Ok(map)
}

pub fn taxon_lookup(pool: &mut PgPool, datasets: &Vec<Uuid>) -> Result<UuidStringMap, Error> {
    use schema::taxa::dsl::*;
    info!(?datasets, "Creating taxa map");

    let mut conn = pool.get()?;

    let results = taxa
        .select((id, dataset_id, scientific_name))
        .filter(dataset_id.eq_any(datasets))
        .load::<(Uuid, Uuid, String)>(&mut conn)?;

    let mut map = UuidStringMap::new();
    for (uuid, dataset_uuid, lookup) in results {
        map.insert((dataset_uuid, lookup), uuid);
    }

    info!(total = map.len(), "Creating taxa map finished");
    Ok(map)
}

pub fn name_lookup(pool: &mut PgPool) -> Result<StringMap, Error> {
    use schema::names::dsl::*;
    info!("Creating name map");

    let mut conn = pool.get()?;

    let results = names.select((id, scientific_name)).load::<(Uuid, String)>(&mut conn)?;

    let mut map = StringMap::new();
    for (uuid, lookup) in results {
        map.insert(lookup, uuid);
    }

    info!(total = map.len(), "Creating name map finished");
    Ok(map)
}

pub fn name_publication_lookup(pool: &mut PgPool) -> Result<StringMap, Error> {
    use schema::name_publications::dsl::*;
    info!("Creating name publication map");

    let mut conn = pool.get()?;

    let results = name_publications
        .select((id, citation))
        .load::<(Uuid, Option<String>)>(&mut conn)?;

    let mut map = StringMap::new();
    for (uuid, lookup) in results {
        if let Some(lookup) = lookup {
            map.insert(lookup, uuid);
        }
    }

    info!(total = map.len(), "Creating name publication map finished");
    Ok(map)
}


#[derive(Clone)]
pub struct FrameLoader<T> {
    pub pool: PgPool,
    marker: std::marker::PhantomData<T>,
}

impl<T> FrameLoader<T> {
    pub fn new(pool: PgPool) -> FrameLoader<T> {
        FrameLoader {
            pool,
            marker: std::marker::PhantomData,
        }
    }
}

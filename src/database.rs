use std::collections::HashMap;

use arga_core::models::DatasetVersion;
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::*;
use tracing::info;
use uuid::Uuid;

use crate::errors::Error;


type PgPool = Pool<ConnectionManager<PgConnection>>;


pub fn get_pool() -> Result<PgPool, Error> {
    let url = arga_core::get_database_url();
    let manager = ConnectionManager::<PgConnection>::new(url);
    let pool = Pool::builder().build(manager)?;
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


/// A String map. The value is a Uuid associated with the string. For example, a
/// name of a dataset stored in this map will return the dataset id when queried.
pub type StringMap = HashMap<String, Uuid>;

/// A Uuid + String map. The key is a tuple of a uuid and string to allow
/// for scoping such as all strings from a specific dataset
pub type UuidStringMap = HashMap<(Uuid, String), Uuid>;

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

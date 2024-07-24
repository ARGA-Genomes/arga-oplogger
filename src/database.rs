use arga_core::models::DatasetVersion;
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::*;
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

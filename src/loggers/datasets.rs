use arga_core::schema::datasets;
use arga_core::{models, schema};
use diesel::*;
use std::path::PathBuf;

use crate::database::{get_pool, source_lookup};
use crate::errors::Error;
use crate::utils::{access_pill_status_from_str, content_type_from_str, data_reuse_status_from_str};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::errors::LookupError;
use arga_core::models::AccessRightsStatus;
use arga_core::models::DataReuseStatus;
use arga_core::models::Dataset;
use arga_core::models::SourceContentType;

pub struct Datasets {
    pub path: PathBuf,
}

#[derive(Deserialize, Debug)]
struct CSVRecord {
    source_name: String,
    global_id: String,
    name: String,
    short_name: Option<String>,
    url: Option<String>,
    citation: Option<String>,
    description: Option<String>,
    license: Option<String>,
    rights_holder: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(deserialize_with = "data_reuse_status_from_str")]
    reuse_pill: Option<DataReuseStatus>,
    #[serde(deserialize_with = "access_pill_status_from_str")]
    access_pill: Option<AccessRightsStatus>,
    publication_year: Option<i16>,
    #[serde(deserialize_with = "content_type_from_str")]
    content_type: Option<SourceContentType>,
}

impl From<CSVRecord> for Dataset {
    fn from(value: CSVRecord) -> Dataset {
        Dataset {
            id: Uuid::new_v4(),
            source_id: get_source_id(&value.source_name).unwrap(),
            global_id: value.global_id,
            name: value.name,
            short_name: value.short_name,
            description: None,
            url: value.url,
            citation: value.citation,
            license: value.license,
            rights_holder: value.rights_holder,
            created_at: value.created_at,
            updated_at: value.updated_at,
            reuse_pill: value.reuse_pill,
            access_pill: value.access_pill,
            publication_year: value.publication_year,
            content_type: value.content_type,
        }
    }
}

fn get_source_id(source_name: &str) -> Result<Uuid, Error> {
    let mut pool = get_pool()?;
    let sources = source_lookup(&mut pool)?;

    let source_uuid = sources
        .get(source_name)
        .ok_or_else(|| Error::Lookup(LookupError::Source(source_name.to_string())))?;

    Ok(*source_uuid)
}

impl Datasets {
    /// Import datasets if they are not already in the table. This is an upsert and will
    /// update the data if it matches on dataset name.
    pub fn import(&self) -> Result<(), Error> {
        use diesel::upsert::excluded;

        let mut reader = csv::Reader::from_path(&self.path)?;
        let records = reader.deserialize();

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        for result in records {
            let record: CSVRecord = result?;
            let dataset_record = Dataset::from(record);

            diesel::insert_into(datasets::table)
                .values(&dataset_record)
                .on_conflict(datasets::global_id)
                .do_update()
                .set((
                    datasets::source_id.eq(excluded(datasets::source_id)),
                    datasets::global_id.eq(excluded(datasets::global_id)),
                    datasets::short_name.eq(excluded(datasets::short_name)),
                    datasets::description.eq(excluded(datasets::description)),
                    datasets::url.eq(excluded(datasets::url)),
                    datasets::citation.eq(excluded(datasets::citation)),
                    datasets::license.eq(excluded(datasets::license)),
                    datasets::rights_holder.eq(excluded(datasets::rights_holder)),
                    datasets::created_at.eq(excluded(datasets::created_at)),
                    datasets::updated_at.eq(excluded(datasets::updated_at)),
                    datasets::reuse_pill.eq(excluded(datasets::reuse_pill)),
                    datasets::access_pill.eq(excluded(datasets::access_pill)),
                    datasets::publication_year.eq(excluded(datasets::publication_year)),
                    datasets::content_type.eq(excluded(datasets::content_type)),
                ))
                .execute(&mut conn)?;
        }

        Ok(())
    }
}

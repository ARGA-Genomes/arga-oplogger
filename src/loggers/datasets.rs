use arga_core::schema::datasets;
use arga_core::{models, schema};
use diesel::*;
use std::path::PathBuf;

use crate::database::get_pool;
use crate::errors::Error;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use arga_core::models::AccessRightsStatus;
use arga_core::models::DataReuseStatus;
use arga_core::models::Dataset;
use arga_core::models::SourceContentType;

pub struct Datasets {
    pub path: PathBuf,
}

#[derive(Deserialize)]
struct CSVRecord {
    id: Uuid,
    source_id: Uuid,
    global_id: String,
    name: String,
    short_name: Option<String>,
    description: Option<String>,
    url: Option<String>,
    citation: Option<String>,
    license: Option<String>,
    rights_holder: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    reuse_pill: Option<DataReuseStatus>,
    access_pill: Option<AccessRightsStatus>,
    publication_year: Option<i16>,
    content_type: Option<SourceContentType>,
}

impl From<CSVRecord> for Dataset {
    fn from(value: CSVRecord) -> Dataset {
        Dataset {
            id: value.id,
            source_id: value.source_id,
            global_id: value.global_id,
            name: value.name,
            short_name: value.short_name,
            description: value.description,
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
                .on_conflict(datasets::name)
                .do_update()
                .set((
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

use std::path::PathBuf;

use arga_core::models::AccessRightsStatus;
use arga_core::models::DataReuseStatus;
use arga_core::models::Source;
use arga_core::models::SourceContentType;
use arga_core::schema::sources;
use arga_core::{models, schema};
use diesel::*;

use crate::database::get_pool;
use crate::errors::Error;
use serde::Deserialize;
use uuid::Uuid;

pub struct Sources {
    pub path: PathBuf,
}

#[derive(Deserialize)]
struct CSVRecord {
    id: Uuid,
    name: String,
    author: String,
    license: String,
    reuse_pill: Option<DataReuseStatus>,
    access_rights: String,
    access_pill: Option<AccessRightsStatus>,
    rights_holder: String,
    content_type: Option<SourceContentType>,
}

impl From<CSVRecord> for Source {
    fn from(value: CSVRecord) -> Source {
        Source {
            id: value.id,
            name: value.name,
            author: value.author,
            rights_holder: value.rights_holder,
            access_rights: value.access_rights,
            license: value.license,
            reuse_pill: value.reuse_pill,
            access_pill: value.access_pill,
            content_type: value.content_type,
        }
    }
}

impl Sources {
    /// Import sources if they are not already in the table. This is an upsert and will
    /// update the data if it matches on source name.
    pub fn import(&self) -> Result<(), Error> {
        use diesel::upsert::excluded;

        let mut reader = csv::Reader::from_path(&self.path)?;
        let records = reader.deserialize();

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        for result in records {
            let record: CSVRecord = result?;
            let source_record = Source::from(record);

            diesel::insert_into(sources::table)
                .values(&source_record)
                .on_conflict(sources::name)
                .do_update()
                .set((
                    sources::author.eq(excluded(sources::author)),
                    sources::rights_holder.eq(excluded(sources::rights_holder)),
                    sources::access_rights.eq(excluded(sources::access_rights)),
                    sources::license.eq(excluded(sources::license)),
                    sources::reuse_pill.eq(excluded(sources::reuse_pill)),
                    sources::access_pill.eq(excluded(sources::access_pill)),
                    sources::content_type.eq(excluded(sources::content_type)),
                ))
                .execute(&mut conn)?;
        }

        Ok(())
    }
}

use arga_core::{models, schema};
use diesel::*;
use tracing::info;

use crate::database::get_pool;
use crate::errors::Error;
use crate::utils::new_progress_bar;
use serde::Deserialize;

pub struct Sources {
    pub path: PathBuf,
}

struct CSVRecord {
    name: String,
    author: String,
    license: Option<String>,
    reuse_pill: Option<String>,
    access_rights: Option<String>,
    access_pill: Option<String>,
    rights_holder: Option<String>,
    content_type: Option<String>,
}

impl From<CSVRecord> for Source {
    fn from(value: CSVRecord) -> Source {
        Source {
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

/// Import sources if they are not already in the table. This is an upsert and will
/// update the data if it matches on source name.
pub fn import(&self) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::names::dsl::*;

    let mut reader = csv::Reader::from_path(&self.path)?;
    let records = reader.deserialize();

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let mut total_imported = 0;
    let bar = new_progress_bar(records.len(), "Importing sources");

    for result in records {
        let record: CSVRecord = result?;
        let source = Source::from(record);

        let inserted = diesel::insert_into(sources)
            .values(&source)
            .on_conflict(name)
            .do_update()
            .set((
                author.eq(excluded(author)),
                rights_holder.eq(excluded(rights_holder)),
                access_rights.eq(excluded(access_rights)),
                license.eq(excluded(license)),
                reuse_pill.eq(excluded(reuse_pill)),
                access_pill.eq(excluded(access_pill)),
                content_type.eq(excluded(content_type)),
            ))
            .execute(&mut conn)?;

        total_imported += inserted;
        bar.inc(1);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Sources import finished");
    Ok(())
}

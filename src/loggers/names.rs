use std::io::Read;

use arga_core::{models, schema};
use diesel::*;
use serde::Deserialize;
use tracing::info;

use crate::database::PgPool;
use crate::errors::Error;
use crate::readers::meta;
use crate::utils::new_progress_bar;
use crate::FrameProgress;


/// The CSV name record to import directly.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize, Default)]
struct Record {
    pub scientific_name: String,
    pub scientific_authorship: Option<String>,
    pub canonical_name: String,
}


/// Import names if they are not already in the table. This is an upsert and will
/// update the data if it matches on scientific name
pub fn import(pool: PgPool, records: &[models::Name]) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::names::dsl::*;

    let mut conn = pool.get()?;

    let mut total_imported = 0;
    let bar = new_progress_bar(records.len(), "Importing names");

    for chunk in records.chunks(10_000) {
        let inserted = diesel::insert_into(names)
            .values(chunk)
            .on_conflict(scientific_name)
            .do_update()
            .set((canonical_name.eq(excluded(canonical_name)), authorship.eq(excluded(authorship))))
            .execute(&mut conn)?;

        total_imported += inserted;
        bar.inc(10_000);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Name import finished");
    Ok(())
}


pub fn import_archive<S: Read + FrameProgress>(stream: S) -> Result<(), Error> {
    let bars = stream.bars();

    let input = brotli::Decompressor::new(stream, 4096);
    let mut reader = csv::Reader::from_reader(input);

    let mut names = Vec::new();

    for row in reader.deserialize::<Record>().into_iter() {
        let record = row?;
        bars.frames.inc(1);

        names.push(models::Name {
            id: uuid::Uuid::new_v4(),
            scientific_name: record.scientific_name,
            canonical_name: record.canonical_name,
            authorship: record.scientific_authorship,
        })
    }
    bars.finish();

    // sort and dedup names otherwise we will hit on conflict errors in postres
    names.sort_by(|a, b| a.scientific_name.cmp(&b.scientific_name));
    names.dedup_by(|a, b| a.scientific_name.eq(&b.scientific_name));

    let pool = crate::database::get_pool()?;
    import(pool, &names)
}

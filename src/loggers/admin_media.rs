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


/// The CSV media record to import directly.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize, Default)]
struct Record {
    scientific_name: Option<String>,
    canonical_name: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    url: String,
    reference_url: Option<String>,
    source: Option<String>,
    title: Option<String>,
    description: Option<String>,
    creator: Option<String>,
    publisher: Option<String>,
    license: Option<String>,
    rights_holder: Option<String>,
}


/// Import media if they are not already in the table
pub fn import(pool: PgPool, records: &[models::AdminMedia]) -> Result<(), Error> {
    use schema::admin_media::dsl::*;
    let mut conn = pool.get()?;

    let mut total_imported = 0;
    let bar = new_progress_bar(records.len(), "Importing admin media");

    for chunk in records.chunks(1_000) {
        let inserted = diesel::insert_into(admin_media).values(chunk).execute(&mut conn)?;

        total_imported += inserted;
        bar.inc(1_000);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Admin media import finished");
    Ok(())
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    let bars = stream.bars();

    let mut pool = crate::database::get_pool()?;

    let input = brotli::Decompressor::new(stream, 4096);
    let mut reader = csv::Reader::from_reader(input);

    let names = crate::database::canonical_name_lookup(&mut pool)?;
    let mut media = Vec::new();

    for row in reader.deserialize::<Record>().into_iter() {
        let record = row?;
        bars.frames.inc(1);

        let Some(name_id) = names.get(&record.canonical_name.unwrap_or_default())
        else {
            continue;
        };

        media.push(models::AdminMedia {
            id: uuid::Uuid::new_v4(),
            name_id: name_id.clone(),
            image_source: dataset.name.clone(),
            width: record.width,
            height: record.height,
            url: record.url,
            reference_url: record.reference_url,
            title: record.title,
            description: record.description,
            source: record.source,
            creator: record.creator,
            publisher: record.publisher,
            license: record.license,
            rights_holder: record.rights_holder,
        })
    }
    bars.finish();

    import(pool, &media)
}

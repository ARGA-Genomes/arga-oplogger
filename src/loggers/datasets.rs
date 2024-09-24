use arga_core::{models, schema};
use diesel::*;
use tracing::info;

use crate::database::get_pool;
use crate::errors::Error;
use crate::utils::new_progress_bar;

/// Import datasets if they are not already in the table. This is an upsert and will
/// update the data if it matches on dataset name.
pub fn import(records: &[models::Dataset]) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::names::dsl::*;

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let mut total_imported = 0;
    let bar = new_progress_bar(records.len(), "Importing sources");

    for chunk in records.chunks(10_000) {
        let inserted = diesel::insert_into(sources)
            .values(chunk)
            .on_conflict(name)
            .do_update()
            .set((
                global_id.eq(excluded(global_id)),
                short_name.eq(excluded(short_name)),
                description.eq(excluded(description)),
                url.eq(excluded(url)),
                citation.eq(excluded(citation)),
                license.eq(excluded(license)),
                rights_holder.eq(excluded(rights_holder)),
                created_at.eq(excluded(created_at)),
                updated_at.eq(excluded(updated_at)),
                reuse_pill.eq(excluded(reuse_pill)),
                access_pill.eq(excluded(access_pill)),
                publication_year.eq(excluded(publication_year)),
                content_type.eq(excluded(content_type)),
            ))
            .execute(&mut conn)?;

        total_imported += inserted;
        bar.inc(10_000);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Sources import finished");
    Ok(())
}

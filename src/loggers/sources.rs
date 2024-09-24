use arga_core::{models, schema};
use diesel::*;
use tracing::info;

use crate::database::get_pool;
use crate::errors::Error;
use crate::utils::new_progress_bar;

/// Import sources if they are not already in the table. This is an upsert and will
/// update the data if it matches on source name.
pub fn import(records: &[models::Source]) -> Result<(), Error> {
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
        bar.inc(10_000);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Sources import finished");
    Ok(())
}

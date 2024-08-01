use arga_core::{models, schema};
use diesel::*;
use tracing::info;

use crate::database::get_pool;
use crate::errors::Error;
use crate::utils::new_progress_bar;


/// Import names if they are not already in the table. This is an upsert and will
/// update the data if it matches on scientific name
pub fn import(records: &Vec<models::Name>) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::names::dsl::*;

    let pool = get_pool()?;
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

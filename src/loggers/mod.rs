mod collections;
mod names;
mod nomenclatural_acts;
mod sequences;
mod taxa;
mod taxonomic_acts;


use std::path::PathBuf;

use arga_core::crdt::DataFrameOperation;
use arga_core::models::LogOperation;
pub use collections::Collections;
pub use nomenclatural_acts::NomenclaturalActs;
use rayon::prelude::*;
pub use sequences::Sequences;
use serde::de::DeserializeOwned;
pub use taxa::Taxa;
pub use taxonomic_acts::TaxonomicActs;
use uuid::Uuid;

use crate::database::{get_pool, FrameLoader};
use crate::errors::Error;
use crate::operations::{distinct_changes, Framer};
use crate::readers::csv::{CsvReader, IntoFrame};
use crate::readers::OperationLoader;
use crate::utils::FrameImportBars;


/// A parallel CSV framer and importer.
///
/// This caters for the general path of importing operations logs from a CSV file by treating each
/// row as a single frame and bulk upserting the distinct changes. It also displays a progress bar.
/// Because its a generic function that relies on implemented traits it has to be called as a
/// turbo fish function, ie. `import_csv_as_logs::<Record, TaxonOperation>(&path, &dataset_version_id)`.
///
/// The Reader (<R>) must implement the IntoFrame trait and be deserializable from a CSV file.
/// The Operation (<Op>) must implement the OperationLoader trait
pub fn import_csv_as_logs<R, Op>(path: &PathBuf, dataset_version_id: &Uuid) -> Result<(), Error>
where
    Op: Sync,
    R: DeserializeOwned + IntoFrame,
    R::Atom: Default + Clone + ToString + PartialEq,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<R::Atom> + From<DataFrameOperation<R::Atom>> + Clone + Sync,
{
    // we need a few components to fully import operation logs. the first is a CSV file reader
    // which parses each row and converts it into a frame. the second is a framer which allows
    // us to conveniently get chunks of frames from the reader and sets us up for easy parallelization.
    // and the third is the frame loader which allows us to query the database to deduplicate and
    // pull out unique operations, as well as upsert the new operations.
    let reader: CsvReader<R> = CsvReader::from_path(path.clone(), dataset_version_id.clone())?;
    let bars = FrameImportBars::new(reader.total_rows);
    let framer = Framer::new(reader);
    let loader = FrameLoader::<Op>::new(get_pool()?);

    // parse and convert big chunks of rows. this is an IO bound task but for each
    // chunk we need to query the database and then insert into the database, so
    // we parallelize the frame merging and inserting instead since it is an order of
    // magnitude slower than the parsing
    for frames in framer.chunks(20_000) {
        let total_frames = frames.len();

        // we flatten out all the frames into operations and process them in chunks of 10k.
        // postgres has a parameter limit and by chunking it we can query the database to
        // filter to distinct changes and import it in bulk without triggering any errors.
        frames.operations()?.par_chunks(10_000).try_for_each(|slice| {
            let total = slice.len();

            // compare the ops with previously imported ops and only return actual changes
            let changes = distinct_changes(slice.to_vec(), &loader)?;
            let inserted = loader.upsert_operations(&changes)?;

            bars.inserted.inc(inserted as u64);
            bars.operations.inc(total as u64);
            Ok::<(), Error>(())
        })?;

        bars.total.inc(total_frames as u64);
    }

    bars.finish();
    Ok(())
}

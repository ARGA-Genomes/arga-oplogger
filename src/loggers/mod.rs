pub mod collections;
pub mod names;
pub mod nomenclatural_acts;
pub mod sequences;
pub mod taxa;
pub mod taxonomic_acts;


use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

use arga_core::crdt::DataFrameOperation;
use arga_core::models::{self, LogOperation};
use arga_core::schema;
pub use collections::Collections;
use diesel::*;
use indicatif::ProgressBarIter;
pub use nomenclatural_acts::NomenclaturalActs;
use rayon::prelude::*;
pub use sequences::Sequences;
use serde::de::DeserializeOwned;
pub use taxa::Taxa;
pub use taxonomic_acts::TaxonomicActs;
use uuid::Uuid;

use crate::database::{create_dataset_version, get_pool, FrameLoader};
use crate::errors::Error;
use crate::operations::{distinct_changes, Framer};
use crate::readers::csv::{CsvReader, IntoFrame};
use crate::readers::{meta, OperationLoader};
use crate::utils::FrameImportBars;


pub trait FrameProgress {
    fn bars(&self) -> FrameImportBars;
}

impl<S: Read + FrameProgress> FrameProgress for brotli::Decompressor<S> {
    fn bars(&self) -> FrameImportBars {
        self.get_ref().bars()
    }
}


pub struct ProgressStream<S: Read> {
    stream: ProgressBarIter<S>,
    bars: FrameImportBars,
}

impl<S: Read> ProgressStream<S> {
    pub fn new(stream: S, total_bytes: usize) -> ProgressStream<S> {
        let bars = FrameImportBars::new(total_bytes);
        let stream = bars.bytes.wrap_read(stream);
        ProgressStream { stream, bars }
    }
}

impl<S: Read> FrameProgress for ProgressStream<S> {
    fn bars(&self) -> FrameImportBars {
        self.bars.clone()
    }
}

impl<S: Read> Read for ProgressStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}


/// A parallel CSV framer and importer.
///
/// This caters for the general path of importing operations logs from a CSV file by treating each
/// row as a single frame and bulk upserting the distinct changes. It also displays a progress bar.
/// Because its a generic function that relies on implemented traits it has to be called as a
/// turbo fish function, ie. `import_csv_as_logs::<Record, TaxonOperation>(&path, &dataset_version_id)`.
///
/// The Reader (<R>) must implement the IntoFrame trait and be deserializable from a CSV file.
/// The Operation (<Op>) must implement the OperationLoader trait
pub fn import_csv_as_logs<T, Op>(path: &PathBuf, dataset_version_id: &Uuid) -> Result<(), Error>
where
    Op: Sync,
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default + Clone + ToString + PartialEq,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync,
{
    let file = File::open(path)?;
    let size = file.metadata()?.size();
    let stream = ProgressStream::new(file, size as usize);
    import_csv_from_stream::<T, Op, _>(stream, dataset_version_id)?;
    Ok(())
}


/// Imports a CSV stream that has been compressed.
///
/// This will use the brotli decompressor before passing the stream on to `import_csv_from_stream` where
/// it will proceed as if it was an extracted CSV file
pub fn import_compressed_csv_stream<S, T, Op>(stream: S, dataset: &meta::Dataset) -> Result<(), Error>
where
    S: Read + FrameProgress,
    Op: Sync,
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default + Clone + ToString + PartialEq,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync,
{
    let input = brotli::Decompressor::new(stream, 4096);
    let dataset_version = create_dataset_version(&dataset.id, &dataset.version, &dataset.published_at.to_string())?;
    import_csv_from_stream::<T, Op, _>(input, &dataset_version.id)?;
    Ok(())
}

/// A parallel CSV framer and importer.
///
/// This caters for the general path of importing operations logs from a CSV file by treating each
/// row as a single frame and bulk upserting the distinct changes. It also displays a progress bar.
/// Because its a generic function that relies on implemented traits it has to be called as a
/// turbo fish function, ie. `import_csv_as_logs::<Record, TaxonOperation>(&path, &dataset_version_id)`.
///
/// The Record (<T>) must implement the IntoFrame trait and be deserializable from a CSV file.
/// The Operation (<Op>) must implement the OperationLoader trait
/// The Reader (<R>) only needs to implement std::io::Read
pub fn import_csv_from_stream<T, Op, R>(reader: R, dataset_version_id: &Uuid) -> Result<(), Error>
where
    R: Read + FrameProgress,
    Op: Sync,
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default + Clone + ToString + PartialEq,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync,
{
    let bars = reader.bars();

    // we need a few components to fully import operation logs. the first is a CSV file reader
    // which parses each row and converts it into a frame. the second is a framer which allows
    // us to conveniently get chunks of frames from the reader and sets us up for easy parallelization.
    // and the third is the frame loader which allows us to query the database to deduplicate and
    // pull out unique operations, as well as upsert the new operations.
    let reader = CsvReader::<T, R>::from_reader(reader, *dataset_version_id)?;
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

        bars.frames.inc(total_frames as u64);
    }

    bars.finish();
    Ok(())
}


pub fn upsert_meta(meta: meta::Meta) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::{datasets, sources};

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    let package = models::Source::from(meta.clone());
    let mut dataset = models::Dataset::from(meta);

    let package_id = diesel::insert_into(sources::table)
        .values(package)
        .on_conflict(sources::name)
        .do_update()
        .set((
            sources::name.eq(excluded(sources::name)),
            sources::author.eq(excluded(sources::author)),
            sources::rights_holder.eq(excluded(sources::rights_holder)),
            sources::access_rights.eq(excluded(sources::access_rights)),
            sources::license.eq(excluded(sources::license)),
        ))
        .returning(sources::id)
        .get_result::<Uuid>(&mut conn)?;

    dataset.source_id = package_id;

    diesel::insert_into(datasets::table)
        .values(dataset)
        .on_conflict(datasets::global_id)
        .do_update()
        .set((
            datasets::name.eq(excluded(datasets::name)),
            datasets::short_name.eq(excluded(datasets::short_name)),
            datasets::url.eq(excluded(datasets::url)),
            datasets::citation.eq(excluded(datasets::citation)),
            datasets::license.eq(excluded(datasets::license)),
            datasets::rights_holder.eq(excluded(datasets::rights_holder)),
            datasets::updated_at.eq(excluded(datasets::updated_at)),
        ))
        .execute(&mut conn)?;

    Ok(())
}

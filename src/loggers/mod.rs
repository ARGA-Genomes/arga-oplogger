pub mod accessions;
pub mod admin_media;
pub mod agents;
pub mod collections;
pub mod datasets;
pub mod extractions;
pub mod libraries;
pub mod names;
pub mod nomenclatural_acts;
pub mod organisms;
pub mod publications;
pub mod sequences;
pub mod sources;
pub mod subsamples;
pub mod taxa;
pub mod taxonomic_acts;
pub mod tissues;


use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

use arga_core::crdt::{DataFrame, DataFrameOperation};
use arga_core::models::DatasetVersion;
use arga_core::models::logs::LogOperation;
use arga_core::{models, schema};
use diesel::*;
use indicatif::ProgressBarIter;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::database::{FrameLoader, PgPool, create_dataset_version, get_pool};
use crate::errors::Error;
use crate::frames::{FrameReader, Framer, IntoFrame};
use crate::operations::{distinct_changes, distinct_dataset_changes};
use crate::readers::csv::CsvReader;
use crate::readers::{OperationLoader, meta};
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
    pub fn new(stream: S, total_bytes: usize, message: &str) -> ProgressStream<S> {
        let bars = FrameImportBars::new(total_bytes, message);
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
    T::Atom: Default + Clone + ToString + PartialEq + std::fmt::Debug,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync + std::fmt::Debug,
{
    let file = File::open(path)?;
    let size = file.metadata()?.size();
    let stream = ProgressStream::new(file, size as usize, "Importing");
    // import_csv_from_stream::<T, Op, _>(stream, dataset_version_id)?;
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
    T::Atom: Default + Clone + ToString + PartialEq + std::fmt::Debug,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync + std::fmt::Debug,
{
    let input = brotli::Decompressor::new(stream, 4096);
    let dataset_version = create_dataset_version(&dataset.id, &dataset.version, &dataset.published_at.to_string())?;
    import_csv_from_stream::<T, Op, _>(input, &dataset_version)?;
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
pub fn import_csv_from_stream<T, Op, R>(reader: R, dataset_version: &DatasetVersion) -> Result<(), Error>
where
    R: Read + FrameProgress,
    Op: Sync,
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default + Clone + ToString + PartialEq + std::fmt::Debug,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<T::Atom> + From<DataFrameOperation<T::Atom>> + Clone + Sync + std::fmt::Debug,
{
    let bars = reader.bars();

    // we need a few components to fully import operation logs. the first is a CSV file reader
    // which parses each row and converts it into a frame. the second is a framer which allows
    // us to conveniently get chunks of frames from the reader and sets us up for easy parallelization.
    // and the third is the frame loader which allows us to query the database to deduplicate and
    // pull out unique operations, as well as upsert the new operations.
    let reader = CsvReader::<T, R>::from_reader(reader, dataset_version.id.clone())?;
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

            // compare the ops with previously imported ops and only return actual changes for the
            // specific dataset being imported. this is effectively the changeset between dataset versions
            let dataset_changes = distinct_dataset_changes(&dataset_version, slice.to_vec(), &loader)?;

            // now compare the dataset changes with the fully reduced entity. this allows us to only
            // keep changes that occur across datasets instead of keeping full changelog versions of
            // all datasets that get imported. importantly we compare against a fully reduced entity
            // *at a specific point in time*, in our case the publication date of the dataset version.
            // without it we would overwrite data changed by newer dataset versions
            let changes = distinct_changes(&dataset_version, dataset_changes, &loader)?;
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
pub fn import_frames_from_stream<Op, R>(reader: R, pool: PgPool) -> Result<(), Error>
where
    R: FrameReader + FrameProgress,
    R::Atom: Default + Clone + ToString + PartialEq + std::fmt::Debug,
    R: Iterator<Item = Result<DataFrame<R::Atom>, Error>>,
    Op: Sync,
    FrameLoader<Op>: OperationLoader + Clone,
    <FrameLoader<Op> as OperationLoader>::Operation:
        LogOperation<R::Atom> + From<DataFrameOperation<R::Atom>> + Clone + Sync + std::fmt::Debug,
{
    let bars = reader.bars();

    // we need a few components to fully import operation logs. the first is a CSV file reader
    // which parses each row and converts it into a frame. the second is a framer which allows
    // us to conveniently get chunks of frames from the reader and sets us up for easy parallelization.
    // and the third is the frame loader which allows us to query the database to deduplicate and
    // pull out unique operations, as well as upsert the new operations.
    let framer = Framer::new(reader);
    let loader = FrameLoader::<Op>::new(pool);

    // parse and convert big chunks of rows. this is an IO bound task but for each
    // chunk we need to query the database and then insert into the database, so
    // we parallelize the frame merging and inserting instead since it is an order of
    // magnitude slower than the parsing
    for frames in framer.chunks(20_000) {
        let total_frames = frames.len();

        // we flatten out all the frames into operations and process them in chunks of 10k.
        // postgres has a parameter limit and by chunking it we can query the database to
        // filter to distinct changes and import it in bulk without triggering any errors.
        // frames.operations()?.par_chunks(10_000).try_for_each(|slice| {
        //     let total = slice.len();

        //     // compare the ops with previously imported ops and only return actual changes
        //     let changes = distinct_changes(slice.to_vec(), &loader)?;
        //     let inserted = loader.upsert_operations(&changes)?;

        //     bars.inserted.inc(inserted as u64);
        //     bars.operations.inc(total as u64);
        //     Ok::<(), Error>(())
        // })?;

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

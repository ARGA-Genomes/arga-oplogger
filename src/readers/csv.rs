use std::path::PathBuf;

use arga_core::crdt::{DataFrame, Version};
use memchr::memchr_iter;
use memmap2::Mmap;
use serde::de::DeserializeOwned;
use tracing::info;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::errors::Error;

pub trait IntoFrame {
    type Atom;
    fn into_frame(self, frame: DataFrame<Self::Atom>) -> DataFrame<Self::Atom>;
}

pub trait EntityHashable {
    fn entity_hashable(&self) -> &[u8];
}

/// A reader that parses a CSV file and decomposes the row into operation logs.
///
/// This uses mmap and rayon to parallelize the read for performance. This means
/// that the order of rows are not guarantee and a frame of operation logs should
/// be contained in one row, with each frame considered a separate transaction and
/// thus a separate 'change' entry.
pub struct CsvReader<R> {
    pub dataset_version_id: Uuid,
    pub total_rows: usize,
    last_version: Version,
    reader: csv::Reader<std::fs::File>,
    phantom_record: std::marker::PhantomData<R>,
}

impl<R> CsvReader<R>
where
    R: DeserializeOwned + IntoFrame + EntityHashable,
    R::Atom: Default,
{
    pub fn from_path(path: PathBuf, dataset_version_id: Uuid) -> Result<CsvReader<R>, Error> {
        let total_rows = Self::total_rows(&path)?;
        Ok(CsvReader {
            reader: csv::Reader::from_path(&path)?,
            total_rows,
            last_version: Version::new(),
            dataset_version_id,
            phantom_record: std::marker::PhantomData,
        })
    }

    pub fn next_frame_chunk(&mut self) -> Option<Vec<Result<DataFrame<R::Atom>, Error>>> {
        let mut frames = Vec::with_capacity(2_000);
        for row in self.reader.deserialize::<R>().take(2_000) {
            match row {
                Err(err) => frames.push(Err(err.into())),
                Ok(record) => {
                    // We hash the entity_id to save on storage in the column
                    let mut hasher = Xxh3::new();
                    hasher.update(record.entity_hashable());
                    let hash = hasher.digest().to_string();

                    let frame = DataFrame::create(hash, self.dataset_version_id, self.last_version);
                    let frame = record.into_frame(frame);
                    self.last_version = frame.last_version();
                    frames.push(Ok(frame))
                }
            }
        }

        if !frames.is_empty() {
            Some(frames)
        } else {
            None
        }
    }

    fn total_rows(path: &PathBuf) -> Result<usize, Error> {
        info!(?path, "Memory mapping file");
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let mut total = 0;
        for _ in memchr_iter(b'\n', &mmap) {
            total += 1
        }

        Ok(total)
    }
}

impl<R> IntoIterator for CsvReader<R>
where
    R: DeserializeOwned + IntoFrame + EntityHashable,
    R::Atom: Default,
{
    type Item = Vec<Result<DataFrame<R::Atom>, Error>>;
    type IntoIter = CsvReaderIntoIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        CsvReaderIntoIterator { reader: self }
    }
}

pub struct CsvReaderIntoIterator<R> {
    reader: CsvReader<R>,
}

impl<R> Iterator for CsvReaderIntoIterator<R>
where
    R: DeserializeOwned + IntoFrame + EntityHashable,
    R::Atom: Default,
{
    type Item = Vec<Result<DataFrame<R::Atom>, Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next_frame_chunk()
    }
}

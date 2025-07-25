use std::io::Read;

use arga_core::crdt::{DataFrame, Version};
use serde::de::DeserializeOwned;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::errors::Error;
use crate::frames::{FrameReader, IntoFrame};


impl<T, R> FrameReader for CsvReader<T, R>
where
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default,
    R: Read,
{
    type Atom = T::Atom;
}

/// A reader that parses a CSV file and decomposes the row into operation logs.
///
/// This uses mmap and rayon to parallelize the read for performance. This means
/// that the order of rows are not guaranteed and a frame of operation logs should
/// be contained in one row, with each frame considered a separate transaction and
/// thus a separate 'change' entry.
pub struct CsvReader<T, R: Read> {
    pub dataset_version_id: Uuid,
    pub total_rows: usize,
    last_version: Version,
    reader: csv::Reader<R>,
    phantom_record: std::marker::PhantomData<T>,
}

impl<T, R> CsvReader<T, R>
where
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default,
    R: Read,
{
    pub fn from_reader(reader: R, dataset_version_id: Uuid) -> Result<CsvReader<T, R>, Error> {
        Ok(CsvReader {
            reader: csv::Reader::from_reader(reader),
            total_rows: 0,
            last_version: Version::new(),
            dataset_version_id,
            phantom_record: std::marker::PhantomData,
        })
    }

    pub fn next_frame(&mut self) -> Option<Result<DataFrame<T::Atom>, Error>> {
        let row = self.reader.deserialize::<T>().next();
        match row {
            Some(Err(err)) => Some(Err(err.into())),
            Some(Ok(record)) => {
                // We hash the entity_id to save on storage in the column
                let hash = xxh3_64(record.entity_hashable()).to_string();

                let frame = DataFrame::create(hash, self.dataset_version_id, self.last_version);
                let frame = record.into_frame(frame);
                self.last_version = frame.last_version();
                Some(Ok(frame))
            }
            None => None,
        }
    }
}

impl<T, R> Iterator for CsvReader<T, R>
where
    T: DeserializeOwned + IntoFrame,
    T::Atom: Default,
    R: Read,
{
    type Item = Result<DataFrame<T::Atom>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_frame()
    }
}

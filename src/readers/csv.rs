use std::io::Read;

use arga_core::crdt::{DataFrame, Version};
use serde::de::DeserializeOwned;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::errors::Error;


pub trait IntoFrame {
    type Atom;
    fn into_frame(self, frame: DataFrame<Self::Atom>) -> DataFrame<Self::Atom>;
    fn entity_hashable(&self) -> &[u8];
}

pub trait TryIntoFrame {
    type Atom;
    type Error;
}

// Used by functions that take a generic reader that also has to handle
// a iterator that returns fallable results
impl<Atom, E> TryIntoFrame for Result<DataFrame<Atom>, E> {
    type Atom = Atom;
    type Error = E;
}


pub trait FrameReader {
    type Atom;
}

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
/// that the order of rows are not guarantee and a frame of operation logs should
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
                let mut hasher = Xxh3::new();
                hasher.update(record.entity_hashable());
                let hash = hasher.digest().to_string();

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

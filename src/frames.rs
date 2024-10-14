use arga_core::crdt::{DataFrame, DataFrameOperation};

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


pub struct Framer<T> {
    stream: T,
}

impl<T> Framer<T>
where
    T: IntoIterator,
    <T as IntoIterator>::Item: TryIntoFrame,
    <<T as IntoIterator>::Item as TryIntoFrame>::Atom: Default,
{
    pub fn new(stream: T) -> Framer<T> {
        Framer { stream }
    }

    pub fn chunks(self, chunk_size: usize) -> FrameChunks<T> {
        FrameChunks {
            stream: self.stream,
            chunk_size,
        }
    }
}


pub struct FrameChunks<R> {
    stream: R,
    chunk_size: usize,
}

impl<R> Iterator for FrameChunks<R>
where
    R: FrameReader,
    R::Atom: Default,
    R: Iterator<Item = Result<DataFrame<R::Atom>, Error>>,
{
    type Item = Frames<R::Atom>;

    fn next(&mut self) -> Option<Self::Item> {
        let frames: Vec<Result<DataFrame<R::Atom>, Error>> = self.stream.by_ref().take(self.chunk_size).collect();
        if !frames.is_empty() {
            Some(Frames::new(frames))
        }
        else {
            None
        }
    }
}


pub struct Frames<A>(Vec<Result<DataFrame<A>, Error>>);

impl<A: Default> Frames<A> {
    pub fn new(frames: Vec<Result<DataFrame<A>, Error>>) -> Frames<A> {
        Frames(frames)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn operations<Op>(self) -> Result<Vec<Op>, Error>
    where
        Op: From<DataFrameOperation<A>>,
    {
        let mut ops: Vec<Op> = Vec::new();
        for frame in self.0 {
            ops.extend(frame?.collect());
        }
        Ok(ops)
    }
}

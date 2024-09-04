use std::collections::HashMap;

use arga_core::crdt::lww::Map;
use arga_core::crdt::{DataFrame, DataFrameOperation};
use arga_core::models::LogOperation;
use bigdecimal::BigDecimal;

use crate::errors::Error;
use crate::readers::csv::{FrameReader, TryIntoFrame};
use crate::readers::OperationLoader;


/// Combine the existing and new operations and group them up by entity id
pub fn group_operations<T, A>(existing_ops: Vec<T>, new_ops: Vec<T>) -> HashMap<String, Vec<T>>
where
    T: LogOperation<A>,
{
    let mut grouped: HashMap<String, Vec<T>> = HashMap::new();

    for op in existing_ops.into_iter() {
        grouped.entry(op.entity_id().clone()).or_default().push(op);
    }

    for op in new_ops.into_iter() {
        grouped.entry(op.entity_id().clone()).or_default().push(op);
    }

    grouped
}

/// Pick out and combine the operations that don't already exist in the existing set of operations.
///
/// This will merge the two lists of operations and use the last-write-wins CRDT map to filter
/// out duplicate operations and operations that don't alter the atom in some way.
/// Because the LWW map ignores operations that doesn't meaningfully change the value of the
/// operation it will ensure that operations from previous imports take precedence even when the
/// operation id is different.
pub fn merge_operations<T, A>(existing_ops: Vec<T>, new_ops: Vec<T>) -> Vec<T>
where
    A: ToString + Clone + PartialEq,
    T: LogOperation<A> + Clone,
{
    let entities = group_operations(existing_ops, new_ops);
    let mut merged = Vec::new();

    for (key, ops) in entities.into_iter() {
        let mut map = Map::new(key);
        let reduced = map.reduce(&ops);
        merged.extend(reduced);
    }

    merged
}


/// Filters out any no-op operations.
///
/// This will query the database for existing operations related to the entity ids found
/// in the `ops` vector and merge them. The merging process uses the LWW policy to determine
/// which changes are made. It will also filter out the operations already found in the database
/// which means this will *only* return operations that actually make a change to the entity.
///
/// Because this uses the loader its best to find an ideal chunk size for the operations vector
/// so that it can load the operations in bulk while staying within memory and database bounds.
pub fn distinct_changes<A, L>(ops: Vec<L::Operation>, loader: &L) -> Result<Vec<L::Operation>, Error>
where
    A: ToString + Clone + PartialEq,
    L: OperationLoader,
    L::Operation: LogOperation<A> + From<DataFrameOperation<A>> + Clone,
{
    // grab all the entity ids in the chunk because we need to check for existing
    // operations in the database for the operation merge
    let entity_ids: Vec<&String> = ops.iter().map(|op| op.entity_id()).collect();

    // load the existing operations by looking for the entity ids present in the frame chunk
    // this allows us to group and compare operations in bulk without using all the memory
    match loader.load_operations(&entity_ids) {
        Err(err) => Err(err),
        Ok(existing_ops) => {
            // use these ids to remove it from the merged operation list as they will end up
            // being no ops. we have to clone the id since they're moved in the merge
            let ids: Vec<BigDecimal> = existing_ops.iter().map(|op| op.id().clone()).collect();

            // merging ensures that we dont have duplicate ops and that we don't have
            // *useless* ops, which will helpfully eliminate any operation with a newer
            // timestamp that doesn't change the actual atom
            let merged = merge_operations(existing_ops, ops);

            // because merging uses the last-write-wins map for reduction it still returns
            // the existing operations. because this is a distinct operation iterator we
            // want to remove the existing ops from the merged set
            let changes = merged.into_iter().filter(|op| !ids.contains(&op.id())).collect();
            Ok(changes)
        }
    }
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
        if frames.len() > 0 {
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

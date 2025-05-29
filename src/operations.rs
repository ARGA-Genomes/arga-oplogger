use std::collections::HashMap;

use arga_core::crdt::DataFrameOperation;
use arga_core::models::logs::LogOperation;
use arga_core::models::DatasetVersion;
use bigdecimal::BigDecimal;

use crate::errors::Error;
use crate::readers::OperationLoader;


struct CausalOperation<Op, A>
where
    A: ToString,
    Op: LogOperation<A>,
{
    index: usize,
    operation: Op,
    atom_marker: std::marker::PhantomData<A>,
}

impl<Op, A> ToString for CausalOperation<Op, A>
where
    A: ToString,
    Op: LogOperation<A>,
{
    fn to_string(&self) -> String {
        format!("{}-{}-{}", self.operation.entity_id(), self.index, self.operation.atom().to_string())
    }
}

/// Combine the existing and new operations and group them up by entity id
pub fn group_causal_operations<T, A>(
    existing_ops: Vec<T>,
    new_ops: Vec<T>,
) -> HashMap<String, Vec<CausalOperation<T, A>>>
where
    A: ToString,
    T: LogOperation<A>,
{
    let mut grouped: HashMap<String, Vec<CausalOperation<T, A>>> = HashMap::new();

    let mut existing_counter: HashMap<String, usize> = HashMap::new();
    for op in existing_ops.into_iter() {
        let entity_id = op.entity_id().clone();

        let index = *existing_counter
            .entry(op.atom().to_string())
            .and_modify(|counter| *counter += 1)
            .or_insert(0);

        let causal = CausalOperation {
            index,
            operation: op,
            atom_marker: std::marker::PhantomData,
        };

        grouped.entry(entity_id).or_default().push(causal);
    }

    let mut new_counter: HashMap<String, usize> = HashMap::new();
    for op in new_ops.into_iter() {
        let entity_id = op.entity_id().clone();

        let index = *new_counter
            .entry(op.atom().to_string())
            .and_modify(|counter| *counter += 1)
            .or_insert(0);

        let causal = CausalOperation {
            index,
            operation: op,
            atom_marker: std::marker::PhantomData,
        };

        grouped.entry(entity_id).or_default().push(causal);
    }

    grouped
}


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
    A: ToString + Clone + PartialEq + std::fmt::Debug,
    T: LogOperation<A> + Clone + std::fmt::Debug,
{
    let entities = group_operations(existing_ops, new_ops);
    // let entities = group_causal_operations(existing_ops, new_ops);
    let mut merged: Vec<T> = Vec::new();

    // get all operations for a specific entity. both existing and new operations.
    for (key, ops) in entities.into_iter() {
        // let mut fields: HashMap<String, Vec<&T>> = HashMap::new();

        // for op in &ops {
        //     println!("{key}: {} {:?} -- {}", op.operation.id(), op.operation.atom(), op.to_string());

        //     let field_name = op.operation.atom().to_string();

        //     fields
        //         .entry(field_name)
        //         .and_modify(|arr| {
        //             if arr.last().map(|o| o.atom()) != Some(&op.operation.atom()) {
        //                 arr.push(&op.operation)
        //             }
        //         })
        //         .or_insert(vec![&op.operation]);
        // }

        // println!("{:#?}", fields);

        // for arr in fields.into_values() {
        //     merged.extend(arr.into_iter().map(|r| r.to_owned()));
        // }

        let mut map = arga_core::crdt::lww::Map::new(key);
        let reduced = map.reduce(&ops);
        merged.extend(reduced.into_iter().map(|r| r.to_owned()));
    }

    merged.sort_by(|a, b| a.id().cmp(b.id()));
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
pub fn distinct_changes<A, L>(
    version: &DatasetVersion,
    ops: Vec<L::Operation>,
    loader: &L,
) -> Result<Vec<L::Operation>, Error>
where
    A: ToString + Clone + PartialEq + std::fmt::Debug,
    L: OperationLoader,
    L::Operation: LogOperation<A> + From<DataFrameOperation<A>> + Clone + std::fmt::Debug,
{
    // grab all the entity ids in the chunk because we need to check for existing
    // operations in the database for the operation merge
    let entity_ids: Vec<&String> = ops.iter().map(|op| op.entity_id()).collect();

    // load the existing operations by looking for the entity ids present in the frame chunk
    // this allows us to group and compare operations in bulk without using all the memory
    match loader.load_operations(&version, &entity_ids) {
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
            // the existing operations. since this is a distinct operation iterator we
            // want to remove the existing ops from the merged set
            let changes: Vec<L::Operation> = merged.into_iter().filter(|op| !ids.contains(op.id())).collect();
            Ok(changes)
        }
    }
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
pub fn distinct_dataset_changes<A, L>(
    version: &DatasetVersion,
    ops: Vec<L::Operation>,
    loader: &L,
) -> Result<Vec<L::Operation>, Error>
where
    A: ToString + Clone + PartialEq + std::fmt::Debug,
    L: OperationLoader,
    L::Operation: LogOperation<A> + From<DataFrameOperation<A>> + Clone + std::fmt::Debug,
{
    // grab all the entity ids in the chunk because we need to check for existing
    // operations in the database for the operation merge
    let entity_ids: Vec<&String> = ops.iter().map(|op| op.entity_id()).collect();

    // load the existing operations by looking for the entity ids present in the frame chunk
    // this allows us to group and compare operations in bulk without using all the memory
    match loader.load_dataset_operations(version, &entity_ids) {
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
            // the existing operations. since this is a distinct operation iterator we
            // want to remove the existing ops from the merged set
            let changes: Vec<L::Operation> = merged.into_iter().filter(|op| !ids.contains(op.id())).collect();
            Ok(changes)
        }
    }
}

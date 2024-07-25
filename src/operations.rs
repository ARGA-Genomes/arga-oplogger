use std::collections::HashMap;

use arga_core::crdt::lww::Map;

use crate::errors::Error;


/// Combine the existing and new operations and group them up by entity id
pub fn group_operations<T, A>(existing_ops: Vec<T>, new_ops: Vec<T>) -> Result<HashMap<String, Vec<T>>, Error>
where
    T: arga_core::models::LogOperation<A>,
{
    let mut grouped: HashMap<String, Vec<T>> = HashMap::new();

    for op in existing_ops.into_iter() {
        grouped.entry(op.id().clone()).or_default().push(op);
    }

    for op in new_ops.into_iter() {
        grouped.entry(op.id().clone()).or_default().push(op);
    }

    Ok(grouped)
}

/// Pick out and combine the operations that don't already exist in the existing set of operations.
///
/// This will merge the two lists of operations and use the last-write-wins CRDT map to filter
/// out duplicate operations and operations that don't alter the atom in some way.
/// Because the LWW map ignores operations that doesn't meaningfully change the value of the
/// operation it will ensure that operations from previous imports take precedence even when the
/// operation id is different.
pub fn merge_operations<T, A>(existing_ops: Vec<T>, new_ops: Vec<T>) -> Result<Vec<T>, Error>
where
    A: ToString + Clone + PartialEq,
    T: arga_core::models::LogOperation<A> + Clone,
{
    let entities = group_operations(existing_ops, new_ops)?;
    let mut merged = Vec::new();

    for (key, ops) in entities.into_iter() {
        let mut map = Map::new(key);
        let reduced = map.reduce(&ops);
        merged.extend(reduced);
    }

    Ok(merged)
}

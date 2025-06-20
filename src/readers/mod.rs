use arga_core::models::DatasetVersion;

use crate::errors::Error;

pub mod csv;
pub mod meta;
pub mod plazi;


pub trait OperationLoader {
    type Operation;
    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error>;

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error>;

    fn upsert_operations(&self, operations: &[Self::Operation]) -> Result<usize, Error>;
}

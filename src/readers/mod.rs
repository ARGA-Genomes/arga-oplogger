use crate::database::PgConn;
use crate::errors::Error;

pub mod csv;


pub trait OperationLoader {
    type Operation;
    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error>;
}

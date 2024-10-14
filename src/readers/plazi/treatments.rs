use arga_core::models::NomenclaturalActOperation;
use arga_core::schema;
use diesel::*;

use crate::database::get_pool;
use crate::errors::Error;


#[derive(Debug)]
pub enum Extent {
    Page { start: usize, end: usize },
}


#[derive(Debug)]
pub struct Author {
    pub name: String,
    pub affiliation: String,
}


fn import_operations(operations: Vec<NomenclaturalActOperation>) -> Result<(), Error> {
    use schema::nomenclatural_act_logs::dsl::*;

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    for chunk in operations.chunks(1000) {
        diesel::insert_into(nomenclatural_act_logs)
            .values(chunk)
            .execute(&mut conn)?;
    }

    Ok(())
}


// impl From<NomenclaturalActTypeError> for SpeciesNameError {
//     fn from(value: NomenclaturalActTypeError) -> Self {
//         SpeciesNameError::InvalidStatus(value)
//     }
// }

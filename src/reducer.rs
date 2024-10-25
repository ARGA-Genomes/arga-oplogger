use arga_core::crdt::lww::Map;
use arga_core::models::LogOperation;

use crate::database::PgPool;
use crate::errors::Error;


pub trait Reducer<L> {
    type Atom: Clone + ToString + PartialEq;
    type ReducedRecord;

    fn reduce(frame: Map<Self::Atom>, lookups: &L) -> Result<Self::ReducedRecord, Error>;
}


pub trait EntityPager {
    type Operation;

    fn total(&self) -> Result<i64, Error>;
    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error>;
}


pub struct DatabaseReducer<R, P, L> {
    pool: PgPool,
    pager: P,
    lookups: L,
    current_page: usize,
    phantom_record: std::marker::PhantomData<R>,
}

impl<R, P, L> DatabaseReducer<R, P, L>
where
    R: Reducer<L>,
    P: EntityPager,
    P::Operation: Clone + LogOperation<R::Atom>,
{
    pub fn new(pool: PgPool, pager: P, lookups: L) -> DatabaseReducer<R, P, L> {
        DatabaseReducer {
            pool,
            pager,
            lookups,
            current_page: 0,
            phantom_record: std::marker::PhantomData,
        }
    }

    pub fn next_entity_chunk(&mut self) -> Result<Vec<R::ReducedRecord>, Error> {
        let operations = self.pager.load_entity_operations(self.current_page)?;
        self.current_page += 1;

        // group up the operations so we can iterate by entity frames
        let entities = crate::operations::group_operations(operations, vec![]);
        let mut records = Vec::new();

        // create an LWW map for each entity and reduce it
        for (key, ops) in entities.into_iter() {
            let mut map = Map::new(key);
            map.reduce(&ops);
            let record = R::reduce(map, &self.lookups)?;
            records.push(record);
        }

        Ok(records)
    }
}


impl<R, P, L> Iterator for DatabaseReducer<R, P, L>
where
    R: Reducer<L>,
    P: EntityPager,
    P::Operation: Clone + LogOperation<R::Atom>,
{
    type Item = Vec<R::ReducedRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk = self.next_entity_chunk().unwrap();
        if !chunk.is_empty() { Some(chunk) } else { None }
    }
}


// like database reducer except it threads through the dataset
// associated with the first operation (the create op)
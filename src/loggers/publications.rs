use arga_core::crdt::lww::Map;
use arga_core::crdt::DataFrame;
use arga_core::models::{self, PublicationAtom, PublicationOperation, PublicationType};
use arga_core::schema;
use chrono::{DateTime, Utc};
use diesel::*;
use rayon::prelude::*;
use serde::Deserialize;

use crate::database::{FrameLoader, PgPool};
use crate::errors::Error;
use crate::frames::{FrameReader, IntoFrame};
use crate::readers::OperationLoader;
use crate::{frame_push_opt, import_frames_from_stream, FrameProgress};

type PublicationFrame = DataFrame<PublicationAtom>;


impl OperationLoader for FrameLoader<PublicationOperation> {
    type Operation = PublicationOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<PublicationOperation>, Error> {
        use schema::publication_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = publication_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<PublicationOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[PublicationOperation]) -> Result<usize, Error> {
        use schema::publication_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(publication_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        Ok(inserted)
    }
}


/// The CSV record to decompose into operation logs.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Record {
    pub entity_id: String,
    pub title: String,
    pub authors: Vec<String>,
    pub published_year: i32,
    pub source_url: String,

    pub published_date: Option<DateTime<Utc>>,
    pub language: Option<String>,
    pub publisher: Option<String>,
    pub doi: Option<String>,
    pub publication_type: Option<PublicationType>,
    pub citation: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl IntoFrame for Record {
    type Atom = PublicationAtom;

    fn entity_hashable(&self) -> &[u8] {
        // the sequence id should be an externally unique value that all datasets
        // reference if they are describing this particular datum
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: PublicationFrame) -> PublicationFrame {
        use PublicationAtom::*;
        frame.push(EntityId(self.entity_id));
        frame.push(Title(self.title));
        frame.push(Authors(self.authors));
        frame.push(PublishedYear(self.published_year));
        frame.push(SourceUrl(self.source_url));
        frame_push_opt!(frame, PublishedDate, self.published_date);
        frame_push_opt!(frame, Language, self.language);
        frame_push_opt!(frame, Publisher, self.publisher);
        frame_push_opt!(frame, Doi, self.doi);
        frame_push_opt!(frame, Type, self.publication_type);
        frame_push_opt!(frame, Citation, self.citation);
        frame_push_opt!(frame, RecordCreatedAt, self.created_at);
        frame_push_opt!(frame, RecordUpdatedAt, self.updated_at);
        frame
    }
}


/// Import frames of publications from the stream
pub fn import<R>(reader: R, pool: PgPool) -> Result<(), Error>
where
    R: FrameReader<Atom = models::PublicationAtom> + FrameProgress,
    R: Iterator<Item = Result<DataFrame<R::Atom>, Error>>,
{
    import_frames_from_stream::<models::PublicationOperation, R>(reader, pool)
}


pub fn update() -> Result<(), Error> {
    use diesel::dsl::count_distinct;
    use schema::publication_logs::dsl::*;

    let pool = crate::database::get_pool()?;
    let mut conn = pool.get_timeout(std::time::Duration::from_secs(1))?;

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total = publication_logs
        .select(count_distinct(entity_id))
        .get_result::<i64>(&mut conn)?;

    let limit = 3;
    let offsets: Vec<i64> = (0..total).step_by(limit as usize).collect();

    offsets
        .into_par_iter()
        .try_for_each(|offset| reduce_and_update(offset, limit, pool.clone()))?;

    Ok(())
}


pub fn reduce_and_update(offset: i64, limit: i64, pool: crate::database::PgPool) -> Result<(), Error> {
    use diesel::upsert::excluded;
    use schema::publication_logs::dsl::*;
    use schema::publications as pubs;

    let mut conn = pool.get_timeout(std::time::Duration::from_secs(1))?;

    // we first get all the entity ids within a specified range. this means that the
    // query here has to return the same amount of results as a COUNT DISTINCT query otherwise
    // some entities will be missed during the update. in particular make sure to always order
    // the query results otherwise random entities might get pulled in since postgres doesnt sort by default
    let entity_ids = publication_logs
        .select(entity_id)
        .group_by(entity_id)
        .order_by(entity_id)
        .offset(offset)
        .limit(limit)
        .into_boxed();

    // get the operations for the entities making sure to order by operation id so that
    // the CRDT structs can do their thing
    let operations = publication_logs
        .filter(entity_id.eq_any(entity_ids))
        .order_by((entity_id, operation_id))
        .load::<PublicationOperation>(&mut conn)?;

    // group the entity operations up and preparing it for use in the LWW map
    let entities = crate::operations::group_operations(operations, vec![]);
    let mut records: Vec<models::Publication> = Vec::new();

    // reduce all the operations by applying them to an empty record
    // as per the last write wins policy
    for (key, ops) in entities.into_iter() {
        let mut map = Map::new(key);
        map.reduce(&ops);

        let reduced = Record::from(map);
        records.push(reduced.into());
    }

    for chunk in records.chunks(1000) {
        // postgres always creates a new row version so we cant get
        // an actual figure of the amount of records changed
        diesel::insert_into(pubs::table)
            .values(chunk)
            .on_conflict(pubs::entity_id)
            .do_update()
            .set((
                pubs::title.eq(excluded(pubs::title)),
                pubs::authors.eq(excluded(pubs::authors)),
                pubs::published_year.eq(excluded(pubs::published_year)),
                pubs::published_date.eq(excluded(pubs::published_date)),
                pubs::language.eq(excluded(pubs::language)),
                pubs::publisher.eq(excluded(pubs::publisher)),
                pubs::doi.eq(excluded(pubs::doi)),
                pubs::publication_type.eq(excluded(pubs::publication_type)),
                pubs::citation.eq(excluded(pubs::citation)),
                pubs::record_created_at.eq(excluded(pubs::record_created_at)),
                pubs::record_updated_at.eq(excluded(pubs::record_updated_at)),
                pubs::updated_at.eq(excluded(pubs::updated_at)),
            ))
            .execute(&mut conn)?;
    }

    Ok(())
}


/// Converts a LWW CRDT map of name publication atoms to a record for serialisation
impl From<Map<PublicationAtom>> for Record {
    fn from(value: Map<PublicationAtom>) -> Self {
        use PublicationAtom::*;

        let mut record = Record {
            entity_id: value.entity_id,
            ..Default::default()
        };

        for val in value.atoms.into_values() {
            match val {
                Empty => {}
                EntityId(_) => {}
                Title(value) => record.title = value,
                Authors(value) => record.authors = value,
                PublishedYear(value) => record.published_year = value,
                SourceUrl(value) => record.source_url = value,
                PublishedDate(value) => record.published_date = Some(value),
                Language(value) => record.language = Some(value),
                Publisher(value) => record.publisher = Some(value),
                Doi(value) => record.doi = Some(value),
                Type(value) => record.publication_type = Some(value),
                Citation(value) => record.citation = Some(value),
                RecordCreatedAt(value) => record.created_at = Some(value),
                RecordUpdatedAt(value) => record.updated_at = Some(value),
            }
        }

        record
    }
}

impl From<Record> for models::Publication {
    fn from(value: Record) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            entity_id: value.entity_id,

            title: value.title,
            authors: value.authors.into_iter().map(|v| Some(v)).collect(),
            published_year: value.published_year,
            published_date: value.published_date,
            language: value.language,
            publisher: value.publisher,
            doi: value.doi,
            publication_type: value.publication_type,
            citation: value.citation,
            source_urls: Some(vec![Some(value.source_url)]),

            record_created_at: value.created_at,
            record_updated_at: value.updated_at,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

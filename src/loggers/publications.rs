use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::crdt::lww::Map;
use arga_core::models::{self, DatasetVersion, PublicationAtom, PublicationOperation, PublicationType};
use arga_core::{schema, schema_gnl};
use chrono::{DateTime, Utc};
use diesel::*;
use rayon::prelude::*;
use serde::Deserialize;
use tracing::error;

use crate::database::{FrameLoader, PgPool};
use crate::errors::Error;
use crate::frames::{FrameReader, IntoFrame};
use crate::readers::{OperationLoader, meta};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream, import_frames_from_stream};

type PublicationFrame = DataFrame<PublicationAtom>;


impl OperationLoader for FrameLoader<PublicationOperation> {
    type Operation = PublicationOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::publication_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = publication_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(publication_logs::all_columns())
            .order(operation_id.asc())
            .load::<PublicationOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::publication_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = publication_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(publication_logs::all_columns())
            .order(operation_id.asc())
            .load::<PublicationOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[PublicationOperation]) -> Result<usize, Error> {
        use schema::publication_logs::dsl::*;
        let mut conn = self.pool.get()?;

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
    pub title: Option<String>,
    pub authors: Option<Vec<String>>,

    pub published_year: Option<i32>,
    pub source_url: Option<String>,
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
        frame_push_opt!(frame, Title, self.title);
        frame_push_opt!(frame, Authors, self.authors);
        frame_push_opt!(frame, PublishedYear, self.published_year);
        frame_push_opt!(frame, SourceUrl, self.source_url);
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
pub fn import_frames<R>(reader: R, pool: PgPool) -> Result<(), Error>
where
    R: FrameReader<Atom = models::PublicationAtom> + FrameProgress,
    R: Iterator<Item = Result<DataFrame<R::Atom>, Error>>,
{
    import_frames_from_stream::<models::PublicationOperation, R>(reader, pool)
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, PublicationOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<PublicationOperation> {
    type Operation = models::PublicationOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::publication_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::publication_logs::dsl::*;
        use schema_gnl::publication_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = publication_entities::table
            .select(publication_entities::entity_id)
            .order_by(publication_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = publication_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<PublicationOperation>(&mut conn)?;

        Ok(operations)
    }
}


struct Lookups;

impl Reducer<Lookups> for models::Publication {
    type Atom = PublicationAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use PublicationAtom::*;

        let mut title = None;
        let mut authors = None;
        let mut published_year = None;
        let mut published_date = None;
        let mut source_urls = None;
        let mut language = None;
        let mut publisher = None;
        let mut doi = None;
        let mut publication_type = None;
        let mut citation = None;
        let mut record_created_at = None;
        let mut record_updated_at = None;

        for atom in atoms {
            match atom {
                Empty => {}
                EntityId(_value) => {}
                Title(value) => title = Some(value),
                Authors(value) => authors = Some(value.into_iter().map(Some).collect()),
                PublishedYear(value) => published_year = Some(value),
                PublishedDate(value) => published_date = Some(value),
                SourceUrl(value) => source_urls = Some(vec![Some(value)]),
                Language(value) => language = Some(value),
                Publisher(value) => publisher = Some(value),
                Doi(value) => doi = Some(value),
                Type(value) => publication_type = Some(value),
                Citation(value) => citation = Some(value),
                RecordCreatedAt(value) => record_created_at = Some(value),
                RecordUpdatedAt(value) => record_updated_at = Some(value),
            }
        }

        let record = models::Publication {
            id: uuid::Uuid::new_v4(),
            entity_id,
            title,
            authors,
            published_year,
            published_date,
            source_urls,
            language,
            publisher,
            doi,
            publication_type,
            citation,
            record_created_at,
            record_updated_at,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<PublicationOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating publications");

    let reducer: DatabaseReducer<models::Publication, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::publications::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(publications)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    title.eq(excluded(title)),
                    authors.eq(excluded(authors)),
                    published_year.eq(excluded(published_year)),
                    published_date.eq(excluded(published_date)),
                    language.eq(excluded(language)),
                    publisher.eq(excluded(publisher)),
                    doi.eq(excluded(doi)),
                    publication_type.eq(excluded(publication_type)),
                    citation.eq(excluded(citation)),
                    record_created_at.eq(excluded(record_created_at)),
                    record_updated_at.eq(excluded(record_updated_at)),
                    updated_at.eq(excluded(updated_at)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}


pub fn update_old() -> Result<(), Error> {
    use diesel::dsl::count_distinct;
    use schema::publication_logs::dsl::*;

    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    // get the total amount of distinct entities in the log table. this allows
    // us to split up the reduction into many threads without loading all operations
    // into memory
    let total = publication_logs
        .select(count_distinct(entity_id))
        .get_result::<i64>(&mut conn)?;

    let limit = 10_000;
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

    let mut conn = pool.get()?;

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
                Title(value) => record.title = Some(value),
                Authors(value) => record.authors = Some(value),
                PublishedYear(value) => record.published_year = Some(value),
                SourceUrl(value) => record.source_url = Some(value),
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
            authors: value.authors.map(|v| v.into_iter().map(Some).collect()),
            published_year: value.published_year,
            published_date: value.published_date,
            language: value.language,
            publisher: value.publisher,
            doi: value.doi,
            publication_type: value.publication_type,
            citation: value.citation,
            source_urls: value.source_url.map(|v| vec![Some(v)]),

            record_created_at: value.created_at,
            record_updated_at: value.updated_at,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{AnnotationAtom, AnnotationOperation};
use arga_core::models::{self, DatasetVersion};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;
use xxhash_rust::xxh3::xxh3_64;

use crate::database::FrameLoader;
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{OperationLoader, meta};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::{new_progress_bar, parse_string_opt};
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type AnnotationFrame = DataFrame<AnnotationAtom>;


impl OperationLoader for FrameLoader<AnnotationOperation> {
    type Operation = AnnotationOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::annotation_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = annotation_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(annotation_logs::all_columns())
            .order(operation_id.asc())
            .load::<AnnotationOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::annotation_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = annotation_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(annotation_logs::all_columns())
            .order(operation_id.asc())
            .load::<AnnotationOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[AnnotationOperation]) -> Result<usize, Error> {
        use schema::annotation_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(annotation_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

        Ok(inserted)
    }
}


// A single row in a supported CSV file.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    entity_id: String,
    assembly_id: String,

    name: Option<String>,
    provider: Option<String>,
    event_date: Option<chrono::NaiveDate>,

    #[serde(deserialize_with = "parse_string_opt")]
    number_of_genes: Option<i32>,
    #[serde(deserialize_with = "parse_string_opt")]
    number_of_proteins: Option<i32>,
}

impl IntoFrame for Record {
    type Atom = AnnotationAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: AnnotationFrame) -> AnnotationFrame {
        use AnnotationAtom::*;

        frame.push(AssemblyId(self.assembly_id));
        frame_push_opt!(frame, Name, self.name);
        frame_push_opt!(frame, Provider, self.provider);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, NumberOfGenes, self.number_of_genes);
        frame_push_opt!(frame, NumberOfProteins, self.number_of_proteins);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, AnnotationOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<AnnotationOperation> {
    type Operation = models::AnnotationOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::annotation_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::annotation_logs::dsl::*;
        use schema_gnl::annotation_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = annotation_entities::table
            .select(annotation_entities::entity_id)
            .order_by(annotation_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = annotation_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<AnnotationOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::Annotation {
    type Atom = AnnotationAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use AnnotationAtom::*;

        let mut assembly_id = None;
        let mut name = None;
        let mut provider = None;
        let mut event_date = None;
        let mut number_of_genes = None;
        let mut number_of_proteins = None;


        for atom in atoms {
            match atom {
                Empty => {}
                AssemblyId(value) => assembly_id = Some(value),
                Name(value) => name = Some(value),
                Provider(value) => provider = Some(value),
                EventDate(value) => event_date = Some(value),
                NumberOfGenes(value) => number_of_genes = Some(value),
                NumberOfProteins(value) => number_of_proteins = Some(value),
            }
        }

        let assembly_id = assembly_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "AssemblyId".to_string()))?;
        let assembly_entity_id = xxh3_64(assembly_id.as_bytes());

        let record = models::Annotation {
            entity_id,
            assembly_id: assembly_entity_id.to_string(),
            name,
            provider,
            event_date,
            number_of_genes,
            number_of_proteins,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<AnnotationOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating annotations");
    let reducer: DatabaseReducer<models::Annotation, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::annotations::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(annotations)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    assembly_id.eq(excluded(assembly_id)),
                    name.eq(excluded(name)),
                    provider.eq(excluded(provider)),
                    event_date.eq(excluded(event_date)),
                    number_of_genes.eq(excluded(number_of_genes)),
                    number_of_proteins.eq(excluded(number_of_proteins)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

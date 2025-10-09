use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{DataProductAtom, DataProductOperation};
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
use crate::utils::new_progress_bar;
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type DataProductFrame = DataFrame<DataProductAtom>;


impl OperationLoader for FrameLoader<DataProductOperation> {
    type Operation = DataProductOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::data_product_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = data_product_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(data_product_logs::all_columns())
            .order(operation_id.asc())
            .load::<DataProductOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::data_product_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = data_product_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(data_product_logs::all_columns())
            .order(operation_id.asc())
            .load::<DataProductOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[DataProductOperation]) -> Result<usize, Error> {
        use schema::data_product_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(data_product_logs)
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
    organism_id: Option<String>,
    extract_id: Option<String>,
    sequence_run_id: Option<String>,
    publication_id: Option<String>,
    custodian: Option<String>,

    sequence_sample_id: Option<String>,
    sequence_analysis_id: Option<String>,
    notes: Option<String>,
    context: Option<String>,
    r#type: Option<String>,
    file_type: Option<String>,
    url: Option<String>,
    licence: Option<String>,
    access: Option<String>,
}

impl IntoFrame for Record {
    type Atom = DataProductAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: DataProductFrame) -> DataProductFrame {
        use DataProductAtom::*;

        frame_push_opt!(frame, OrganismId, self.organism_id);
        frame_push_opt!(frame, ExtractId, self.extract_id);
        frame_push_opt!(frame, SequenceRunId, self.sequence_run_id);
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, Custodian, self.custodian);
        frame_push_opt!(frame, SequenceSampleId, self.sequence_sample_id);
        frame_push_opt!(frame, SequenceAnalysisId, self.sequence_analysis_id);
        frame_push_opt!(frame, Notes, self.notes);
        frame_push_opt!(frame, Context, self.context);
        frame_push_opt!(frame, Type, self.r#type);
        frame_push_opt!(frame, FileType, self.file_type);
        frame_push_opt!(frame, Url, self.url);
        frame_push_opt!(frame, Licence, self.licence);
        frame_push_opt!(frame, Access, self.access);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, DataProductOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<DataProductOperation> {
    type Operation = models::DataProductOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::data_product_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::data_product_logs::dsl::*;
        use schema_gnl::data_product_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = data_product_entities::table
            .select(data_product_entities::entity_id)
            .order_by(data_product_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = data_product_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<DataProductOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::DataProduct {
    type Atom = DataProductAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use DataProductAtom::*;

        let mut organism_id = None;
        let mut extract_id = None;
        let mut sequence_run_id = None;
        let mut publication_id = None;
        let mut custodian = None;
        let mut sequence_sample_id = None;
        let mut sequence_analysis_id = None;
        let mut notes = None;
        let mut context = None;
        let mut type_ = None;
        let mut file_type = None;
        let mut url = None;
        let mut licence = None;
        let mut access = None;


        for atom in atoms {
            match atom {
                Empty => {}
                OrganismId(value) => organism_id = Some(value),
                ExtractId(value) => extract_id = Some(value),
                SequenceRunId(value) => sequence_run_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                Custodian(value) => custodian = Some(value),
                SequenceSampleId(value) => sequence_sample_id = Some(value),
                SequenceAnalysisId(value) => sequence_analysis_id = Some(value),
                Notes(value) => notes = Some(value),
                Context(value) => context = Some(value),
                Type(value) => type_ = Some(value),
                FileType(value) => file_type = Some(value),
                Url(value) => url = Some(value),
                Licence(value) => licence = Some(value),
                Access(value) => access = Some(value),
            }
        }

        let organism_entity_id = organism_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let extract_entity_id = extract_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let sequence_run_entity_id = sequence_run_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let custodian_entity_id = custodian.map(|id| xxh3_64(id.as_bytes()).to_string());

        let record = models::DataProduct {
            entity_id,
            organism_id: organism_entity_id,
            extract_id: extract_entity_id,
            sequence_run_id: sequence_run_entity_id,
            publication_id: publication_entity_id,
            custodian: custodian_entity_id,
            sequence_sample_id,
            sequence_analysis_id,
            notes,
            context,
            type_,
            file_type,
            url,
            licence,
            access,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<DataProductOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating data products");
    let reducer: DatabaseReducer<models::DataProduct, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::data_products::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(data_products)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    organism_id.eq(excluded(organism_id)),
                    extract_id.eq(excluded(extract_id)),
                    sequence_run_id.eq(excluded(sequence_run_id)),
                    publication_id.eq(excluded(publication_id)),
                    custodian.eq(excluded(custodian)),
                    sequence_sample_id.eq(excluded(sequence_sample_id)),
                    sequence_analysis_id.eq(excluded(sequence_analysis_id)),
                    notes.eq(excluded(notes)),
                    context.eq(excluded(context)),
                    type_.eq(excluded(type_)),
                    file_type.eq(excluded(file_type)),
                    url.eq(excluded(url)),
                    licence.eq(excluded(licence)),
                    access.eq(excluded(access)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

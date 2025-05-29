use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{AccessionEventAtom, AccessionEventOperation};
use arga_core::models::DatasetVersion;
use arga_core::schema;
use diesel::*;
use serde::Deserialize;

use crate::database::FrameLoader;
use crate::errors::Error;
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type AccessionEventFrame = DataFrame<AccessionEventAtom>;


impl OperationLoader for FrameLoader<AccessionEventOperation> {
    type Operation = AccessionEventOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::accession_event_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = accession_event_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(accession_event_logs::all_columns())
            .order(operation_id.asc())
            .load::<AccessionEventOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::accession_event_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = accession_event_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(accession_event_logs::all_columns())
            .order(operation_id.asc())
            .load::<AccessionEventOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[AccessionEventOperation]) -> Result<usize, Error> {
        use schema::accession_event_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(accession_event_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

        Ok(inserted)
    }
}


// A single row in a supported CSV file.
//
// For specimens this is conflated with a collection event, so we deserialize both
// in order to split them up into different operation logs down the line without having
// to reprocess the CSV file.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    entity_id: String,
    specimen_id: String,
    scientific_name: String,

    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,

    type_status: Option<String>,

    collection_repository_id: Option<String>,
    collection_repository_code: Option<String>,
    institution_name: Option<String>,
    institution_code: Option<String>,
    other_catalog_numbers: Option<String>,

    accessioned_by: Option<String>,
    disposition: Option<String>,
    preparation: Option<String>,
}

impl IntoFrame for Record {
    type Atom = AccessionEventAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: AccessionEventFrame) -> AccessionEventFrame {
        use AccessionEventAtom::*;

        frame.push(SpecimenId(self.specimen_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, TypeStatus, self.type_status);
        frame_push_opt!(frame, CollectionRepositoryId, self.collection_repository_id);
        frame_push_opt!(frame, CollectionRepositoryCode, self.collection_repository_code);
        frame_push_opt!(frame, InstitutionName, self.institution_name);
        frame_push_opt!(frame, InstitutionCode, self.institution_code);
        frame_push_opt!(frame, OtherCatalogNumbers, self.other_catalog_numbers);
        frame_push_opt!(frame, AccessionedBy, self.accessioned_by);
        frame_push_opt!(frame, Disposition, self.disposition);
        frame_push_opt!(frame, Preparation, self.preparation);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, AccessionEventOperation>(stream, dataset)
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::{DatasetVersion, OrganismAtom, OrganismOperation};
use arga_core::schema;
use diesel::*;
use serde::Deserialize;

use crate::database::FrameLoader;
use crate::errors::Error;
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type OrganismFrame = DataFrame<OrganismAtom>;


impl OperationLoader for FrameLoader<OrganismOperation> {
    type Operation = OrganismOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::organism_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = organism_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(organism_logs::all_columns())
            .order(operation_id.asc())
            .load::<OrganismOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::organism_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = organism_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(organism_logs::all_columns())
            .order(operation_id.asc())
            .load::<OrganismOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[OrganismOperation]) -> Result<usize, Error> {
        use schema::organism_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(organism_logs)
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
    scientific_name: String,
    organism_id: String,

    sex: Option<String>,
    genotypic_sex: Option<String>,
    phenotypic_sex: Option<String>,
    life_stage: Option<String>,
    reproductive_condition: Option<String>,
    behavior: Option<String>,
}

impl IntoFrame for Record {
    type Atom = OrganismAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: OrganismFrame) -> OrganismFrame {
        use OrganismAtom::*;

        frame.push(OrganismId(self.organism_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, Sex, self.sex);
        frame_push_opt!(frame, GenotypicSex, self.genotypic_sex);
        frame_push_opt!(frame, PhenotypicSex, self.phenotypic_sex);
        frame_push_opt!(frame, LifeStage, self.life_stage);
        frame_push_opt!(frame, ReproductiveCondition, self.reproductive_condition);
        frame_push_opt!(frame, Behavior, self.behavior);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, OrganismOperation>(stream, dataset)
}

use std::path::PathBuf;

use arga_core::crdt::DataFrame;
use arga_core::models::{SpecimenAtom, SpecimenOperation};
use arga_core::schema;
use diesel::*;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::database::FrameLoader;
use crate::errors::Error;
use crate::readers::csv::IntoFrame;
use crate::readers::OperationLoader;
use crate::utils::titleize_first_word;

type SpecimenFrame = DataFrame<SpecimenAtom>;


impl OperationLoader for FrameLoader<SpecimenOperation> {
    type Operation = SpecimenOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<SpecimenOperation>, Error> {
        use schema::specimen_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = specimen_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<SpecimenOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[SpecimenOperation]) -> Result<usize, Error> {
        use schema::specimen_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(specimen_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

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
    record_id: String,
    scientific_name: String,
    // canonical_name: Option<String>,
    // type_status: Option<String>,
    // institution_name: Option<String>,
    // institution_code: Option<String>,
    // collection_code: Option<String>,
    // catalog_number: Option<String>,
    // collected_by: Option<String>,
    // identified_by: Option<String>,
    // identified_date: Option<String>,
    // organism_id: Option<String>,
    // material_sample_id: Option<String>,
    // details: Option<String>,
    // remarks: Option<String>,
    // identification_remarks: Option<String>,

    // // location block
    // locality: Option<String>,
    // country: Option<String>,
    // country_code: Option<String>,
    // state_province: Option<String>,
    // county: Option<String>,
    // municipality: Option<String>,
    // latitude: Option<f64>,
    // longitude: Option<f64>,
    // // verbatim_lat_long: Option<String>,
    // elevation: Option<f64>,
    // depth: Option<f64>,
    // elevation_accuracy: Option<f64>,
    // depth_accuracy: Option<f64>,
    // location_source: Option<String>,

    // // collection event block
    // event_date: Option<String>,
    // event_time: Option<String>,
    // field_number: Option<String>,
    // field_notes: Option<String>,
    // record_number: Option<String>,
    // individual_count: Option<String>,
    // organism_quantity: Option<String>,
    // organism_quantity_type: Option<String>,
    // sex: Option<String>,
    // genotypic_sex: Option<String>,
    // phenotypic_sex: Option<String>,
    // life_stage: Option<String>,
    // reproductive_condition: Option<String>,
    // behavior: Option<String>,
    // establishment_means: Option<String>,
    // degree_of_establishment: Option<String>,
    // pathway: Option<String>,
    // occurrence_status: Option<String>,
    // preparation: Option<String>,
    // other_catalog_numbers: Option<String>,
    // env_broad_scale: Option<String>,
    // env_local_scale: Option<String>,
    // env_medium: Option<String>,
    // habitat: Option<String>,
    // ref_biomaterial: Option<String>,
    // source_mat_id: Option<String>,
    // specific_host: Option<String>,
    // strain: Option<String>,
    // isolate: Option<String>,
}

impl IntoFrame for Record {
    type Atom = SpecimenAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: SpecimenFrame) -> SpecimenFrame {
        use SpecimenAtom::*;
        frame.push(EntityId(self.entity_id));
        frame.push(RecordId(self.record_id));
        frame.push(ScientificName(titleize_first_word(&self.scientific_name)));
        frame
    }
}

pub struct Collections {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl Collections {
    pub fn import(&self) -> Result<(), Error> {
        crate::import_csv_as_logs::<Record, SpecimenOperation>(&self.path, &self.dataset_version_id)?;
        info!("Specimen logs imported");
        Ok(())
    }
}

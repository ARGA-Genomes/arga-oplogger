use std::{path::PathBuf, time::Duration};

use arga_core::{
    crdt::DataFrame,
    models::{SpecimenAtom, SpecimenOperation},
    schema,
};
use diesel::*;
use rayon::prelude::*;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    database::get_pool,
    errors::Error,
    operations::merge_operations,
    readers::csv::{CsvReader, EntityHashable, IntoFrame},
    utils::{new_progress_bar, titleize_first_word},
};

type SpecimenFrame = DataFrame<SpecimenAtom>;

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

impl EntityHashable for Record {
    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }
}

impl IntoFrame for Record {
    type Atom = SpecimenAtom;

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
        use schema::specimen_logs::dsl::*;

        let pool = get_pool()?;
        // let conn = pool.get()?;

        let reader: CsvReader<Record> = CsvReader::from_path(self.path.clone(), self.dataset_version_id)?;

        let progress = new_progress_bar(reader.total_rows, "Converting and importing");
        progress.enable_steady_tick(Duration::from_millis(100));

        // let mut total_inserted = 0;
        // let mut total_skipped = 0;

        // for chunk in reader {
        //     let total_rows = chunk.len();

        //     // bail if we run into an error
        //     let mut records = Vec::with_capacity(chunk.len());
        //     for record in chunk {
        //         records.push(record?)
        //     }

        //     let entity_ids: Vec<&String> = records.iter().map(|r| &r.entity_id).collect();

        //     let existing = specimen_logs
        //         .filter(entity_id.eq_any(entity_ids))
        //         .order(operation_id.asc())
        //         .load::<SpecimenOperation>(&mut conn)?;

        //     let mut new_ops = Vec::new();
        //     for record in records {
        //         let ops: Vec<SpecimenOperation> = record.operations.into_iter().map(|o| o.into()).collect();
        //         new_ops.extend(ops);
        //     }
        //     let ops = merge_operations(existing, new_ops)?;

        //     let inserted = diesel::insert_into(specimen_logs)
        //         .values(&ops)
        //         .on_conflict_do_nothing()
        //         .execute(&mut conn)?;

        //     total_inserted += inserted;
        //     total_skipped += ops.len() - inserted;

        //     progress.inc(total_rows as u64);
        //     progress.set_message(format!("Operations: inserted: {}, skipped: {}", total_inserted, total_skipped));
        // }

        reader.into_iter().par_bridge().try_for_each_with(pool, |pool, chunk| {
            let total_rows = chunk.len();
            let mut conn = pool.get()?;

            // bail if we run into an error
            let mut records = Vec::with_capacity(chunk.len());
            for record in chunk {
                records.push(record?)
            }

            let entity_ids: Vec<&String> = records.iter().map(|r| &r.entity_id).collect();

            let existing = specimen_logs
                .filter(entity_id.eq_any(entity_ids))
                .order(operation_id.asc())
                .load::<SpecimenOperation>(&mut conn)?;

            let mut new_ops = Vec::new();
            for record in records {
                let ops: Vec<SpecimenOperation> = record.operations.into_iter().map(|o| o.into()).collect();
                new_ops.extend(ops);
            }
            let ops = merge_operations(existing, new_ops)?;

            let inserted = diesel::insert_into(specimen_logs)
                .values(&ops)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;

            progress.inc(total_rows as u64);
            progress.set_message(format!("inserted: {inserted}"));

            Ok::<(), Error>(())
        })?;

        // progress.finish_with_message("Totals: Inserted: {total_inserted}, Skipped: {total_skipped}");

        Ok(())
    }
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{ExtractionAtom, ExtractionOperation};
use arga_core::models::{self, DatasetVersion};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;
use xxhash_rust::xxh3::xxh3_64;

use crate::database::{FrameLoader, StringMap, name_lookup};
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{OperationLoader, meta};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type ExtractionFrame = DataFrame<ExtractionAtom>;


impl OperationLoader for FrameLoader<ExtractionOperation> {
    type Operation = ExtractionOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::extraction_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = extraction_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(extraction_logs::all_columns())
            .order(operation_id.asc())
            .load::<ExtractionOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::extraction_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = extraction_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(extraction_logs::all_columns())
            .order(operation_id.asc())
            .load::<ExtractionOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[ExtractionOperation]) -> Result<usize, Error> {
        use schema::extraction_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(extraction_logs)
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
    subsample_id: String,
    extract_id: String,
    scientific_name: String,

    publication_id: Option<String>,
    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,
    extracted_by: Option<String>,
    material_extracted_by: Option<String>,
    nucleic_acid_type: Option<String>,
    preparation_type: Option<String>,
    preservation_type: Option<String>,
    preservation_method: Option<String>,
    extraction_method: Option<String>,
    concentration_method: Option<String>,
    conformation: Option<String>,
    concentration: Option<f64>,
    concentration_unit: Option<String>,
    quantification: Option<String>,
    absorbance_260_230_ratio: Option<f64>,
    absorbance_260_280_ratio: Option<f64>,
    cell_lysis_method: Option<String>,
    action_extracted: Option<String>,
    number_of_extracts_pooled: Option<String>,
}

impl IntoFrame for Record {
    type Atom = ExtractionAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: ExtractionFrame) -> ExtractionFrame {
        use ExtractionAtom::*;

        frame.push(SubsampleId(self.subsample_id));
        frame.push(ExtractId(self.extract_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, ExtractedBy, self.extracted_by);
        frame_push_opt!(frame, MaterialExtractedBy, self.material_extracted_by);
        frame_push_opt!(frame, NucleicAcidType, self.nucleic_acid_type);
        frame_push_opt!(frame, PreparationType, self.preparation_type);
        frame_push_opt!(frame, PreservationType, self.preservation_type);
        frame_push_opt!(frame, PreservationMethod, self.preservation_method);
        frame_push_opt!(frame, ExtractionMethod, self.extraction_method);
        frame_push_opt!(frame, ConcentrationMethod, self.concentration_method);
        frame_push_opt!(frame, Conformation, self.conformation);
        frame_push_opt!(frame, Concentration, self.concentration);
        frame_push_opt!(frame, ConcentrationUnit, self.concentration_unit);
        frame_push_opt!(frame, Quantification, self.quantification);
        frame_push_opt!(frame, Absorbance260230Ratio, self.absorbance_260_230_ratio);
        frame_push_opt!(frame, Absorbance260280Ratio, self.absorbance_260_280_ratio);
        frame_push_opt!(frame, CellLysisMethod, self.cell_lysis_method);
        frame_push_opt!(frame, ActionExtracted, self.action_extracted);
        frame_push_opt!(frame, NumberOfExtractsPooled, self.number_of_extracts_pooled);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, ExtractionOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<ExtractionOperation> {
    type Operation = models::ExtractionOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::extraction_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::extraction_logs::dsl::*;
        use schema_gnl::extraction_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = extraction_entities::table
            .select(extraction_entities::entity_id)
            .order_by(extraction_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = extraction_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<ExtractionOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::DnaExtract {
    type Atom = ExtractionAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use ExtractionAtom::*;

        let mut scientific_name = None;
        let mut extract_id = None;
        let mut subsample_id = None;
        let mut publication_id = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut extracted_by = None;
        let mut material_extracted_by = None;
        let mut nucleic_acid_type = None;
        let mut preparation_type = None;
        let mut preservation_type = None;
        let mut preservation_method = None;
        let mut extraction_method = None;
        let mut concentration_method = None;
        let mut conformation = None;
        let mut concentration = None;
        let mut concentration_unit = None;
        let mut quantification = None;
        let mut absorbance_260_230_ratio = None;
        let mut absorbance_260_280_ratio = None;
        let mut cell_lysis_method = None;
        let mut action_extracted = None;
        let mut number_of_extracts_pooled = None;

        for atom in atoms {
            match atom {
                Empty => {}
                ScientificName(value) => scientific_name = Some(value),
                SubsampleId(value) => subsample_id = Some(value),
                ExtractId(value) => extract_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                ExtractedBy(value) => extracted_by = Some(value),
                MaterialExtractedBy(value) => material_extracted_by = Some(value),
                NucleicAcidType(value) => nucleic_acid_type = Some(value),
                PreparationType(value) => preparation_type = Some(value),
                PreservationType(value) => preservation_type = Some(value),
                PreservationMethod(value) => preservation_method = Some(value),
                ExtractionMethod(value) => extraction_method = Some(value),
                ConcentrationMethod(value) => concentration_method = Some(value),
                Conformation(value) => conformation = Some(value),
                Concentration(value) => concentration = Some(value),
                ConcentrationUnit(value) => concentration_unit = Some(value),
                Quantification(value) => quantification = Some(value),
                Absorbance260230Ratio(value) => absorbance_260_230_ratio = Some(value),
                Absorbance260280Ratio(value) => absorbance_260_280_ratio = Some(value),
                CellLysisMethod(value) => cell_lysis_method = Some(value),
                ActionExtracted(value) => action_extracted = Some(value),
                NumberOfExtractsPooled(value) => number_of_extracts_pooled = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let subsample_id =
            subsample_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "SubsampleId".to_string()))?;
        let extract_id = extract_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ExtractId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let subsample_entity_id = xxh3_64(subsample_id.as_bytes());
        let scientific_name_entity_id = xxh3_64(scientific_name.as_bytes());

        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let extracted_by_entity_id = extracted_by.map(|v| xxh3_64(v.as_bytes()).to_string());
        let material_extracted_by_entity_id = material_extracted_by.map(|v| xxh3_64(v.as_bytes()).to_string());

        let record = models::DnaExtract {
            entity_id,
            subsample_id: subsample_entity_id.to_string(),
            species_name_id: scientific_name_entity_id as i64, // should still retain identity despite negative range
            publication_id: publication_entity_id,
            extract_id,
            event_date,
            event_time,
            extracted_by: extracted_by_entity_id,
            material_extracted_by: material_extracted_by_entity_id,
            nucleic_acid_type,
            preparation_type,
            preservation_type,
            preservation_method,
            extraction_method,
            concentration_method,
            conformation,
            concentration,
            concentration_unit,
            quantification,
            absorbance_260_230_ratio,
            absorbance_260_280_ratio,
            cell_lysis_method,
            action_extracted,
            number_of_extracts_pooled,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<ExtractionOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating extractions");
    let reducer: DatabaseReducer<models::DnaExtract, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::dna_extracts::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }


            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(dna_extracts)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    extract_id.eq(excluded(extract_id)),
                    subsample_id.eq(excluded(subsample_id)),
                    species_name_id.eq(excluded(species_name_id)),
                    publication_id.eq(excluded(publication_id)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    extracted_by.eq(excluded(extracted_by)),
                    material_extracted_by.eq(excluded(material_extracted_by)),
                    nucleic_acid_type.eq(excluded(nucleic_acid_type)),
                    preparation_type.eq(excluded(preparation_type)),
                    preservation_type.eq(excluded(preservation_type)),
                    preservation_method.eq(excluded(preservation_method)),
                    extraction_method.eq(excluded(extraction_method)),
                    concentration_method.eq(excluded(concentration_method)),
                    conformation.eq(excluded(conformation)),
                    concentration.eq(excluded(concentration)),
                    concentration_unit.eq(excluded(concentration_unit)),
                    quantification.eq(excluded(quantification)),
                    absorbance_260_230_ratio.eq(excluded(absorbance_260_230_ratio)),
                    absorbance_260_280_ratio.eq(excluded(absorbance_260_280_ratio)),
                    cell_lysis_method.eq(excluded(cell_lysis_method)),
                    action_extracted.eq(excluded(action_extracted)),
                    number_of_extracts_pooled.eq(excluded(number_of_extracts_pooled)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

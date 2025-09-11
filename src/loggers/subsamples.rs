use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{SubsampleAtom, SubsampleOperation};
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

type SubsampleFrame = DataFrame<SubsampleAtom>;


impl OperationLoader for FrameLoader<SubsampleOperation> {
    type Operation = SubsampleOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::subsample_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = subsample_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(subsample_logs::all_columns())
            .order(operation_id.asc())
            .load::<SubsampleOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::subsample_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = subsample_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(subsample_logs::all_columns())
            .order(operation_id.asc())
            .load::<SubsampleOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[SubsampleOperation]) -> Result<usize, Error> {
        use schema::subsample_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(subsample_logs)
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
    specimen_id: String,
    subsample_id: String,
    scientific_name: String,

    publication_id: Option<String>,
    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,
    institution_name: Option<String>,
    institution_code: Option<String>,
    sample_type: Option<String>,
    name: Option<String>,
    custodian: Option<String>,
    description: Option<String>,
    notes: Option<String>,
    culture_method: Option<String>,
    culture_media: Option<String>,
    weight_or_volume: Option<String>,
    preservation_method: Option<String>,
    preservation_temperature: Option<String>,
    preservation_duration: Option<String>,
    quality: Option<String>,
    cell_type: Option<String>,
    cell_line: Option<String>,
    clone_name: Option<String>,
    lab_host: Option<String>,
    sample_processing: Option<String>,
    sample_pooling: Option<String>,
}

impl IntoFrame for Record {
    type Atom = SubsampleAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: SubsampleFrame) -> SubsampleFrame {
        use SubsampleAtom::*;

        frame.push(SpecimenId(self.specimen_id));
        frame.push(SubsampleId(self.subsample_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, InstitutionName, self.institution_name);
        frame_push_opt!(frame, InstitutionCode, self.institution_code);
        frame_push_opt!(frame, SampleType, self.sample_type);
        frame_push_opt!(frame, Name, self.name);
        frame_push_opt!(frame, Custodian, self.custodian);
        frame_push_opt!(frame, Description, self.description);
        frame_push_opt!(frame, Notes, self.notes);
        frame_push_opt!(frame, CultureMethod, self.culture_method);
        frame_push_opt!(frame, CultureMedia, self.culture_media);
        frame_push_opt!(frame, WeightOrVolume, self.weight_or_volume);
        frame_push_opt!(frame, PreservationMethod, self.preservation_method);
        frame_push_opt!(frame, PreservationTemperature, self.preservation_temperature);
        frame_push_opt!(frame, PreservationDuration, self.preservation_duration);
        frame_push_opt!(frame, Quality, self.quality);
        frame_push_opt!(frame, CellType, self.cell_type);
        frame_push_opt!(frame, CellLine, self.cell_line);
        frame_push_opt!(frame, CloneName, self.clone_name);
        frame_push_opt!(frame, LabHost, self.lab_host);
        frame_push_opt!(frame, SampleProcessing, self.sample_processing);
        frame_push_opt!(frame, SamplePooling, self.sample_pooling);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, SubsampleOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<SubsampleOperation> {
    type Operation = models::SubsampleOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::subsample_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::subsample_logs::dsl::*;
        use schema_gnl::subsample_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = subsample_entities::table
            .select(subsample_entities::entity_id)
            .order_by(subsample_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = subsample_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<SubsampleOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups {
    names: StringMap,
}

impl Reducer<Lookups> for models::Subsample {
    type Atom = SubsampleAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use SubsampleAtom::*;

        let mut scientific_name = None;
        let mut specimen_id = None;
        let mut subsample_id = None;
        let mut publication_id = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut institution_name = None;
        let mut institution_code = None;
        let mut sample_type = None;
        let mut name = None;
        let mut custodian = None;
        let mut description = None;
        let mut notes = None;
        let mut culture_method = None;
        let mut culture_media = None;
        let mut weight_or_volume = None;
        let mut preservation_method = None;
        let mut preservation_temperature = None;
        let mut preservation_duration = None;
        let mut quality = None;
        let mut cell_type = None;
        let mut cell_line = None;
        let mut clone_name = None;
        let mut lab_host = None;
        let mut sample_processing = None;
        let mut sample_pooling = None;

        for atom in atoms {
            match atom {
                Empty => {}
                ScientificName(value) => scientific_name = Some(value),
                SpecimenId(value) => specimen_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                SubsampleId(value) => subsample_id = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                InstitutionName(value) => institution_name = Some(value),
                InstitutionCode(value) => institution_code = Some(value),
                SampleType(value) => sample_type = Some(value),
                Name(value) => name = Some(value),
                Custodian(value) => custodian = Some(value),
                Description(value) => description = Some(value),
                Notes(value) => notes = Some(value),
                CultureMethod(value) => culture_method = Some(value),
                CultureMedia(value) => culture_media = Some(value),
                WeightOrVolume(value) => weight_or_volume = Some(value),
                PreservationMethod(value) => preservation_method = Some(value),
                PreservationTemperature(value) => preservation_temperature = Some(value),
                PreservationDuration(value) => preservation_duration = Some(value),
                Quality(value) => quality = Some(value),
                CellType(value) => cell_type = Some(value),
                CellLine(value) => cell_line = Some(value),
                CloneName(value) => clone_name = Some(value),
                LabHost(value) => lab_host = Some(value),
                SampleProcessing(value) => sample_processing = Some(value),
                SamplePooling(value) => sample_pooling = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let subsample_id =
            subsample_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "SubsampleId".to_string()))?;
        let specimen_id = specimen_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "SpecimenId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let specimen_entity_id = xxh3_64(specimen_id.as_bytes());
        let scientific_name_entity_id = xxh3_64(scientific_name.as_bytes());

        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());


        let record = models::Subsample {
            entity_id,
            specimen_id: specimen_entity_id.to_string(),
            species_name_id: scientific_name_entity_id as i64, // should still retain identity despite negative range
            publication_id: publication_entity_id,
            subsample_id,
            event_date,
            event_time,
            institution_name,
            institution_code,
            sample_type,
            name,
            custodian,
            description,
            notes,
            culture_method,
            culture_media,
            weight_or_volume,
            preservation_method,
            preservation_temperature,
            preservation_duration,
            quality,
            cell_type,
            cell_line,
            clone_name,
            lab_host,
            sample_processing,
            sample_pooling,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let lookups = Lookups {
        names: name_lookup(&mut pool)?,
    };

    let pager: FrameLoader<SubsampleOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating subsamples");
    let reducer: DatabaseReducer<models::Subsample, _, _> = DatabaseReducer::new(pager, lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::subsamples::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(subsamples)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    specimen_id.eq(excluded(specimen_id)),
                    subsample_id.eq(excluded(subsample_id)),
                    species_name_id.eq(excluded(species_name_id)),
                    publication_id.eq(excluded(publication_id)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    institution_name.eq(excluded(institution_name)),
                    institution_code.eq(excluded(institution_code)),
                    sample_type.eq(excluded(sample_type)),
                    name.eq(excluded(name)),
                    custodian.eq(excluded(custodian)),
                    description.eq(excluded(description)),
                    notes.eq(excluded(notes)),
                    culture_method.eq(excluded(culture_method)),
                    culture_media.eq(excluded(culture_media)),
                    weight_or_volume.eq(excluded(weight_or_volume)),
                    preservation_method.eq(excluded(preservation_method)),
                    preservation_temperature.eq(excluded(preservation_temperature)),
                    preservation_duration.eq(excluded(preservation_duration)),
                    quality.eq(excluded(quality)),
                    cell_type.eq(excluded(cell_type)),
                    cell_line.eq(excluded(cell_line)),
                    clone_name.eq(excluded(clone_name)),
                    lab_host.eq(excluded(lab_host)),
                    sample_processing.eq(excluded(sample_processing)),
                    sample_pooling.eq(excluded(sample_pooling)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

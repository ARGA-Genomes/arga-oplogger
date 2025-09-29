use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{SequenceRunAtom, SequenceRunOperation};
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

type SequenceRunFrame = DataFrame<SequenceRunAtom>;


impl OperationLoader for FrameLoader<SequenceRunOperation> {
    type Operation = SequenceRunOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::sequence_run_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = sequence_run_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(sequence_run_logs::all_columns())
            .order(operation_id.asc())
            .load::<SequenceRunOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::sequence_run_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = sequence_run_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(sequence_run_logs::all_columns())
            .order(operation_id.asc())
            .load::<SequenceRunOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[SequenceRunOperation]) -> Result<usize, Error> {
        use schema::sequence_run_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(sequence_run_logs)
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
    library_id: String,
    sequence_id: String,
    scientific_name: String,
    publication_id: Option<String>,

    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,
    facility: Option<String>,
    instrument_or_method: Option<String>,
    platform: Option<String>,
    kit_chemistry: Option<String>,
    flowcell_type: Option<String>,
    cell_movie_length: Option<String>,
    base_caller_model: Option<String>,
    fast5_compression: Option<String>,
    analysis_software: Option<String>,
    analysis_software_version: Option<String>,
    target_gene: Option<String>,
    sra_run_accession: Option<String>,
}

impl IntoFrame for Record {
    type Atom = SequenceRunAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: SequenceRunFrame) -> SequenceRunFrame {
        use SequenceRunAtom::*;

        frame.push(LibraryId(self.library_id));
        frame.push(SequenceRunId(self.sequence_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, Facility, self.facility);
        frame_push_opt!(frame, InstrumentOrMethod, self.instrument_or_method);
        frame_push_opt!(frame, Platform, self.platform);
        frame_push_opt!(frame, KitChemistry, self.kit_chemistry);
        frame_push_opt!(frame, FlowcellType, self.flowcell_type);
        frame_push_opt!(frame, CellMovieLength, self.cell_movie_length);
        frame_push_opt!(frame, BaseCallerModel, self.base_caller_model);
        frame_push_opt!(frame, Fast5Compression, self.fast5_compression);
        frame_push_opt!(frame, AnalysisSoftware, self.analysis_software);
        frame_push_opt!(frame, AnalysisSoftwareVersion, self.analysis_software_version);
        frame_push_opt!(frame, TargetGene, self.target_gene);
        frame_push_opt!(frame, SraRunAccession, self.sra_run_accession);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, SequenceRunOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<SequenceRunOperation> {
    type Operation = models::SequenceRunOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::sequence_run_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::sequence_run_logs::dsl::*;
        use schema_gnl::sequence_run_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = sequence_run_entities::table
            .select(sequence_run_entities::entity_id)
            .order_by(sequence_run_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = sequence_run_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<SequenceRunOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::SequenceRun {
    type Atom = SequenceRunAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use SequenceRunAtom::*;

        let mut library_id = None;
        let mut sequence_run_id = None;
        let mut publication_id = None;
        let mut scientific_name = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut facility = None;
        let mut instrument_or_method = None;
        let mut platform = None;
        let mut kit_chemistry = None;
        let mut flowcell_type = None;
        let mut cell_movie_length = None;
        let mut base_caller_model = None;
        let mut fast5_compression = None;
        let mut analysis_software = None;
        let mut analysis_software_version = None;
        let mut target_gene = None;
        let mut sra_run_accession = None;

        for atom in atoms {
            match atom {
                Empty => {}
                LibraryId(value) => library_id = Some(value),
                SequenceRunId(value) => sequence_run_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                Facility(value) => facility = Some(value),
                InstrumentOrMethod(value) => instrument_or_method = Some(value),
                Platform(value) => platform = Some(value),
                KitChemistry(value) => kit_chemistry = Some(value),
                FlowcellType(value) => flowcell_type = Some(value),
                CellMovieLength(value) => cell_movie_length = Some(value),
                BaseCallerModel(value) => base_caller_model = Some(value),
                Fast5Compression(value) => fast5_compression = Some(value),
                AnalysisSoftware(value) => analysis_software = Some(value),
                AnalysisSoftwareVersion(value) => analysis_software_version = Some(value),
                TargetGene(value) => target_gene = Some(value),
                SraRunAccession(value) => sra_run_accession = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let library_id = library_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "LibraryId".to_string()))?;
        let sequence_run_id =
            sequence_run_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "SequenceRunId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let library_entity_id = xxh3_64(library_id.as_bytes());
        let scientific_name_entity_id = xxh3_64(scientific_name.as_bytes());

        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());

        let record = models::SequenceRun {
            entity_id,
            library_id: library_entity_id.to_string(),
            species_name_id: scientific_name_entity_id as i64,
            publication_id: publication_entity_id,
            sequence_run_id,
            event_date,
            event_time,
            facility,
            instrument_or_method,
            platform,
            kit_chemistry,
            flowcell_type,
            cell_movie_length,
            base_caller_model,
            fast5_compression,
            analysis_software,
            analysis_software_version,
            target_gene,
            sra_run_accession,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<SequenceRunOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating sequence runs");
    let reducer: DatabaseReducer<models::SequenceRun, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::sequence_runs::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(sequence_runs)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    library_id.eq(excluded(library_id)),
                    sequence_run_id.eq(excluded(sequence_run_id)),
                    species_name_id.eq(excluded(species_name_id)),
                    publication_id.eq(excluded(publication_id)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    facility.eq(excluded(facility)),
                    instrument_or_method.eq(excluded(instrument_or_method)),
                    platform.eq(excluded(platform)),
                    kit_chemistry.eq(excluded(kit_chemistry)),
                    flowcell_type.eq(excluded(flowcell_type)),
                    cell_movie_length.eq(excluded(cell_movie_length)),
                    base_caller_model.eq(excluded(base_caller_model)),
                    fast5_compression.eq(excluded(fast5_compression)),
                    analysis_software.eq(excluded(analysis_software)),
                    analysis_software_version.eq(excluded(analysis_software_version)),
                    target_gene.eq(excluded(target_gene)),
                    sra_run_accession.eq(excluded(sra_run_accession)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

use std::path::PathBuf;

use arga_core::crdt::DataFrame;
use arga_core::models::{SequenceAtom, SequenceOperation};
use arga_core::schema;
use diesel::*;
use rayon::prelude::*;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::database::{get_pool, FrameLoader};
use crate::errors::Error;
use crate::frame_push_opt;
use crate::operations::{distinct_changes, Framer};
use crate::readers::csv::{CsvReader, IntoFrame};
use crate::readers::OperationLoader;
use crate::utils::FrameImportBars;

type SequenceFrame = DataFrame<SequenceAtom>;


impl OperationLoader for FrameLoader<SequenceOperation> {
    type Operation = SequenceOperation;

    fn load_operations(&self, entity_ids: &[&String]) -> Result<Vec<SequenceOperation>, Error> {
        use schema::sequence_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        let ops = sequence_logs
            .filter(entity_id.eq_any(entity_ids))
            .order(operation_id.asc())
            .load::<SequenceOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[SequenceOperation]) -> Result<usize, Error> {
        use schema::sequence_logs::dsl::*;
        let mut conn = self.pool.get_timeout(std::time::Duration::from_secs(1))?;

        // if there is a conflict based on the operation id then it is a duplicate
        // operation so do nothing with it
        let inserted = diesel::insert_into(sequence_logs)
            .values(operations)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        Ok(inserted)
    }
}


/// The CSV record to decompose into operation logs.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    /// The record id assigned by the dataset
    sequence_id: String,
    /// The record id of the dna extraction that was sequenced
    dna_extract_id: String,

    /// The date the sequence occurred
    event_date: Option<String>,
    /// The time the sequence occurred
    event_time: Option<String>,
    /// Who carried out the sequencing
    sequenced_by: Option<String>,
    /// An external reference id to the material that was sequenced
    material_sample_id: Option<String>,

    /// The concentration used for the sequencing
    concentration: Option<String>,
    amplicon_size: Option<i64>,
    /// The basepair size of the sequence. eg 140 bp
    estimated_size: Option<String>,
    bait_set_name: Option<String>,
    bait_set_reference: Option<String>,

    /// The gene being sequenced. eg COI-5P
    target_gene: Option<String>,
    /// The sequence data. eg ACTGTTGGCAC
    dna_sequence: Option<String>,
}

impl IntoFrame for Record {
    type Atom = SequenceAtom;

    fn entity_hashable(&self) -> &[u8] {
        // the sequence id should be an externally unique value that all datasets
        // reference if they are describing this particular datum
        self.sequence_id.as_bytes()
    }

    fn into_frame(self, mut frame: SequenceFrame) -> SequenceFrame {
        use SequenceAtom::*;
        frame.push(EntityId(self.sequence_id.clone()));
        frame.push(SequenceId(self.sequence_id));
        frame.push(DnaExtractId(self.dna_extract_id));
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, SequencedBy, self.sequenced_by);
        frame_push_opt!(frame, MaterialSampleId, self.material_sample_id);
        frame_push_opt!(frame, Concentration, self.concentration);
        frame_push_opt!(frame, AmpliconSize, self.amplicon_size);
        frame_push_opt!(frame, EstimatedSize, self.estimated_size);
        frame_push_opt!(frame, BaitSetName, self.bait_set_name);
        frame_push_opt!(frame, BaitSetReference, self.bait_set_reference);
        frame_push_opt!(frame, TargetGene, self.target_gene);
        frame_push_opt!(frame, DnaSequence, self.dna_sequence);
        frame
    }
}


pub struct Sequences {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl Sequences {
    /// Import the CSV file as sequence operations into the sequence_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing logs
    /// and then insert them into the database, effectively updating sequence_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        // we need a few components to fully import operation logs. the first is a CSV file reader
        // which parses each row and converts it into a frame. the second is a framer which allows
        // us to conveniently get chunks of frames from the reader and sets us up for easy parallelization.
        // and the third is the frame loader which allows us to query the database to deduplicate and
        // pull out unique operations, as well as upsert the new operations.
        let reader: CsvReader<Record> = CsvReader::from_path(self.path.clone(), self.dataset_version_id)?;
        let bars = FrameImportBars::new(reader.total_rows);
        let framer = Framer::new(reader);
        let loader = FrameLoader::<SequenceOperation>::new(get_pool()?);

        // parse and convert big chunks of rows. this is an IO bound task but for each
        // chunk we need to query the database and then insert into the database, so
        // we parallelize the frame merging and inserting instead since it is an order of
        // magnitude slower than the parsing
        for frames in framer.chunks(20_000) {
            let total_frames = frames.len();

            // we flatten out all the frames into operations and process them in chunks of 10k.
            // postgres has a parameter limit and by chunking it we can query the database to
            // filter to distinct changes and import it in bulk without triggering any errors.
            frames.operations()?.par_chunks(10_000).try_for_each(|slice| {
                let total = slice.len();

                // compare the ops with previously imported ops and only return actual changes
                let changes = distinct_changes(slice.to_vec(), &loader)?;
                let inserted = loader.upsert_operations(&changes)?;

                bars.inserted.inc(inserted as u64);
                bars.operations.inc(total as u64);
                Ok::<(), Error>(())
            })?;

            bars.total.inc(total_frames as u64);
        }

        bars.finish();
        info!("Sequence operations import finished");
        Ok(())
    }
}

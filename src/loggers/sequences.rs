use std::path::PathBuf;

use arga_core::crdt::DataFrame;
use arga_core::models::{SequenceAtom, SequenceOperation};
use arga_core::schema;
use diesel::*;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::database::FrameLoader;
use crate::errors::Error;
use crate::frame_push_opt;
use crate::readers::csv::IntoFrame;
use crate::readers::OperationLoader;

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
        crate::import_csv_as_logs::<Record, SequenceOperation>(&self.path, &self.dataset_version_id)?;
        info!("Sequence operations import finished");
        Ok(())
    }
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{LibraryAtom, LibraryOperation};
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

type LibraryFrame = DataFrame<LibraryAtom>;


impl OperationLoader for FrameLoader<LibraryOperation> {
    type Operation = LibraryOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::library_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = library_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(library_logs::all_columns())
            .order(operation_id.asc())
            .load::<LibraryOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::library_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = library_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(library_logs::all_columns())
            .order(operation_id.asc())
            .load::<LibraryOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[LibraryOperation]) -> Result<usize, Error> {
        use schema::library_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(library_logs)
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
    extract_id: String,
    library_id: String,
    scientific_name: String,
    publication_id: Option<String>,

    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,
    #[serde(deserialize_with = "try_parse_f64")]
    concentration: Option<f64>,
    concentration_unit: Option<String>,
    #[serde(deserialize_with = "try_parse_i32")]
    pcr_cycles: Option<i32>,
    layout: Option<String>,
    prepared_by: Option<String>,
    selection: Option<String>,
    bait_set_name: Option<String>,
    bait_set_reference: Option<String>,
    construction_protocol: Option<String>,
    source: Option<String>,
    insert_size: Option<String>,
    design_description: Option<String>,
    strategy: Option<String>,
    index_tag: Option<String>,
    index_dual_tag: Option<String>,
    index_oligo: Option<String>,
    index_dual_oligo: Option<String>,
    location: Option<String>,
    remarks: Option<String>,
    dna_treatment: Option<String>,
    #[serde(deserialize_with = "try_parse_i32")]
    number_of_libraries_pooled: Option<i32>,
    #[serde(deserialize_with = "try_parse_i32")]
    pcr_replicates: Option<i32>,
}

impl IntoFrame for Record {
    type Atom = LibraryAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: LibraryFrame) -> LibraryFrame {
        use LibraryAtom::*;

        frame.push(ExtractId(self.extract_id));
        frame.push(LibraryId(self.library_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, PreparedBy, self.prepared_by);
        frame_push_opt!(frame, Concentration, self.concentration);
        frame_push_opt!(frame, ConcentrationUnit, self.concentration_unit);
        frame_push_opt!(frame, PcrCycles, self.pcr_cycles);
        frame_push_opt!(frame, Layout, self.layout);
        frame_push_opt!(frame, Selection, self.selection);
        frame_push_opt!(frame, BaitSetName, self.bait_set_name);
        frame_push_opt!(frame, BaitSetReference, self.bait_set_reference);
        frame_push_opt!(frame, ConstructionProtocol, self.construction_protocol);
        frame_push_opt!(frame, Source, self.source);
        frame_push_opt!(frame, InsertSize, self.insert_size);
        frame_push_opt!(frame, DesignDescription, self.design_description);
        frame_push_opt!(frame, Strategy, self.strategy);
        frame_push_opt!(frame, IndexTag, self.index_tag);
        frame_push_opt!(frame, IndexDualTag, self.index_dual_tag);
        frame_push_opt!(frame, IndexOligo, self.index_oligo);
        frame_push_opt!(frame, IndexDualOligo, self.index_dual_oligo);
        frame_push_opt!(frame, Location, self.location);
        frame_push_opt!(frame, Remarks, self.remarks);
        frame_push_opt!(frame, DnaTreatment, self.dna_treatment);
        frame_push_opt!(frame, NumberOfLibrariesPooled, self.number_of_libraries_pooled);
        frame_push_opt!(frame, PcrReplicates, self.pcr_replicates);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, LibraryOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<LibraryOperation> {
    type Operation = models::LibraryOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::library_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::library_logs::dsl::*;
        use schema_gnl::library_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = library_entities::table
            .select(library_entities::entity_id)
            .order_by(library_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = library_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<LibraryOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::Library {
    type Atom = LibraryAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use LibraryAtom::*;

        let mut extract_id = None;
        let mut library_id = None;
        let mut publication_id = None;
        let mut scientific_name = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut concentration = None;
        let mut concentration_unit = None;
        let mut pcr_cycles = None;
        let mut layout = None;
        let mut prepared_by = None;
        let mut selection = None;
        let mut bait_set_name = None;
        let mut bait_set_reference = None;
        let mut construction_protocol = None;
        let mut source = None;
        let mut insert_size = None;
        let mut design_description = None;
        let mut strategy = None;
        let mut index_tag = None;
        let mut index_dual_tag = None;
        let mut index_oligo = None;
        let mut index_dual_oligo = None;
        let mut location = None;
        let mut remarks = None;
        let mut dna_treatment = None;
        let mut number_of_libraries_pooled = None;
        let mut pcr_replicates = None;

        for atom in atoms {
            match atom {
                Empty => {}
                ExtractId(value) => extract_id = Some(value),
                LibraryId(value) => library_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                PreparedBy(value) => prepared_by = Some(value),
                Concentration(value) => concentration = Some(value),
                ConcentrationUnit(value) => concentration_unit = Some(value),
                PcrCycles(value) => pcr_cycles = Some(value),
                Layout(value) => layout = Some(value),
                Selection(value) => selection = Some(value),
                BaitSetName(value) => bait_set_name = Some(value),
                BaitSetReference(value) => bait_set_reference = Some(value),
                ConstructionProtocol(value) => construction_protocol = Some(value),
                Source(value) => source = Some(value),
                InsertSize(value) => insert_size = Some(value),
                DesignDescription(value) => design_description = Some(value),
                Strategy(value) => strategy = Some(value),
                IndexTag(value) => index_tag = Some(value),
                IndexDualTag(value) => index_dual_tag = Some(value),
                IndexOligo(value) => index_oligo = Some(value),
                IndexDualOligo(value) => index_dual_oligo = Some(value),
                Location(value) => location = Some(value),
                Remarks(value) => remarks = Some(value),
                DnaTreatment(value) => dna_treatment = Some(value),
                NumberOfLibrariesPooled(value) => number_of_libraries_pooled = Some(value),
                PcrReplicates(value) => pcr_replicates = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let extract_id = extract_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ExtractId".to_string()))?;
        let library_id = library_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "LibraryId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let extract_entity_id = xxh3_64(extract_id.as_bytes());
        let scientific_name_entity_id = xxh3_64(scientific_name.as_bytes());

        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());
        let prepared_by_entity_id = prepared_by.map(|v| xxh3_64(v.as_bytes()).to_string());

        let record = models::Library {
            entity_id,
            extract_id: extract_entity_id.to_string(),
            species_name_id: scientific_name_entity_id as i64,
            publication_id: publication_entity_id,
            library_id,
            event_date,
            event_time,
            prepared_by: prepared_by_entity_id,
            concentration,
            concentration_unit,
            pcr_cycles,
            layout,
            selection,
            bait_set_name,
            bait_set_reference,
            construction_protocol,
            source,
            insert_size,
            design_description,
            strategy,
            index_tag,
            index_dual_tag,
            index_oligo,
            index_dual_oligo,
            location,
            remarks,
            dna_treatment,
            number_of_libraries_pooled,
            pcr_replicates,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<LibraryOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating libraries");
    let reducer: DatabaseReducer<models::Library, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::libraries::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(libraries)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    extract_id.eq(excluded(extract_id)),
                    species_name_id.eq(excluded(species_name_id)),
                    publication_id.eq(excluded(publication_id)),
                    library_id.eq(excluded(library_id)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    prepared_by.eq(excluded(prepared_by)),
                    concentration.eq(excluded(concentration)),
                    concentration_unit.eq(excluded(concentration_unit)),
                    pcr_cycles.eq(excluded(pcr_cycles)),
                    layout.eq(excluded(layout)),
                    selection.eq(excluded(selection)),
                    bait_set_name.eq(excluded(bait_set_name)),
                    bait_set_reference.eq(excluded(bait_set_reference)),
                    construction_protocol.eq(excluded(construction_protocol)),
                    source.eq(excluded(source)),
                    insert_size.eq(excluded(insert_size)),
                    design_description.eq(excluded(design_description)),
                    strategy.eq(excluded(strategy)),
                    index_tag.eq(excluded(index_tag)),
                    index_dual_tag.eq(excluded(index_dual_tag)),
                    index_oligo.eq(excluded(index_oligo)),
                    index_dual_oligo.eq(excluded(index_dual_oligo)),
                    location.eq(excluded(location)),
                    remarks.eq(excluded(remarks)),
                    dna_treatment.eq(excluded(dna_treatment)),
                    number_of_libraries_pooled.eq(excluded(number_of_libraries_pooled)),
                    pcr_replicates.eq(excluded(pcr_replicates)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}


// temporary utilities until types make its way into the rdf transformers
pub fn try_parse_i32<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let value = s.parse::<i32>().ok();
    Ok(value)
}

pub fn try_parse_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let value = s.parse::<f64>().ok();
    Ok(value)
}

use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{AssemblyAtom, AssemblyOperation};
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

type AssemblyFrame = DataFrame<AssemblyAtom>;


impl OperationLoader for FrameLoader<AssemblyOperation> {
    type Operation = AssemblyOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::assembly_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = assembly_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(assembly_logs::all_columns())
            .order(operation_id.asc())
            .load::<AssemblyOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::assembly_logs::dsl::*;
        use schema::dataset_versions;

        let mut conn = self.pool.get()?;

        let ops = assembly_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(assembly_logs::all_columns())
            .order(operation_id.asc())
            .load::<AssemblyOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[AssemblyOperation]) -> Result<usize, Error> {
        use schema::assembly_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(assembly_logs)
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
    assembly_id: String,
    scientific_name: String,
    publication_id: Option<String>,

    event_date: Option<chrono::NaiveDate>,
    event_time: Option<chrono::NaiveTime>,
    facility: Option<String>,
    name: Option<String>,
    r#type: Option<String>,
    method: Option<String>,
    method_version: Option<String>,
    method_link: Option<String>,
    size: Option<String>,
    minimum_gap_length: Option<String>,
    completeness: Option<String>,
    completeness_method: Option<String>,
    source_molecule: Option<String>,
    reference_genome_used: Option<String>,
    reference_genome_link: Option<String>,
    number_of_scaffolds: Option<String>,
    genome_coverage: Option<String>,
    hybrid: Option<String>,
    hybrid_information: Option<String>,
    polishing_or_scaffolding_method: Option<String>,
    polishing_or_scaffolding_data: Option<String>,
    computational_infrastructure: Option<String>,
    system_used: Option<String>,
    assembly_n50: Option<String>,
}

impl IntoFrame for Record {
    type Atom = AssemblyAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: AssemblyFrame) -> AssemblyFrame {
        use AssemblyAtom::*;

        frame.push(LibraryId(self.library_id));
        frame.push(AssemblyId(self.assembly_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, PublicationId, self.publication_id);
        frame_push_opt!(frame, EventDate, self.event_date);
        frame_push_opt!(frame, EventTime, self.event_time);
        frame_push_opt!(frame, Name, self.name);
        frame_push_opt!(frame, Type, self.r#type);
        frame_push_opt!(frame, Method, self.method);
        frame_push_opt!(frame, MethodVersion, self.method_version);
        frame_push_opt!(frame, MethodLink, self.method_link);
        frame_push_opt!(frame, Size, self.size);
        frame_push_opt!(frame, MinimumGapLength, self.minimum_gap_length);
        frame_push_opt!(frame, Completeness, self.completeness);
        frame_push_opt!(frame, CompletenessMethod, self.completeness_method);
        frame_push_opt!(frame, SourceMolecule, self.source_molecule);
        frame_push_opt!(frame, ReferenceGenomeUsed, self.reference_genome_used);
        frame_push_opt!(frame, ReferenceGenomeLink, self.reference_genome_link);
        frame_push_opt!(frame, NumberOfScaffolds, self.number_of_scaffolds);
        frame_push_opt!(frame, GenomeCoverage, self.genome_coverage);
        frame_push_opt!(frame, Hybrid, self.hybrid);
        frame_push_opt!(frame, HybridInformation, self.hybrid_information);
        frame_push_opt!(frame, PolishingOrScaffoldingMethod, self.polishing_or_scaffolding_method);
        frame_push_opt!(frame, PolishingOrScaffoldingData, self.polishing_or_scaffolding_data);
        frame_push_opt!(frame, ComputationalInfrastructure, self.computational_infrastructure);
        frame_push_opt!(frame, SystemUsed, self.system_used);
        frame_push_opt!(frame, AssemblyN50, self.assembly_n50);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, AssemblyOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<AssemblyOperation> {
    type Operation = models::AssemblyOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::assembly_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::assembly_logs::dsl::*;
        use schema_gnl::assembly_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = assembly_entities::table
            .select(assembly_entities::entity_id)
            .order_by(assembly_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = assembly_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<AssemblyOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

struct AssemblyWithLibrary {
    assembly: models::Assembly,
    library_entity_id: String,
}

impl Reducer<Lookups> for AssemblyWithLibrary {
    type Atom = AssemblyAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use AssemblyAtom::*;

        let mut library_id = None;
        let mut assembly_id = None;
        let mut publication_id = None;
        let mut scientific_name = None;
        let mut event_date = None;
        let mut event_time = None;
        let mut name = None;
        let mut type_ = None;
        let mut method = None;
        let mut method_version = None;
        let mut method_link = None;
        let mut size = None;
        let mut minimum_gap_length = None;
        let mut completeness = None;
        let mut completeness_method = None;
        let mut source_molecule = None;
        let mut reference_genome_used = None;
        let mut reference_genome_link = None;
        let mut number_of_scaffolds = None;
        let mut genome_coverage = None;
        let mut hybrid = None;
        let mut hybrid_information = None;
        let mut polishing_or_scaffolding_method = None;
        let mut polishing_or_scaffolding_data = None;
        let mut computational_infrastructure = None;
        let mut system_used = None;
        let mut assembly_n50 = None;


        for atom in atoms {
            match atom {
                Empty => {}
                LibraryId(value) => library_id = Some(value),
                AssemblyId(value) => assembly_id = Some(value),
                PublicationId(value) => publication_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                EventDate(value) => event_date = Some(value),
                EventTime(value) => event_time = Some(value),
                Name(value) => name = Some(value),
                Type(value) => type_ = Some(value),
                Method(value) => method = Some(value),
                MethodVersion(value) => method_version = Some(value),
                MethodLink(value) => method_link = Some(value),
                Size(value) => size = Some(value),
                MinimumGapLength(value) => minimum_gap_length = Some(value),
                Completeness(value) => completeness = Some(value),
                CompletenessMethod(value) => completeness_method = Some(value),
                SourceMolecule(value) => source_molecule = Some(value),
                ReferenceGenomeUsed(value) => reference_genome_used = Some(value),
                ReferenceGenomeLink(value) => reference_genome_link = Some(value),
                NumberOfScaffolds(value) => number_of_scaffolds = Some(value),
                GenomeCoverage(value) => genome_coverage = Some(value),
                Hybrid(value) => hybrid = Some(value),
                HybridInformation(value) => hybrid_information = Some(value),
                PolishingOrScaffoldingMethod(value) => polishing_or_scaffolding_method = Some(value),
                PolishingOrScaffoldingData(value) => polishing_or_scaffolding_data = Some(value),
                ComputationalInfrastructure(value) => computational_infrastructure = Some(value),
                SystemUsed(value) => system_used = Some(value),
                AssemblyN50(value) => assembly_n50 = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let library_id = library_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "LibraryId".to_string()))?;
        let assembly_id = assembly_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "AssemblyId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let library_entity_id = xxh3_64(library_id.as_bytes());
        let scientific_name_entity_id = xxh3_64(scientific_name.as_bytes());

        let publication_entity_id = publication_id.map(|id| xxh3_64(id.as_bytes()).to_string());

        let record = models::Assembly {
            entity_id,
            species_name_id: scientific_name_entity_id as i64,
            publication_id: publication_entity_id,
            assembly_id,
            event_date,
            event_time,
            name,
            type_,
            method,
            method_version,
            method_link,
            size,
            minimum_gap_length,
            completeness,
            completeness_method,
            source_molecule,
            reference_genome_used,
            reference_genome_link,
            number_of_scaffolds,
            genome_coverage,
            hybrid,
            hybrid_information,
            polishing_or_scaffolding_method,
            polishing_or_scaffolding_data,
            computational_infrastructure,
            system_used,
            assembly_n50,
        };

        Ok(AssemblyWithLibrary {
            assembly: record,
            library_entity_id: library_entity_id.to_string(),
        })
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<AssemblyOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating assemblies");
    let reducer: DatabaseReducer<AssemblyWithLibrary, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::assemblies::dsl::*;
            use schema::library_assemblies;

            let mut valid_records = Vec::new();
            let mut links = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => {
                        links.push((
                            library_assemblies::library_entity_id.eq(&record.library_entity_id),
                            library_assemblies::assembly_entity_id.eq(&record.assembly.entity_id),
                        ));
                        valid_records.push(&record.assembly);
                    }
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(assemblies)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    assembly_id.eq(excluded(assembly_id)),
                    species_name_id.eq(excluded(species_name_id)),
                    publication_id.eq(excluded(publication_id)),
                    event_date.eq(excluded(event_date)),
                    event_time.eq(excluded(event_time)),
                    name.eq(excluded(name)),
                    type_.eq(excluded(type_)),
                    method.eq(excluded(method)),
                    method_version.eq(excluded(method_version)),
                    method_link.eq(excluded(method_link)),
                    size.eq(excluded(size)),
                    minimum_gap_length.eq(excluded(minimum_gap_length)),
                    completeness.eq(excluded(completeness)),
                    completeness_method.eq(excluded(completeness_method)),
                    source_molecule.eq(excluded(source_molecule)),
                    reference_genome_used.eq(excluded(reference_genome_used)),
                    reference_genome_link.eq(excluded(reference_genome_link)),
                    number_of_scaffolds.eq(excluded(number_of_scaffolds)),
                    genome_coverage.eq(excluded(genome_coverage)),
                    hybrid.eq(excluded(hybrid)),
                    hybrid_information.eq(excluded(hybrid_information)),
                    polishing_or_scaffolding_method.eq(excluded(polishing_or_scaffolding_method)),
                    polishing_or_scaffolding_data.eq(excluded(polishing_or_scaffolding_data)),
                    computational_infrastructure.eq(excluded(computational_infrastructure)),
                    system_used.eq(excluded(system_used)),
                    assembly_n50.eq(excluded(assembly_n50)),
                ))
                .execute(&mut conn)?;

            diesel::insert_into(library_assemblies::table)
                .values(links)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

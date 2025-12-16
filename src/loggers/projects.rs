use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{ProjectAtom, ProjectOperation};
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
use crate::utils::{new_progress_bar, parse_string_array_opt, to_pg_array};
use crate::{FrameProgress, frame_push_opt, import_compressed_csv_stream};

type ProjectFrame = DataFrame<ProjectAtom>;


impl OperationLoader for FrameLoader<ProjectOperation> {
    type Operation = ProjectOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::project_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = project_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(project_logs::all_columns())
            .order(operation_id.asc())
            .load::<ProjectOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::project_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = project_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(project_logs::all_columns())
            .order(operation_id.asc())
            .load::<ProjectOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[ProjectOperation]) -> Result<usize, Error> {
        use schema::project_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(project_logs)
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
    project_id: String,

    scientific_name: Option<String>,
    initiative: Option<String>,
    initiative_theme: Option<String>,
    title: Option<String>,
    description: Option<String>,

    #[serde(deserialize_with = "parse_string_array_opt")]
    data_context: Option<Vec<String>>,
    #[serde(deserialize_with = "parse_string_array_opt")]
    data_types: Option<Vec<String>>,
    #[serde(deserialize_with = "parse_string_array_opt")]
    data_assay_types: Option<Vec<String>>,
    #[serde(deserialize_with = "parse_string_array_opt")]
    partners: Option<Vec<String>>,
}

impl IntoFrame for Record {
    type Atom = ProjectAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: ProjectFrame) -> ProjectFrame {
        use ProjectAtom::*;

        frame.push(ProjectId(self.project_id));
        frame_push_opt!(frame, TargetSpecies, self.scientific_name);
        frame_push_opt!(frame, Initiative, self.initiative);
        frame_push_opt!(frame, InitiativeTheme, self.initiative_theme);
        frame_push_opt!(frame, Title, self.title);
        frame_push_opt!(frame, Description, self.description);
        frame_push_opt!(frame, DataContext, self.data_context);
        frame_push_opt!(frame, DataTypes, self.data_types);
        frame_push_opt!(frame, DataAssayTypes, self.data_assay_types);
        frame_push_opt!(frame, Partners, self.partners);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, ProjectOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<ProjectOperation> {
    type Operation = models::ProjectOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::project_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::project_logs::dsl::*;
        use schema_gnl::project_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = project_entities::table
            .select(project_entities::entity_id)
            .order_by(project_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = project_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<ProjectOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups;

impl Reducer<Lookups> for models::Project {
    type Atom = ProjectAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use ProjectAtom::*;

        let mut project_id = None;
        let mut target_species = None;
        let mut title = None;
        let mut description = None;
        let mut initiative = None;
        let mut initiative_theme = None;
        let mut registration_date = None;
        let mut data_context = None;
        let mut data_types = None;
        let mut data_assay_types = None;
        let mut partners = None;


        for atom in atoms {
            match atom {
                Empty => {}
                ProjectId(value) => project_id = Some(value),
                TargetSpecies(value) => target_species = Some(value),
                Title(value) => title = Some(value),
                Description(value) => description = Some(value),
                Initiative(value) => initiative = Some(value),
                InitiativeTheme(value) => initiative_theme = Some(value),
                RegistrationDate(value) => registration_date = Some(value),
                DataContext(items) => data_context = Some(items),
                DataTypes(items) => data_types = Some(items),
                DataAssayTypes(items) => data_assay_types = Some(items),
                Partners(items) => partners = Some(items),
            }
        }

        let target_species_name_id = target_species.map(|id| xxh3_64(id.as_bytes()) as i64);

        let record = models::Project {
            entity_id,
            project_id,
            target_species_name_id,
            title,
            description,
            initiative,
            registration_date,
            data_context: data_context.map(to_pg_array),
            data_types: data_types.map(to_pg_array),
            data_assay_types: data_assay_types.map(to_pg_array),
            partners: partners.map(to_pg_array),
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let pool = crate::database::get_pool()?;
    let mut conn = pool.get()?;

    let pager: FrameLoader<ProjectOperation> = FrameLoader::new(pool.clone());

    let bar = new_progress_bar(pager.total()? as usize, "Updating projects");
    let reducer: DatabaseReducer<models::Project, _, _> = DatabaseReducer::new(pager, Lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::projects::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(projects)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    project_id.eq(excluded(project_id)),
                    target_species_name_id.eq(excluded(target_species_name_id)),
                    title.eq(excluded(title)),
                    description.eq(excluded(description)),
                    initiative.eq(excluded(initiative)),
                    registration_date.eq(excluded(registration_date)),
                    data_context.eq(excluded(data_context)),
                    data_types.eq(excluded(data_types)),
                    data_assay_types.eq(excluded(data_assay_types)),
                    partners.eq(excluded(partners)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

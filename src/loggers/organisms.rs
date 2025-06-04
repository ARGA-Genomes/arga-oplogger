use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::{self, DatasetVersion, OrganismAtom, OrganismOperation};
use arga_core::{schema, schema_gnl};
use diesel::*;
use serde::Deserialize;
use tracing::error;

use crate::database::{name_lookup, FrameLoader, StringMap};
use crate::errors::*;
use crate::frames::IntoFrame;
use crate::readers::{meta, OperationLoader};
use crate::reducer::{DatabaseReducer, EntityPager, Reducer};
use crate::utils::new_progress_bar;
use crate::{frame_push_opt, import_compressed_csv_stream, FrameProgress};

type OrganismFrame = DataFrame<OrganismAtom>;


impl OperationLoader for FrameLoader<OrganismOperation> {
    type Operation = OrganismOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::organism_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = organism_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(organism_logs::all_columns())
            .order(operation_id.asc())
            .load::<OrganismOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::organism_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = organism_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(organism_logs::all_columns())
            .order(operation_id.asc())
            .load::<OrganismOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[OrganismOperation]) -> Result<usize, Error> {
        use schema::organism_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(organism_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

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
    scientific_name: String,
    organism_id: String,

    sex: Option<String>,
    genotypic_sex: Option<String>,
    phenotypic_sex: Option<String>,
    life_stage: Option<String>,
    reproductive_condition: Option<String>,
    behavior: Option<String>,
}

impl IntoFrame for Record {
    type Atom = OrganismAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: OrganismFrame) -> OrganismFrame {
        use OrganismAtom::*;

        frame.push(OrganismId(self.organism_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, Sex, self.sex);
        frame_push_opt!(frame, GenotypicSex, self.genotypic_sex);
        frame_push_opt!(frame, PhenotypicSex, self.phenotypic_sex);
        frame_push_opt!(frame, LifeStage, self.life_stage);
        frame_push_opt!(frame, ReproductiveCondition, self.reproductive_condition);
        frame_push_opt!(frame, Behavior, self.behavior);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, OrganismOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<OrganismOperation> {
    type Operation = models::OrganismOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::organism_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::organism_logs::dsl::*;
        use schema_gnl::organism_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = organism_entities::table
            .select(organism_entities::entity_id)
            .order_by(organism_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = organism_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<OrganismOperation>(&mut conn)?;

        Ok(operations)
    }
}


struct Lookups {
    names: StringMap,
}

impl Reducer<Lookups> for models::Organism {
    type Atom = OrganismAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use OrganismAtom::*;

        let mut organism_id = None;
        let mut scientific_name = None;
        let mut sex = None;
        let mut genotypic_sex = None;
        let mut phenotypic_sex = None;
        let mut life_stage = None;
        let mut reproductive_condition = None;
        let mut behavior = None;

        for atom in atoms {
            match atom {
                Empty => {}
                OrganismId(value) => organism_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                Sex(value) => sex = Some(value),
                GenotypicSex(value) => genotypic_sex = Some(value),
                PhenotypicSex(value) => phenotypic_sex = Some(value),
                LifeStage(value) => life_stage = Some(value),
                ReproductiveCondition(value) => reproductive_condition = Some(value),
                Behavior(value) => behavior = Some(value),
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;
        let organism_id = organism_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "OrganismId".to_string()))?;

        let record = models::Organism {
            entity_id,

            // everything in our database basically links to a name. we never should get an error
            // here as all names _should_ be imported with every dataset. however that is outside
            // the control of the oplogger so if you can't match a name make a loud noise
            name_id: lookups
                .names
                .get(&scientific_name)
                .ok_or(LookupError::Name(scientific_name))?
                .clone(),

            organism_id,
            sex,
            genotypic_sex,
            phenotypic_sex,
            life_stage,
            reproductive_condition,
            behavior,
        };

        Ok(record)
    }
}


pub fn update() -> Result<(), Error> {
    let mut pool = crate::database::get_pool()?;

    let lookups = Lookups {
        names: name_lookup(&mut pool)?,
    };

    let pager: FrameLoader<OrganismOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating organisms");

    let reducer: DatabaseReducer<models::Organism, _, _> = DatabaseReducer::new(pager, lookups);
    let mut conn = pool.get()?;

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::organisms::dsl::*;

            let mut valid_records = Vec::new();
            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(organisms)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    name_id.eq(excluded(name_id)),
                    organism_id.eq(excluded(organism_id)),
                    sex.eq(excluded(sex)),
                    genotypic_sex.eq(excluded(genotypic_sex)),
                    phenotypic_sex.eq(excluded(phenotypic_sex)),
                    life_stage.eq(excluded(life_stage)),
                    reproductive_condition.eq(excluded(reproductive_condition)),
                    behavior.eq(excluded(behavior)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();

    Ok(())
}

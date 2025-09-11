use std::io::Read;

use arga_core::crdt::DataFrame;
use arga_core::models::logs::{TissueAtom, TissueOperation};
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

type TissueFrame = DataFrame<TissueAtom>;


impl OperationLoader for FrameLoader<TissueOperation> {
    type Operation = TissueOperation;

    fn load_operations(&self, version: &DatasetVersion, entity_ids: &[&String]) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::tissue_logs::dsl::*;

        let mut conn = self.pool.get()?;

        // NOTE: there's no good reason to use a UUID for the dataset version, but if we instead
        // used a hybrid logical clock derived from the meta.toml then it would be trivial to find
        // all operations that occur <= a certain dataset
        //
        // it would still be nice to be able to mark all the operations with a HLC derived from the
        // the dataset but the problem there is the limited space for the logical component. it would
        // not be unreasonable to try and import a single dataset with over 5 million records.
        let ops = tissue_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(tissue_logs::all_columns())
            .order(operation_id.asc())
            .load::<TissueOperation>(&mut conn)?;

        Ok(ops)
    }

    fn load_dataset_operations(
        &self,
        version: &DatasetVersion,
        entity_ids: &[&String],
    ) -> Result<Vec<Self::Operation>, Error> {
        use schema::dataset_versions;
        use schema::tissue_logs::dsl::*;

        let mut conn = self.pool.get()?;

        let ops = tissue_logs
            .inner_join(dataset_versions::table.on(dataset_versions::id.eq(dataset_version_id)))
            .filter(dataset_versions::dataset_id.eq(version.dataset_id))
            .filter(dataset_versions::created_at.le(version.created_at))
            .filter(entity_id.eq_any(entity_ids))
            .select(tissue_logs::all_columns())
            .order(operation_id.asc())
            .load::<TissueOperation>(&mut conn)?;

        Ok(ops)
    }

    fn upsert_operations(&self, operations: &[TissueOperation]) -> Result<usize, Error> {
        use schema::tissue_logs::dsl::*;
        let mut conn = self.pool.get()?;

        let inserted = diesel::insert_into(tissue_logs)
            .values(operations)
            .execute(&mut conn)
            .unwrap();

        Ok(inserted)
    }
}


pub fn parse_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(match s.trim().to_lowercase().as_str() {
        "" => None,
        "true" => Some(true),
        "t" => Some(true),
        "1" => Some(true),
        "yes" => Some(true),
        _ => Some(false),
    })
}


// A single row in a supported CSV file.
//
// There can be many tissues per collection material sample and every
// tissue should have it's own specimen stub record.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    entity_id: String,
    tissue_id: String,
    material_sample_id: String,
    organism_id: String,
    scientific_name: String,

    #[serde(deserialize_with = "parse_bool")]
    identification_verified: Option<bool>,
    #[serde(deserialize_with = "parse_bool")]
    reference_material: Option<bool>,

    custodian: Option<String>,
    institution: Option<String>,
    institution_code: Option<String>,
    sampling_protocol: Option<String>,
    tissue_type: Option<String>,
    disposition: Option<String>,
    fixation: Option<String>,
    storage: Option<String>,
}

impl IntoFrame for Record {
    type Atom = TissueAtom;

    fn entity_hashable(&self) -> &[u8] {
        self.entity_id.as_bytes()
    }

    fn into_frame(self, mut frame: TissueFrame) -> TissueFrame {
        use TissueAtom::*;

        frame.push(TissueId(self.tissue_id));
        frame.push(MaterialSampleId(self.material_sample_id));
        frame.push(OrganismId(self.organism_id));
        frame.push(ScientificName(self.scientific_name));
        frame_push_opt!(frame, IdentificationVerified, self.identification_verified);
        frame_push_opt!(frame, ReferenceMaterial, self.reference_material);
        frame_push_opt!(frame, Custodian, self.custodian);
        frame_push_opt!(frame, Institution, self.institution);
        frame_push_opt!(frame, InstitutionCode, self.institution_code);
        frame_push_opt!(frame, SamplingProtocol, self.sampling_protocol);
        frame_push_opt!(frame, TissueType, self.tissue_type);
        frame_push_opt!(frame, Disposition, self.disposition);
        frame_push_opt!(frame, Fixation, self.fixation);
        frame_push_opt!(frame, Storage, self.storage);
        frame
    }
}


pub fn import_archive<S: Read + FrameProgress>(stream: S, dataset: &meta::Dataset) -> Result<(), Error> {
    import_compressed_csv_stream::<S, Record, TissueOperation>(stream, dataset)
}


impl EntityPager for FrameLoader<TissueOperation> {
    type Operation = models::TissueOperation;

    fn total(&self) -> Result<i64, Error> {
        let mut conn = self.pool.get()?;
        let total = schema_gnl::tissue_entities::table
            .count()
            .get_result::<i64>(&mut conn)?;
        Ok(total)
    }

    fn load_entity_operations(&self, page: usize) -> Result<Vec<Self::Operation>, Error> {
        use schema::tissue_logs::dsl::*;
        use schema_gnl::tissue_entities;

        let mut conn = self.pool.get()?;

        let limit = 1000;
        let offset = page as i64 * limit;

        let entity_ids = tissue_entities::table
            .select(tissue_entities::entity_id)
            .order_by(tissue_entities::entity_id)
            .offset(offset)
            .limit(limit)
            .into_boxed();

        let operations = tissue_logs
            .filter(entity_id.eq_any(entity_ids))
            .order_by(operation_id)
            .load::<TissueOperation>(&mut conn)?;

        Ok(operations)
    }
}


#[derive(Clone)]
struct Lookups {
    names: StringMap,
}

impl Reducer<Lookups> for models::Tissue {
    type Atom = TissueAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, _lookups: &Lookups) -> Result<Self, Error> {
        use TissueAtom::*;

        let mut material_sample_id = None;
        let mut tissue_id = None;
        let mut identification_verified = None;
        let mut reference_material = None;
        let mut custodian = None;
        let mut institution = None;
        let mut institution_code = None;
        let mut sampling_protocol = None;
        let mut tissue_type = None;
        let mut disposition = None;
        let mut fixation = None;
        let mut storage = None;

        for atom in atoms {
            match atom {
                Empty => {}
                TissueId(value) => tissue_id = Some(value),
                MaterialSampleId(value) => material_sample_id = Some(value),
                IdentificationVerified(value) => identification_verified = Some(value),
                ReferenceMaterial(value) => reference_material = Some(value),
                Custodian(value) => custodian = Some(value),
                Institution(value) => institution = Some(value),
                InstitutionCode(value) => institution_code = Some(value),
                SamplingProtocol(value) => sampling_protocol = Some(value),
                TissueType(value) => tissue_type = Some(value),
                Disposition(value) => disposition = Some(value),
                Fixation(value) => fixation = Some(value),
                Storage(value) => storage = Some(value),
                // atoms used for the specimen stub
                OrganismId(_) => {}
                ScientificName(_) => {}
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let tissue_id = tissue_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "TissueId".to_string()))?;
        let material_sample_id =
            material_sample_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "MaterialSampleId".to_string()))?;

        let specimen_entity_id = xxh3_64(tissue_id.as_bytes());
        let material_sample_entity_id = xxh3_64(material_sample_id.as_bytes());


        let record = models::Tissue {
            entity_id,
            specimen_id: specimen_entity_id.to_string(),
            material_sample_id: material_sample_entity_id.to_string(),
            tissue_id,

            identification_verified,
            reference_material,
            custodian,
            institution,
            institution_code,
            sampling_protocol,
            tissue_type,
            disposition,
            fixation,
            storage,
        };

        Ok(record)
    }
}

impl Reducer<Lookups> for models::Specimen {
    type Atom = TissueAtom;

    fn reduce(entity_id: String, atoms: Vec<Self::Atom>, lookups: &Lookups) -> Result<Self, Error> {
        use TissueAtom::*;

        let mut organism_id = None;
        let mut scientific_name = None;

        for atom in atoms {
            match atom {
                OrganismId(value) => organism_id = Some(value),
                ScientificName(value) => scientific_name = Some(value),
                _ => {}
            }
        }

        // error out if a mandatory atom is not present. without these fields
        // we cannot construct a reduced record
        let organism_id = organism_id.ok_or(ReduceError::MissingAtom(entity_id.clone(), "OrganismId".to_string()))?;
        let scientific_name =
            scientific_name.ok_or(ReduceError::MissingAtom(entity_id.clone(), "ScientificName".to_string()))?;

        let organism_id = xxh3_64(organism_id.as_bytes());


        let record = models::Specimen {
            entity_id,
            organism_id: organism_id.to_string(),
            // everything in our database basically links to a name. we never should get an error
            // here as all names _should_ be imported with every dataset. however that is outside
            // the control of the oplogger so if you can't match a name make a loud noise
            name_id: lookups
                .names
                .get(&scientific_name)
                .ok_or(LookupError::Name(scientific_name))?
                .clone(),
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

    let pager: FrameLoader<TissueOperation> = FrameLoader::new(pool.clone());
    let bar = new_progress_bar(pager.total()? as usize, "Updating specimens");

    // insert specimen records first as the tissues will link to it via the entity id
    // TODO: non ideal use of cloning here. we should at least allow lookups to be sent as references
    let reducer: DatabaseReducer<models::Specimen, _, _> = DatabaseReducer::new(pager.clone(), lookups.clone());
    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::specimens::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(specimens)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    entity_id.eq(excluded(entity_id)),
                    organism_id.eq(excluded(organism_id)),
                    name_id.eq(excluded(name_id)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }
    bar.finish();

    // insert the tissues
    let bar = new_progress_bar(pager.total()? as usize, "Updating tissues");
    let reducer: DatabaseReducer<models::Tissue, _, _> = DatabaseReducer::new(pager, lookups);

    for records in reducer.into_iter() {
        for chunk in records.chunks(1000) {
            use diesel::upsert::excluded;
            use schema::tissues::dsl::*;

            let mut valid_records = Vec::new();

            for record in chunk {
                match record {
                    Ok(record) => valid_records.push(record),
                    Err(err) => error!(?err),
                }
            }

            // postgres always creates a new row version so we cant get
            // an actual figure of the amount of records changed
            diesel::insert_into(tissues)
                .values(valid_records)
                .on_conflict(entity_id)
                .do_update()
                .set((
                    specimen_id.eq(excluded(specimen_id)),
                    material_sample_id.eq(excluded(material_sample_id)),
                    tissue_id.eq(excluded(tissue_id)),
                    identification_verified.eq(excluded(identification_verified)),
                    reference_material.eq(excluded(reference_material)),
                    custodian.eq(excluded(custodian)),
                    institution.eq(excluded(institution)),
                    institution_code.eq(excluded(institution_code)),
                    sampling_protocol.eq(excluded(sampling_protocol)),
                    tissue_type.eq(excluded(tissue_type)),
                    disposition.eq(excluded(disposition)),
                    fixation.eq(excluded(fixation)),
                    storage.eq(excluded(storage)),
                ))
                .execute(&mut conn)?;

            bar.inc(chunk.len() as u64);
        }
    }

    bar.finish();
    Ok(())
}

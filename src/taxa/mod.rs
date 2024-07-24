mod utils;

use std::path::PathBuf;

use arga_core::crdt::{DataFrame, Version};
use arga_core::models::{TaxonAtom, TaxonOperation, TaxonomicRank, TaxonomicStatus};
use arga_core::schema;
use diesel::*;
use serde::Deserialize;
use utils::{taxonomic_rank_from_str, taxonomic_status_from_str};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::database::get_pool;
use crate::errors::Error;


type TaxonFrame = DataFrame<TaxonAtom>;


/// The CSV record to decompose into operation logs.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Deserialize)]
struct Record {
    /// The record id assigned by the dataset
    taxon_id: String,
    /// The scientific name of the parent taxon to link up in a tree
    parent_taxon: Option<String>,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The authorship of the taxon
    scientific_name_authorship: Option<String>,

    /// The name of the taxon without the author
    canonical_name: String,

    /// The rank of the taxon. Refer to TaxonomicRank for all options
    #[serde(deserialize_with = "taxonomic_rank_from_str")]
    taxon_rank: TaxonomicRank,
    /// The status of the taxon. Refer to TaxonomicStatus for all options
    #[serde(deserialize_with = "taxonomic_status_from_str")]
    taxonomic_status: TaxonomicStatus,

    /// The code used to define the taxon. Eg. ICZN
    nomenclatural_code: String,

    citation: Option<String>,
    references: Option<String>,
    last_updated: Option<String>,
}


pub struct Taxa {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl Taxa {
    pub fn taxa(&self) -> Result<Vec<TaxonOperation>, Error> {
        use TaxonAtom::*;

        let mut records: Vec<Record> = Vec::new();
        for row in csv::Reader::from_path(&self.path)?.deserialize() {
            records.push(row?);
        }

        let mut last_version = Version::new();
        let mut operations: Vec<TaxonOperation> = Vec::new();

        for record in records.into_iter() {
            // because arga supports multiple taxonomic systems we use the taxon_id
            // from the system as the unique entity id. if we used the scientific name
            // instead then we would combine and reduce changes from all systems which
            // is not desireable for our purposes
            let mut hasher = Xxh3::new();
            hasher.update(record.taxon_id.as_bytes());
            let hash = hasher.digest();

            let mut frame = TaxonFrame::create(hash.to_string(), self.dataset_version_id, last_version);
            frame.push(TaxonId(record.taxon_id));
            frame.push(ScientificName(record.scientific_name));
            frame.push(CanonicalName(record.canonical_name));
            frame.push(TaxonomicRank(record.taxon_rank));
            frame.push(TaxonomicStatus(record.taxonomic_status));
            frame.push(NomenclaturalCode(record.nomenclatural_code));

            if let Some(value) = record.parent_taxon {
                frame.push(ParentTaxon(value));
            }
            if let Some(value) = record.scientific_name_authorship {
                frame.push(Authorship(value));
            }
            if let Some(value) = record.citation {
                frame.push(Citation(value));
            }
            if let Some(value) = record.references {
                frame.push(References(value));
            }
            if let Some(value) = record.last_updated {
                frame.push(LastUpdated(value));
            }

            last_version = frame.last_version();
            operations.extend(frame.collect());
        }

        Ok(operations)
    }

    pub fn import(&self) -> Result<(), Error> {
        use schema::taxa_logs::dsl::*;

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let records = self.taxa()?;

        for chunk in records.chunks(1000) {
            diesel::insert_into(taxa_logs)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;
        }

        Ok(())
    }
}

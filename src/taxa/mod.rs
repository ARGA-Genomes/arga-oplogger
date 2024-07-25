mod utils;

use std::path::PathBuf;
use std::time::Duration;

use arga_core::crdt::lww::Map;
use arga_core::crdt::{DataFrame, Version};
use arga_core::models::{TaxonAtom, TaxonOperation, TaxonOperationWithDataset, TaxonomicRank, TaxonomicStatus};
use arga_core::schema;
use diesel::*;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use utils::{taxonomic_rank_from_str, taxonomic_status_from_str};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::database::get_pool;
use crate::errors::Error;
use crate::operations::{group_operations, merge_operations};
use crate::{PROGRESS_TEMPLATE, SPINNER_TEMPLATE};


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


/// The ARGA taxon CSV record output
/// This is the record in a CSV after reducing the taxa logs
/// from multiple datasets.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Taxon {
    /// The id of this record entity in the taxa logs
    entity_id: String,
    /// The id of the taxon as determined by the source dataset
    taxon_id: String,
    /// The scientific name of the parent taxon. Useful for taxonomy trees
    parent_taxon: Option<String>,
    /// The external identifier of the source dataset as determined by ARGA
    dataset_id: Option<String>,

    /// The name of the taxon. Should include author when possible
    scientific_name: String,
    /// The authorship of the taxon
    scientific_name_authorship: Option<String>,
    /// The name of the taxon without the author
    canonical_name: String,
    /// The code used to define the taxon. Eg. ICZN
    nomenclatural_code: String,

    /// The rank of the taxon. Refer to TaxonomicRank for all options
    taxon_rank: TaxonomicRank,
    /// The status of the taxon. Refer to TaxonomicStatus for all options
    taxonomic_status: TaxonomicStatus,

    citation: Option<String>,
    references: Option<String>,
    last_updated: Option<String>,
}


pub struct Taxa {
    pub path: PathBuf,
    pub dataset_version_id: Uuid,
}

impl Taxa {
    /// Process a taxonomy CSV file and convert the records to a list of operations.
    pub fn taxa(&self) -> Result<Vec<TaxonOperation>, Error> {
        use TaxonAtom::*;

        let style = ProgressStyle::with_template(PROGRESS_TEMPLATE).expect("Invalid progress bar template");
        let spinner = ProgressStyle::with_template(SPINNER_TEMPLATE).expect("Invalid spinner template");

        let bar = ProgressBar::new_spinner()
            .with_message("Parsing taxonomy CSV file")
            .with_style(spinner);
        bar.enable_steady_tick(Duration::from_millis(100));

        let mut records: Vec<Record> = Vec::new();
        for row in csv::Reader::from_path(&self.path)?.deserialize() {
            records.push(row?);
        }
        bar.finish();


        let bar = ProgressBar::new(records.len() as u64)
            .with_message("Decomposing records into taxa operation logs")
            .with_style(style);

        // an operation represents one field and a record is a single entity so we have a logical grouping
        // that would be ideal to represent as a frame (like a database transaction). this allows us to
        // leverage the logical clock in the operation_id to closely associate these fields and make it
        // obvious that they occur within the same record, a kind of co-locating of operations.
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

            bar.inc(1);
        }
        bar.finish();

        Ok(operations)
    }

    /// Import the CSV file as taxon operations into the taxa_logs table.
    ///
    /// This will parse and decompose the CSV file, merge it with the existing taxa logs
    /// and then insert them into the database, effectively updating taxa_logs with the
    /// latest changes from the dataset.
    pub fn import(&self) -> Result<(), Error> {
        use schema::taxa_logs::dsl::*;

        let style = ProgressStyle::with_template(PROGRESS_TEMPLATE).expect("Invalid progress bar template");
        let spinner = ProgressStyle::with_template(SPINNER_TEMPLATE).expect("Invalid spinner template");

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let bar = ProgressBar::new_spinner()
            .with_message("Loading existing taxon operations")
            .with_style(spinner.clone());
        bar.enable_steady_tick(Duration::from_millis(100));

        // load the existing operations. this can be quite large it includes
        // all operations ever, and is a grow-only table.
        // a future memory optimised operation could instead group by entity id
        // first and then query large chunks of logs in parallel
        let taxon_ops = taxa_logs.order(operation_id.asc()).load::<TaxonOperation>(&mut conn)?;
        bar.finish();

        // parse and decompose the input file into taxon operations
        let records = self.taxa()?;

        let bar = ProgressBar::new_spinner()
            .with_message("Merging existing and new operations")
            .with_style(spinner);
        bar.enable_steady_tick(Duration::from_millis(100));

        // merge the new operations with the existing ones in the database
        // to deduplicate all ops
        let records = merge_operations(taxon_ops, records)?;
        bar.finish();

        let bar = ProgressBar::new(records.len() as u64)
            .with_message("Importing taxon operations")
            .with_style(style);

        let mut total_imported = 0;
        // finally import the operations. if there is a conflict based on the operation_id
        // then it is a duplicate operation so do nothing with it
        for chunk in records.chunks(1000) {
            let inserted = diesel::insert_into(taxa_logs)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;

            total_imported += inserted;
            bar.inc(1000);
        }

        bar.finish();
        println!("Imported {total_imported} taxon operations");
        Ok(())
    }

    /// Reduce the entire taxa_logs table into an ARGA CSV file.
    ///
    /// This will generate a snapshot of every taxon built from all datasets
    /// using the last-write-win CRDT map. The snapshot output is a reproducible
    /// dataset that should be imported into the ARGA database and used by the application.
    pub fn reduce() -> Result<(), Error> {
        use schema::taxa_logs::dsl::*;
        use schema::{dataset_versions, datasets};

        let pool = get_pool()?;
        let mut conn = pool.get()?;

        let ops = taxa_logs
            .inner_join(dataset_versions::table.on(dataset_version_id.eq(dataset_versions::id)))
            .inner_join(datasets::table.on(dataset_versions::dataset_id.eq(datasets::id)))
            .order(operation_id.asc())
            .load::<TaxonOperationWithDataset>(&mut conn)?;

        let entities = group_operations(ops, vec![])?;
        let mut taxa = Vec::new();

        for (key, ops) in entities.into_iter() {
            let mut map = Map::new(key);
            map.reduce(&ops);

            // include the dataset global id in the reduced output to
            // allow for multiple taxonomic systems
            let mut taxon = Taxon::from(map);
            if let Some(op) = ops.first() {
                taxon.dataset_id = Some(op.dataset.global_id.clone());
                taxa.push(taxon);
            }
        }

        // our taxa table has a unique constraint on scientific name and dataset id.
        // some data sources will duplicate a taxon (separate record id) to record
        // multiple accepted names. we sort and deduplicate it here since the relationship
        // between taxa isn't of concern here, just the name itself.
        taxa.sort_by(|a, b| {
            a.dataset_id
                .cmp(&b.dataset_id)
                .then_with(|| a.scientific_name.cmp(&b.scientific_name))
        });
        taxa.dedup_by(|a, b| a.scientific_name == b.scientific_name && a.dataset_id == b.dataset_id);

        let mut writer = csv::Writer::from_writer(std::io::stdout());

        for taxon in taxa {
            writer.serialize(taxon)?;
        }

        Ok(())
    }
}


/// Converts a LWW CRDT map of taxon atoms to a Taxon record for serialisation
impl From<Map<TaxonAtom>> for Taxon {
    fn from(value: Map<TaxonAtom>) -> Self {
        use TaxonAtom::*;

        let mut taxon = Taxon {
            entity_id: value.entity_id,
            ..Default::default()
        };

        for val in value.atoms.into_values() {
            match val {
                Empty => {}
                TaxonId(value) => taxon.taxon_id = value,
                ParentTaxon(value) => taxon.parent_taxon = Some(value),
                ScientificName(value) => taxon.scientific_name = value,
                Authorship(value) => taxon.scientific_name_authorship = Some(value),
                CanonicalName(value) => taxon.canonical_name = value,
                NomenclaturalCode(value) => taxon.nomenclatural_code = value,
                TaxonomicRank(value) => taxon.taxon_rank = value,
                TaxonomicStatus(value) => taxon.taxonomic_status = value,
                Citation(value) => taxon.citation = Some(value),
                References(value) => taxon.references = Some(value),
                LastUpdated(value) => taxon.last_updated = Some(value),

                // fields currently not supported
                AcceptedNameUsageId(_value) => {}
                ParentNameUsageId(_value) => {}
                AcceptedNameUsage(_value) => {}
                ParentNameUsage(_value) => {}
                NomenclaturalStatus(_value) => {}
                NamePublishedIn(_value) => {}
                NamePublishedInYear(_value) => {}
                NamePublishedInUrl(_value) => {}
            }
        }

        taxon
    }
}

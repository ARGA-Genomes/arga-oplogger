mod collections;
mod database;
mod errors;
mod names;
mod nomenclatural_acts;
mod operations;
mod readers;
mod taxa;
mod taxonomic_acts;
mod utils;

use std::path::PathBuf;

use clap::Parser;
use database::create_dataset_version;
use errors::Error;

/// The ARGA operation logger
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
pub enum Commands {
    /// Process and import a csv as operation logs
    #[command(subcommand)]
    Import(ImportCommand),

    /// Reduce operation logs and output as an ARGA CSV
    #[command(subcommand)]
    Reduce(ReduceCommand),

    /// Update the database with the latest reduced data
    #[command(subcommand)]
    Update(UpdateCommand),
}

#[derive(clap::Subcommand)]
pub enum ImportCommand {
    /// Import taxa from a CSV dataset
    Taxa {
        /// The global identifier describing the dataset
        dataset_id: String,
        /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
        version: String,
        /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
        created_at: String,
        /// The path to the CSV file to import as operation logs
        path: PathBuf,
    },

    /// Import taxonomic acts from a CSV dataset
    TaxonomicActs {
        /// The global identifier describing the dataset
        dataset_id: String,
        /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
        version: String,
        /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
        created_at: String,
        /// The path to the CSV file to import as operation logs
        path: PathBuf,
    },

    /// Import nomenclatural acts from a CSV dataset
    NomenclaturalActs {
        /// The global identifier describing the dataset
        dataset_id: String,
        /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
        version: String,
        /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
        created_at: String,
        /// The path to the CSV file to import as operation logs
        path: PathBuf,
    },

    /// Import collections from a CSV dataset
    Collections {
        /// The global identifier describing the dataset
        dataset_id: String,
        /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
        version: String,
        /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
        created_at: String,
        /// The path to the CSV file to import as operation logs
        path: PathBuf,
    },
}

#[derive(clap::Subcommand)]
pub enum ReduceCommand {
    /// Reduce taxa logs into a CSV
    Taxa,
    /// Reduce taxonomic act logs into a CSV
    TaxonomicActs,
}

#[derive(clap::Subcommand)]
pub enum UpdateCommand {
    /// Update the taxa with the reduced logs
    Taxa,
    /// Update taxonomic acts with the reduced logs
    TaxonomicActs,
    /// Update nomenclatural acts with the reduced logs
    NomenclaturalActs,
}

fn main() -> Result<(), Error> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Import(cmd) => match cmd {
            ImportCommand::Taxa {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let taxa = taxa::Taxa {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                taxa.import()?
            }

            ImportCommand::TaxonomicActs {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let taxa = taxonomic_acts::TaxonomicActs {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                taxa.import()?
            }

            ImportCommand::NomenclaturalActs {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let acts = nomenclatural_acts::NomenclaturalActs {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                acts.import()?
            }

            ImportCommand::Collections {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let collections = collections::Collections {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                collections.import()?
            }
        },
        Commands::Reduce(cmd) => match cmd {
            ReduceCommand::Taxa => {
                let records = taxa::Taxa::reduce()?;
                let mut writer = csv::Writer::from_writer(std::io::stdout());
                for record in records {
                    writer.serialize(record)?;
                }
            }
            ReduceCommand::TaxonomicActs => {
                let records = taxonomic_acts::TaxonomicActs::reduce()?;
                let mut writer = csv::Writer::from_writer(std::io::stdout());
                for record in records {
                    writer.serialize(record)?;
                }
            }
        },

        Commands::Update(cmd) => match cmd {
            UpdateCommand::Taxa => {
                taxa::Taxa::update()?;
                taxa::Taxa::link()?;
            }
            UpdateCommand::TaxonomicActs => taxonomic_acts::TaxonomicActs::update()?,
            UpdateCommand::NomenclaturalActs => nomenclatural_acts::NomenclaturalActs::update()?,
        },
    }

    Ok(())
}

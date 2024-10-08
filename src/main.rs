mod archive;
mod database;
mod errors;
mod loggers;
mod operations;
mod readers;
mod utils;

use std::path::PathBuf;

use clap::Parser;
use database::create_dataset_version;
use errors::Error;
use loggers::*;

use crate::datasets::Datasets;
use crate::sources::Sources;

/// The ARGA operation logger
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
pub enum Commands {
    /// Process and import an ARGA dataset archive as operation logs
    Import { path: PathBuf },

    /// Process and import a csv as operation logs
    #[command(subcommand)]
    ImportFile(ImportCommand),

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

    /// Import sequences from a CSV dataset
    Sequences {
        /// The global identifier describing the dataset
        dataset_id: String,
        /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
        version: String,
        /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
        created_at: String,
        /// The path to the CSV file to import as operation logs
        path: PathBuf,
    },

    /// Import sources from a CSV dataset
    Sources { path: PathBuf },

    /// Import datasets from a CSV dataset
    Datasets { path: PathBuf },
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
        Commands::Import { path } => {
            let archive = archive::Archive::new(path.clone());
            archive.import()?;
        }
        Commands::ImportFile(cmd) => match cmd {
            ImportCommand::Taxa {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let taxa = Taxa {
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
                let taxa = TaxonomicActs {
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
                let acts = NomenclaturalActs {
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
                let collections = Collections {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                collections.import()?
            }

            ImportCommand::Sequences {
                dataset_id,
                version,
                created_at,
                path,
            } => {
                let dataset_version = create_dataset_version(dataset_id, version, created_at)?;
                let sequences = Sequences {
                    path: path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                sequences.import()?
            }

            ImportCommand::Sources { path } => {
                let sources = Sources { path: path.clone() };
                sources.import()?
            }

            ImportCommand::Datasets { path } => {
                let datasets = Datasets { path: path.clone() };
                datasets.import()?
            }
        },
        Commands::Reduce(cmd) => match cmd {
            ReduceCommand::Taxa => {
                let records = Taxa::reduce()?;
                let mut writer = csv::Writer::from_writer(std::io::stdout());
                for record in records {
                    writer.serialize(record)?;
                }
            }
            ReduceCommand::TaxonomicActs => {
                let records = TaxonomicActs::reduce()?;
                let mut writer = csv::Writer::from_writer(std::io::stdout());
                for record in records {
                    writer.serialize(record)?;
                }
            }
        },

        Commands::Update(cmd) => match cmd {
            UpdateCommand::Taxa => {
                Taxa::update()?;
                Taxa::link()?;
            }
            UpdateCommand::TaxonomicActs => TaxonomicActs::update()?,
            UpdateCommand::NomenclaturalActs => NomenclaturalActs::update()?,
        },
    }

    Ok(())
}

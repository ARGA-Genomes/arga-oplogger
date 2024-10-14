mod archive;
mod database;
mod errors;
mod frames;
mod loggers;
mod operations;
mod readers;
mod utils;

use std::path::PathBuf;

use clap::{Args, Parser};
use database::create_dataset_version;
use errors::Error;
use loggers::*;
use readers::plazi;

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

    /// Specific commands for the plazi treatment bank dataset
    #[command(subcommand)]
    Plazi(PlaziCommand),
}

#[derive(Args)]
pub struct DefaultImportArgs {
    /// The global identifier describing the dataset
    dataset_id: String,
    /// The version of this dataset. eg (v4, 20240102, abf839sfa0939faz204)
    version: String,
    /// The timestamp of when this dataset version was created. in yyyy-mm-dd hh:mm:ss format
    created_at: String,
    /// The path to the CSV file to import as operation logs
    path: PathBuf,
}

#[derive(clap::Subcommand)]
pub enum ImportCommand {
    /// Import taxa from a CSV dataset
    Taxa(DefaultImportArgs),

    /// Import taxonomic acts from a CSV dataset
    TaxonomicActs(DefaultImportArgs),

    /// Import nomenclatural acts from a CSV dataset
    NomenclaturalActs(DefaultImportArgs),

    /// Import collections from a CSV dataset
    Collections(DefaultImportArgs),

    /// Import sequences from a CSV dataset
    Sequences(DefaultImportArgs),
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
    /// Update publications with the reduced logs
    Publications,
}

#[derive(clap::Subcommand)]
pub enum PlaziCommand {
    /// Transform and import plazi treatment bank xml files
    Import(DefaultImportArgs),
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
            ImportCommand::Taxa(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let taxa = Taxa {
                    path: args.path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                taxa.import()?
            }

            ImportCommand::TaxonomicActs(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let taxa = TaxonomicActs {
                    path: args.path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                taxa.import()?
            }

            ImportCommand::NomenclaturalActs(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let acts = NomenclaturalActs {
                    path: args.path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                acts.import()?
            }

            ImportCommand::Collections(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let collections = Collections {
                    path: args.path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                collections.import()?
            }

            ImportCommand::Sequences(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let sequences = Sequences {
                    path: args.path.clone(),
                    dataset_version_id: dataset_version.id,
                };
                sequences.import()?
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
            UpdateCommand::Publications => publications::update()?,
        },

        Commands::Plazi(cmd) => match cmd {
            PlaziCommand::Import(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                plazi::document::import_all(args.path.clone(), dataset_version.id)?;
            }
        },
    }

    Ok(())
}

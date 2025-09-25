mod archive;
mod database;
mod errors;
mod frames;
mod loggers;
mod operations;
mod readers;
mod reducer;
mod transformer;
mod utils;

use std::path::PathBuf;

use clap::{Args, Parser};
use database::create_dataset_version;
use diesel::connection::set_default_instrumentation;
use errors::Error;
use loggers::*;
use nomenclatural_acts::NomenclaturalActs;
use readers::plazi;
use sequences::Sequences;
use taxonomic_acts::TaxonomicActs;
use tracing_subscriber::fmt::format::FmtSpan;

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

    /// Link records with the latest reduced data
    #[command(subcommand)]
    Link(LinkCommand),

    /// Specific commands for the plazi treatment bank dataset
    #[command(subcommand)]
    Plazi(PlaziCommand),

    /// Transform source CSV data into an importable archive
    Transform { path: PathBuf },
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

    /// Import sequences from a CSV dataset
    Sequences(DefaultImportArgs),

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
    /// Update publications with the reduced logs
    Publications,
    /// Update agents with the reduced logs
    Agents,
    /// Update organisms with the reduced logs
    Organisms,
    /// Update collections with the reduced logs
    Collections,
    /// Update accessions with the reduced logs
    Accessions,
    /// Update tissues with the reduced logs
    Tissues,
    /// Update subsamples with the reduced logs
    Subsamples,
    /// Update extractions with the reduced logs
    Extractions,
    /// Update extractions with the reduced logs
    Libraries,
}

#[derive(clap::Subcommand)]
pub enum LinkCommand {
    /// Link the taxa with the reduced logs
    Taxa,
}


#[derive(clap::Subcommand)]
pub enum PlaziCommand {
    /// Transform and import plazi treatment bank xml files
    Import(DefaultImportArgs),
}


fn main() -> Result<(), Error> {
    dotenvy::dotenv().ok();
    // tracing_subscriber::fmt::init();
    tracing_subscriber::fmt::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_target(false)
        .with_level(false)
        .init();

    set_default_instrumentation(database::simple_logger).expect("Failed to setup database instrumentation");

    let cli = Cli::parse();

    match &cli.command {
        Commands::Import { path } => {
            let archive = archive::Archive::new(path.clone());
            archive.import()?;
        }
        Commands::Transform { path } => {
            transformer::transform(path)?;
        }
        Commands::ImportFile(cmd) => match cmd {
            ImportCommand::Taxa(args) => {
                // let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                // let taxa = Taxa {
                //     path: args.path.clone(),
                //     dataset_version_id: dataset_version.id,
                // };
                // taxa.import()?
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

            ImportCommand::Sequences(args) => {
                let dataset_version = create_dataset_version(&args.dataset_id, &args.version, &args.created_at)?;
                let sequences = Sequences {
                    path: args.path.clone(),
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
                // let records = Taxa::reduce()?;
                // let mut writer = csv::Writer::from_writer(std::io::stdout());
                // for record in records {
                //     writer.serialize(record)?;
                // }
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
            UpdateCommand::Taxa => taxa::update()?,
            UpdateCommand::TaxonomicActs => taxonomic_acts::update()?,
            UpdateCommand::NomenclaturalActs => NomenclaturalActs::update()?,
            UpdateCommand::Publications => publications::update()?,
            UpdateCommand::Agents => agents::update()?,
            UpdateCommand::Organisms => organisms::update()?,
            UpdateCommand::Tissues => tissues::update()?,
            UpdateCommand::Collections => collections::update()?,
            UpdateCommand::Accessions => accessions::update()?,
            UpdateCommand::Subsamples => subsamples::update()?,
            UpdateCommand::Extractions => extractions::update()?,
            UpdateCommand::Libraries => libraries::update()?,
        },

        Commands::Link(cmd) => match cmd {
            LinkCommand::Taxa => taxa::link()?,
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

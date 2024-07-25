mod database;
mod errors;
mod operations;
mod taxa;

use std::path::PathBuf;

use clap::Parser;
use database::create_dataset_version;
use errors::Error;


pub static PROGRESS_TEMPLATE: &str = "[{elapsed_precise}] {bar:40.cyan/blue} {human_pos:>7}/{human_len:7} {msg}";
pub static SPINNER_TEMPLATE: &str = "[{elapsed_precise}] {spinner:2.cyan/blue} {msg}";


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
}

#[derive(clap::Subcommand)]
pub enum ReduceCommand {
    /// Reduce taxa logs into a taxonomy CSV
    Taxa,
}


fn main() -> Result<(), Error> {
    dotenvy::dotenv().ok();

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
        },
        Commands::Reduce(cmd) => match cmd {
            ReduceCommand::Taxa => taxa::Taxa::reduce()?,
        },
    }

    Ok(())
}

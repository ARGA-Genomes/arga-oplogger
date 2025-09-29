pub mod dataset;
pub mod errors;
pub mod models;
pub mod rdf;
pub mod resolver;


use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use dataset::Dataset;
use tracing::info;

use crate::errors::{Error, ParseError};
use crate::loggers::ProgressStream;
use crate::readers::meta::Meta;


pub fn transform(path: &PathBuf) -> Result<(), Error> {
    let meta = meta(path)?;

    info!(mapping = meta.dataset.schema, "Creating triples database (TriG)");
    let mut dataset = Dataset::new(&meta.dataset.schema)?;

    // load the mapping definitions
    dataset.load_trig_path("rdf/names.ttl")?;
    dataset.load_trig_path("rdf/specimens.ttl")?;
    dataset.load_trig_path("rdf/subsamples.ttl")?;
    dataset.load_trig_path("rdf/extractions.ttl")?;
    dataset.load_trig_path("rdf/sequences.ttl")?;
    dataset.load_trig_path("rdf/arga.ttl")?;

    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);

    for entry in archive.entries_with_seek()? {
        let entry = entry?;
        let header = entry.header();

        let size = header.size()?;
        let path = header.path()?;
        let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        if path.file_name().map(|p| p.to_str().unwrap_or_default()) == Some("meta.toml") {
            continue;
        }

        let message = format!("Loading {}", path.to_str().unwrap_or_default());
        let stream = ProgressStream::new(entry, size as usize, &message);

        dataset.load_csv(stream, &name)?;
    }

    info!("CSV files loaded into TriG dataset");
    export(dataset)
}


fn meta(path: &PathBuf) -> Result<Meta, Error> {
    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);
    let meta_filename = String::from("meta.toml");

    for entry in archive.entries_with_seek()? {
        let mut file = entry?;
        let path = file.header().path()?.to_str().unwrap_or_default().to_string();

        if path == meta_filename {
            let mut s = String::new();
            file.read_to_string(&mut s)?;
            let meta = toml::from_str(&s).map_err(|err| Error::Parsing(ParseError::Toml(err)))?;
            return Ok(meta);
        }
    }

    Err(Error::Parsing(ParseError::FileNotFound(meta_filename)))
}


fn export(dataset: Dataset) -> Result<(), Error> {
    info!("Exporting TriG dataset as importable CSV files");

    export_names(&dataset)?;
    export_agents(&dataset)?;
    export_publications(&dataset)?;

    export_organisms(&dataset)?;
    export_collections(&dataset)?;
    export_tissues(&dataset)?;
    export_subsamples(&dataset)?;
    export_extractions(&dataset)?;
    export_libraries(&dataset)?;
    export_sequencing_runs(&dataset)?;
    export_assemblies(&dataset)?;

    Ok(())
}


#[tracing::instrument(skip_all)]
fn export_names(dataset: &Dataset) -> Result<(), Error> {
    let names = models::name::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/names.csv")?;
    for name in names {
        writer.serialize(name)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_agents(dataset: &Dataset) -> Result<(), Error> {
    let agents = models::agent::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/agents.csv")?;
    for agent in agents {
        writer.serialize(agent)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_publications(dataset: &Dataset) -> Result<(), Error> {
    let publications = models::publications::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/publications.csv")?;
    for publication in publications {
        writer.serialize(publication)?;
    }

    Ok(())
}


#[tracing::instrument(skip_all)]
fn export_organisms(dataset: &Dataset) -> Result<(), Error> {
    let organisms = models::organism::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/organisms.csv")?;
    for organism in organisms {
        writer.serialize(organism)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_collections(dataset: &Dataset) -> Result<(), Error> {
    let collections = models::collecting::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/collections.csv")?;
    for collecting in collections {
        writer.serialize(collecting)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_tissues(dataset: &Dataset) -> Result<(), Error> {
    let tissues = models::tissue::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/tissues.csv")?;
    for tissue in tissues {
        writer.serialize(tissue)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_subsamples(dataset: &Dataset) -> Result<(), Error> {
    let subsamples = models::subsample::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/subsamples.csv")?;
    for subsample in subsamples {
        writer.serialize(subsample)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_extractions(dataset: &Dataset) -> Result<(), Error> {
    let extractions = models::extraction::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/extractions.csv")?;
    for extraction in extractions {
        writer.serialize(extraction)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_libraries(dataset: &Dataset) -> Result<(), Error> {
    let libraries = models::library::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/libraries.csv")?;
    for library in libraries {
        writer.serialize(library)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_sequencing_runs(dataset: &Dataset) -> Result<(), Error> {
    let sequences = models::sequencing_run::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/sequences.csv")?;
    for sequence in sequences {
        writer.serialize(sequence)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_assemblies(dataset: &Dataset) -> Result<(), Error> {
    let assemblies = models::assembly::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/assemblies.csv")?;
    for assembly in assemblies {
        writer.serialize(assembly)?;
    }

    Ok(())
}

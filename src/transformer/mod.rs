pub mod dataset;
pub mod errors;
pub mod models;
pub mod rdf;
pub mod resolver;


use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

use dataset::Dataset;
use tracing::info;

use crate::errors::{Error, ParseError};
use crate::loggers::ProgressStream;
use crate::readers::meta::Meta;


mod ttl {
    pub const NAMES: &[u8] = include_bytes!("../../rdf/names.ttl");
    pub const SPECIMENS: &[u8] = include_bytes!("../../rdf/specimens.ttl");
    pub const SUBSAMPLES: &[u8] = include_bytes!("../../rdf/subsamples.ttl");
    pub const EXTRACTIONS: &[u8] = include_bytes!("../../rdf/extractions.ttl");
    pub const SEQUENCES: &[u8] = include_bytes!("../../rdf/sequences.ttl");
    pub const DATA_PRODUCTS: &[u8] = include_bytes!("../../rdf/data_products.ttl");
    pub const ARGA: &[u8] = include_bytes!("../../rdf/arga.ttl");
}


pub fn transform(path: &PathBuf) -> Result<String, Error> {
    let meta = meta(path)?;

    info!(mapping = meta.dataset.schema, "Creating triples database (TriG)");
    let mut dataset = Dataset::new(
        &meta
            .dataset
            .schema
            .clone()
            .expect("schema must be set in meta.toml for transformations"),
    )?;

    // load the mapping definitions
    dataset.load_trig(BufReader::new(ttl::NAMES))?;
    dataset.load_trig(BufReader::new(ttl::SPECIMENS))?;
    dataset.load_trig(BufReader::new(ttl::SUBSAMPLES))?;
    dataset.load_trig(BufReader::new(ttl::EXTRACTIONS))?;
    dataset.load_trig(BufReader::new(ttl::SEQUENCES))?;
    dataset.load_trig(BufReader::new(ttl::DATA_PRODUCTS))?;
    dataset.load_trig(BufReader::new(ttl::ARGA))?;

    // dataset.load_trig_path("rdf/names.ttl")?;
    // dataset.load_trig_path("rdf/specimens.ttl")?;
    // dataset.load_trig_path("rdf/subsamples.ttl")?;
    // dataset.load_trig_path("rdf/extractions.ttl")?;
    // dataset.load_trig_path("rdf/sequences.ttl")?;
    // dataset.load_trig_path("rdf/data_products.ttl")?;
    // dataset.load_trig_path("rdf/arga.ttl")?;

    let file = File::open(path)?;
    let mut archive = tar::Archive::new(file);

    for entry in archive.entries_with_seek()? {
        let entry = entry?;
        let header = entry.header().clone();

        let size = header.size()?;
        let path = header.path()?;

        let filename = path.file_name().map(|p| p.to_str().unwrap_or_default());
        let ext = path.extension().unwrap_or_default().to_string_lossy();
        let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        if filename == Some("meta.toml") || ext == "etag" {
            continue;
        }

        let path = path.to_str();
        info!(path, name, "Loading csv file");

        let message = format!("Loading {}", path.unwrap_or_default());
        // let input = flate2::read::GzDecoder::new(entry);
        let input = brotli::Decompressor::new(entry, 4096);
        let stream = ProgressStream::new(input, size as usize, &message);

        // dataset.load_csv_oxi(stream, &name)?;
        let rows = dataset.load_csv(stream, &name)?;
        info!(path, name, rows, "CSV loaded");
    }

    info!("CSV files loaded into TriG dataset");
    export(dataset, meta)
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


fn export(dataset: Dataset, meta: Meta) -> Result<String, Error> {
    info!("Exporting TriG dataset as importable CSV files");

    // export_names(&dataset)?;
    // export_agents(&dataset)?;
    // export_publications(&dataset)?;

    // export_organisms(&dataset)?;
    // export_collections(&dataset)?;
    // export_tissues(&dataset)?;
    // export_subsamples(&dataset)?;
    // export_extractions(&dataset)?;
    // export_libraries(&dataset)?;
    // export_sequencing_runs(&dataset)?;
    export_assemblies(&dataset)?;
    // export_data_products(&dataset)?;

    package(meta)
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

    let file = File::create("out/assemblies.csv.br")?;
    let out = brotli::CompressorWriter::new(file, 8092, 7, 22);
    let mut writer = csv::Writer::from_writer(out);

    // let mut writer = csv::Writer::from_path("out/assemblies.csv")?;
    for assembly in assemblies {
        writer.serialize(assembly)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
fn export_data_products(dataset: &Dataset) -> Result<(), Error> {
    let data_products = models::data_products::get_all(&dataset)?;

    let mut writer = csv::Writer::from_path("out/data_products.csv")?;
    for product in data_products {
        writer.serialize(product)?;
    }

    Ok(())
}


pub fn package(meta: Meta) -> Result<String, Error> {
    let filename = format!("{}-{}.tar", meta.dataset.name, meta.dataset.published_at.to_string());
    info!(?filename, "Packaging export");

    // create the toml file for the package metadata
    let mut file = File::create("out/meta.toml")?;
    let toml = toml::to_string_pretty(&meta).unwrap();
    file.write_all(toml.as_bytes())?;

    // create a tar archive containing everything the package needs
    let file = File::create(&filename)?;
    let mut archive = tar::Builder::new(file);

    archive.append_path_with_name("out/meta.toml", "meta.toml")?;
    archive.append_path_with_name("out/assemblies.csv.br", "assemblies.csv.br")?;

    archive.finish()?;
    Ok(filename)
}

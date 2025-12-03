pub mod dataset;
pub mod errors;
pub mod models;
pub mod rdf;
pub mod resolver;


use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

use dataset::Dataset;
use serde::Serialize;
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
    pub const ANNOTATIONS: &[u8] = include_bytes!("../../rdf/annotations.ttl");
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
    dataset.load_trig(BufReader::new(ttl::ANNOTATIONS))?;
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

        if filename == Some("meta.toml") || ext == "etag" || ext == "last_modified" {
            continue;
        }

        let path = path.to_str();
        info!(path, name, "Loading csv file");

        let message = format!("Loading {}", path.unwrap_or_default());
        // let input = flate2::read::GzDecoder::new(entry);
        let input = brotli::Decompressor::new(entry, 4096);
        let stream = ProgressStream::new(input, size as usize, &message);

        // dataset.load_csv_oxi(stream, &name)?;
        // let rows = dataset.load_csv(stream, &name)?;
        let rows = dataset.load_jsonl(stream, &name)?;
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

    export_compressed(models::name::get_all(&dataset)?, "out/names.csv.br")?;
    export_compressed(models::agent::get_all(&dataset)?, "out/agents.csv.br")?;
    export_compressed(models::publications::get_all(&dataset)?, "out/publications.csv.br")?;

    export_compressed(models::organism::get_all(&dataset)?, "out/organisms.csv.br")?;
    export_compressed(models::collecting::get_all(&dataset)?, "out/collections.csv.br")?;
    export_compressed(models::tissue::get_all(&dataset)?, "out/tissues.csv.br")?;
    export_compressed(models::subsample::get_all(&dataset)?, "out/subsamples.csv.br")?;
    export_compressed(models::extraction::get_all(&dataset)?, "out/extractions.csv.br")?;
    export_compressed(models::library::get_all(&dataset)?, "out/libraries.csv.br")?;
    export_compressed(models::sequencing_run::get_all(&dataset)?, "out/sequencing_runs.csv.br")?;
    export_compressed(models::assembly::get_all(&dataset)?, "out/assemblies.csv.br")?;
    export_compressed(models::data_products::get_all(&dataset)?, "out/data_products.csv.br")?;
    export_compressed(models::annotation::get_all(&dataset)?, "out/annotations.csv.br")?;

    package(meta)
}


#[tracing::instrument(skip_all)]
fn export_compressed<T: Serialize>(records: Vec<T>, outfile: &str) -> Result<(), Error> {
    if records.len() > 0 {
        let file = File::create(outfile)?;
        let out = brotli::CompressorWriter::new(file, 8092, 7, 22);
        let mut writer = csv::Writer::from_writer(out);

        for record in records {
            writer.serialize(record)?;
        }
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
    append_if_exists(&mut archive, "names.csv.br")?;
    append_if_exists(&mut archive, "agents.csv.br")?;
    append_if_exists(&mut archive, "publications.csv.br")?;
    append_if_exists(&mut archive, "organisms.csv.br")?;
    append_if_exists(&mut archive, "collections.csv.br")?;
    append_if_exists(&mut archive, "tissues.csv.br")?;
    append_if_exists(&mut archive, "subsamples.csv.br")?;
    append_if_exists(&mut archive, "extractions.csv.br")?;
    append_if_exists(&mut archive, "libraries.csv.br")?;
    append_if_exists(&mut archive, "sequencing_runs.csv.br")?;
    append_if_exists(&mut archive, "assemblies.csv.br")?;
    append_if_exists(&mut archive, "data_products.csv.br")?;
    append_if_exists(&mut archive, "annotations.csv.br")?;

    archive.finish()?;
    Ok(filename)
}

fn append_if_exists(archive: &mut tar::Builder<File>, filename: &str) -> Result<(), Error> {
    let path = format!("out/{filename}");

    if std::path::Path::new(&path).exists() {
        archive.append_path_with_name(path, filename)?;
    }

    Ok(())
}

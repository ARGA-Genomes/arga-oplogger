use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::path::Path;
use std::str::FromStr;

use arga_core::schema;
use chrono::NaiveDateTime;
use diesel::sql_types::{Nullable, Varchar};
use diesel::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;
use ureq::Agent;

use super::errors::ExtractError;
use crate::database;
use crate::errors::Error;
use crate::readers::meta::{Attribution, Changelog, Collection, Dataset, Meta};


#[derive(Debug, Serialize, Deserialize)]
struct Record {
    #[serde(alias = "Scientific_name_processed")]
    scientific_name: Option<String>,

    #[serde(alias = "Canonical_name_verbatim")]
    canonical_name: String,

    #[serde(alias = "Assembly_name_verbatim")]
    assembly_name: String,

    #[serde(alias = "Assembly_method")]
    assembly_method: String,

    #[serde(alias = "DNA Zoo JSON")]
    metadata_url: String,

    #[serde(alias = "DNA Zoo Species folder path")]
    source_folder_url: String,

    #[serde(alias = "Source_URL")]
    source_url: String,

    #[serde(alias = "Citations")]
    citations: String,

    #[serde(alias = "recordCurator")]
    curator: String,

    #[serde(alias = "recordCuratorID")]
    curator_orcid: String,

    #[serde(alias = "recordCreated")]
    created_at: String,

    #[serde(alias = "recordModified")]
    updated_at: String,
}


pub fn extract() -> Result<Option<String>, ExtractError> {
    let mut reader = csv::Reader::from_path("dnazoo.csv").unwrap();
    let records = reader.deserialize::<Record>();

    let agent: Agent = Agent::new_with_defaults();

    let file = File::create("metadata.jsonl.br")?;
    let mut writer = brotli::CompressorWriter::new(file, 4096, 7, 22);

    for record in records {
        let record = record.unwrap();
        info!(name = record.assembly_name, "Getting metadata");

        let mut metadata = agent
            .get(&record.metadata_url)
            .call()?
            .body_mut()
            .read_json::<serde_json::Map<String, serde_json::Value>>()?;

        metadata.insert("arga_curation".to_string(), serde_json::to_value(&record)?);

        let serialized = serde_json::to_string(&metadata)?;
        writer.write(serialized.as_bytes())?;
        writer.write(&[b'\n'])?;
    }

    writer.flush()?;
    writer.into_inner().sync_all()?;

    info!("Download finished");

    let metadata_modified = NaiveDateTime::parse_from_str("2025-12-12 00:00:00", "%Y-%m-%d %H:%M:%S")?;
    let meta = meta(&metadata_modified, &metadata_modified)?;
    let filename = package(meta)?;

    Ok(Some(filename))
}


pub fn package(meta: Meta) -> Result<String, ExtractError> {
    let filename = format!("dnazoo-{}.tar", meta.dataset.published_at.to_string());
    info!(?filename, "Packaging extract");

    // create the toml file for the package metadata
    let mut file = File::create("meta.toml")?;
    let toml = toml::to_string_pretty(&meta)?;
    file.write_all(toml.as_bytes())?;

    // create a tar archive containing everything the package needs
    let file = File::create(&filename)?;
    let mut archive = tar::Builder::new(file);

    archive.append_path("meta.toml")?;
    archive.append_path("metadata.jsonl.br")?;

    archive.finish()?;
    Ok(filename)
}


pub fn meta(version: &NaiveDateTime, published_at: &NaiveDateTime) -> Result<Meta, ExtractError> {
    // convert the metadata_modified datetime into a toml datetime
    let published_at = toml::value::Datetime::from_str(&published_at.and_utc().to_rfc3339())?;

    let dataset = Dataset {
        id: "".into(),
        name: "DNA Zoo".into(),
        short_name: "DNA Zoo".into(),
        version: version.to_string(),
        published_at,
        url: "https://www.dnazoo.org/".into(),
        schema: Some("http://arga.org.au/schemas/maps/dnazoo/".into()),
    };

    let changelog = Changelog { notes: vec![] };

    let attribution = Attribution {
        citation: "DNA Zoo Consortium, Aiden Lab, 2018-2025".into(),
        source_url: "https://www.dnazoo.org/".into(),
        license: "https://www.dnazoo.org/usage".into(),
        rights_holder: "DNA Zoo Consortium".into(),
    };

    let collection = Collection {
        name: "ARGA Genomes".into(),
        author: "ARGA Team".into(),
        license: "https://creativecommons.org/licenses/by/4.0/".into(),
        access_rights: "https://arga.org.au/user-guide#data-usage".into(),
        rights_holder: "Australian Reference Genome Atlas (ARGA) Project for the Atlas of Living Australia and Bioplatforms Australia".into(),
    };

    Ok(Meta {
        dataset,
        changelog,
        attribution,
        collection,
    })
}

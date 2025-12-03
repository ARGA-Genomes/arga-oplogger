use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::path::Path;
use std::str::FromStr;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info};
use ureq::{Agent, http};

use super::errors::ExtractError;
use crate::readers::meta::{Attribution, Changelog, Collection, Dataset, Meta};


const API_URL: &'static str = "https://data.bioplatforms.com/api/3/action";
const TIMESTAMP_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.f";


#[derive(Debug, Deserialize)]
struct PackageList {
    success: bool,
    result: Vec<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct Organisation {
    id: String,
    name: String,
    title: String,
    description: String,
    image_url: String,
}


fn ckan_action(action: &str) -> String {
    return format!("{API_URL}/{action}");
}


pub fn extract() -> Result<Option<String>, ExtractError> {
    let agent: Agent = Agent::new_with_defaults();

    let last_modified = last_modified()?;

    let url = ckan_action("current_package_list_with_resources");
    info!(url, "Requesting packages with resources");

    // there are no headers we can use to determine if the last request is different
    // so we instead read pages until we reach the last modified record
    let list = agent
        .get(&url)
        .query("limit", "5")
        .call()?
        .body_mut()
        .read_json::<PackageList>()?;

    if list.success == false {
        error!("Request failed");
        return Err(ExtractError::RequestFailed);
    }

    let newest_package = list.result.first().ok_or(ExtractError::UnknownResponse)?;
    let metadata_modified = newest_package
        .get("metadata_modified")
        .ok_or(ExtractError::UnknownResponse)?;


    // the modified date is not timezone aware so we have to assume that it's all
    // based in a specific location. the potential issue being that a record change
    // could be backdated if made in a different timezone
    let metadata_modified = parse_datetime(&metadata_modified)?;

    // create meta file from response
    let meta = meta(&metadata_modified, &metadata_modified)?;
    let mut offset = 0;
    let limit = 10_000;

    let filename = if last_modified.is_none() || last_modified != Some(metadata_modified) {
        info!(?last_modified, ?metadata_modified, "File changed, downloading.");

        let file = File::create(format!("bpa.jsonl.br"))?;
        let mut writer = brotli::CompressorWriter::new(file, 4096, 7, 22);

        // get all the records in batches since these are API calls
        loop {
            info!(offset, limit, "Batch requested");

            let list = agent
                .get(&url)
                .query("limit", limit.to_string())
                .query("offset", offset.to_string())
                .call()?
                .body_mut()
                .read_json::<PackageList>()?;

            // either an error occurred or all packages have been retrieved
            if list.result.is_empty() || !list.success {
                break;
            }

            let size = list.result.len();
            info!(size, "Batch downloaded");

            // write the package as a single line in a json array
            for package in list.result {
                let organisation: Option<Organisation> = match package.get("organization") {
                    Some(value) => Some(serde_json::from_value(value.clone())?),
                    _ => None,
                };

                if let Some(org) = organisation {
                    if org.name != "threatened-species" {
                        continue;
                    }
                }
                else {
                    continue;
                }


                let serialized = serde_json::to_string(&package)?;
                writer.write(serialized.as_bytes())?;
                writer.write(&[b'\n'])?;
            }

            offset = offset + size;
        }

        writer.flush()?;
        writer.into_inner().sync_all()?;

        // update the last_modified file for future checks
        let modified_file = format!("bpa.last_modified");
        let mut file = File::create(modified_file)?;
        file.write_all(metadata_modified.format(TIMESTAMP_FORMAT).to_string().as_bytes())?;

        info!("Download finished");

        Some(package(meta)?)
    }
    else {
        info!(?last_modified, ?metadata_modified, "File unchanged, skipping.");
        None
    };

    Ok(filename)
}


fn parse_datetime(value: &serde_json::Value) -> Result<NaiveDateTime, ExtractError> {
    match value {
        serde_json::Value::String(val) => Ok(NaiveDateTime::parse_from_str(val, TIMESTAMP_FORMAT)?),
        _ => Err(ExtractError::UnknownResponse),
    }
}


pub fn last_modified() -> Result<Option<NaiveDateTime>, ExtractError> {
    let modified_file = format!("bpa.last_modified");
    let path = Path::new(&modified_file);
    if !path.exists() {
        return Ok(None);
    }

    let mut file = File::open(modified_file)?;
    let mut modified = String::new();
    file.read_to_string(&mut modified)?;

    let modified = NaiveDateTime::parse_from_str(modified.trim(), TIMESTAMP_FORMAT)?;
    Ok(Some(modified))
}


pub fn package(meta: Meta) -> Result<String, ExtractError> {
    let filename = format!("bpa-{}.tar", meta.dataset.published_at.to_string());
    info!(?filename, "Packaging extract");

    // create the toml file for the package metadata
    let mut file = File::create("meta.toml")?;
    let toml = toml::to_string_pretty(&meta)?;
    file.write_all(toml.as_bytes())?;

    // create a tar archive containing everything the package needs
    let file = File::create(&filename)?;
    let mut archive = tar::Builder::new(file);

    archive.append_path("meta.toml")?;
    archive.append_path("bpa.jsonl.br")?;
    archive.append_path("bpa.last_modified")?;

    archive.finish()?;
    Ok(filename)
}


pub fn meta(version: &NaiveDateTime, published_at: &NaiveDateTime) -> Result<Meta, ExtractError> {
    // convert the metadata_modified datetime into a toml datetime
    let published_at = toml::value::Datetime::from_str(&published_at.and_utc().to_rfc3339())?;

    let dataset = Dataset {
        id: "".into(),
        name: "Bioplatforms Australia Data Portal".into(),
        short_name: "BPA".into(),
        version: version.to_string(),
        published_at,
        url: "https://data.bioplatforms.com/".into(),
        schema: Some("http://arga.org.au/schemas/maps/bpa/".into()),
    };

    let changelog = Changelog { notes: vec![] };

    let attribution = Attribution {
        citation: "".into(),
        source_url: "".into(),
        license: "".into(),
        rights_holder: "".into(),
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

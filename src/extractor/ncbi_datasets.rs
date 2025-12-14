use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::path::Path;

use arga_core::schema;
use diesel::sql_types::{Nullable, Varchar};
use diesel::*;
use serde::Serialize;
use serde_json::json;
use tracing::info;
use ureq::Agent;

use super::errors::ExtractError;
use crate::database;
use crate::errors::Error;


const ASSEMBLY_REPORT_URL: &'static str = "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/dataset_report";
const ASSEMBLY_REPORT_FILE: &'static str = "assembly_reports.jsonl";


pub fn extract() -> Result<Option<String>, ExtractError> {
    let agent: Agent = Agent::config_builder().http_status_as_error(false).build().into();
    // let agent: Agent = Agent::new_with_defaults();

    info!("Getting changed accessions since last extraction");
    let accessions = get_changed_assemblies().expect("failed to get changed assemblies");

    info!(new_assemblies = accessions.len(), url = ASSEMBLY_REPORT_URL, "Requesting assembly reports");

    // the network might fail during a long extraction so instead we
    // try to resume from the last chunk if it is available
    let resume_offset = resume()?;

    let filename = format!("{ASSEMBLY_REPORT_FILE}");
    let file = match resume_offset {
        Some(offset) => {
            info!(offset, "Resuming extraction");
            File::options().append(true).open(filename)?
        }
        None => File::create(filename)?,
    };

    let mut writer = std::io::BufWriter::new(file);
    // let mut writer = brotli::CompressorWriter::new(file, 4096, 7, 22);
    let mut offset = 0;

    // this could potentially be a large amount so we chunk up the requests
    for chunk in accessions.chunks(10_000) {
        offset += chunk.len();

        // skip to the resumption offset
        if let Some(resume) = resume_offset {
            if offset < resume {
                continue;
            }
        }

        info!(offset, "Getting chunk");
        let mut next_page_token = None;

        // get the next page if the api has a max page size limit
        loop {
            let body = json!({ "accessions": chunk, "page_size": 10_000, "page_token": next_page_token });

            let mut response = agent
                .post(ASSEMBLY_REPORT_URL)
                .header("accept", "application/x-ndjson")
                .header("api-token", std::env::var("NCBI_DATASETS_API_TOKEN").unwrap_or_default())
                .send_json(&body)?;

            let body = response.body_mut().as_reader();
            let mut reader = BufReader::new(body);

            // read all the contents and compress it while writing to a file
            let mut read_buf = [0; 8092];
            loop {
                let result = reader.read(&mut read_buf[..]);

                // retry reading if interrupted as it is non-fatal
                if result.as_ref().is_err_and(|err| err.kind() == ErrorKind::Interrupted) {
                    continue;
                }

                let bytes_read = result?;

                // EOF
                if bytes_read == 0 {
                    break;
                }

                writer.write(&read_buf[..bytes_read])?;
            }

            // sleep for a second to refil the api request limit bucket
            if let Some(remaining) = response.headers().get("x-ratelimit-remaining") {
                if remaining.to_str().unwrap_or("0") == "0" {
                    info!("rate limit hit, slowing down");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }

            if let Some(next_page) = response.headers().get("x-ncbi-next-page-token") {
                next_page_token = next_page.to_str().map(|s| s.to_string()).ok();
            }
            else {
                break;
            }
        }

        writer.flush()?;

        save_resume(offset)?;
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    // writer.into_inner().sync_all()?;

    info!("Download finished");

    Ok(None)
}


fn resume() -> Result<Option<usize>, ExtractError> {
    let resume_file = format!("{ASSEMBLY_REPORT_FILE}.resume");
    let path = Path::new(&resume_file);
    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(resume_file)?;
    let mut buf = BufReader::new(&file);
    let mut offset = String::new();
    buf.read_line(&mut offset)?;

    Ok(Some(offset.trim().parse::<usize>().unwrap()))
}


fn save_resume(offset: usize) -> Result<(), ExtractError> {
    let resume_file = format!("{ASSEMBLY_REPORT_FILE}.resume");
    let mut file = File::create(resume_file)?;
    file.write(offset.to_string().as_bytes());
    Ok(())
}


fn get_changed_assemblies() -> Result<Vec<String>, Error> {
    use diesel::dsl::sql;
    use schema::{assembly_logs, dataset_versions};

    let pool = database::get_pool()?;
    let mut conn = pool.get()?;

    // do a dumb change check by looking for any AssemblyId atom in the op logs
    // after a specific date. because the AssemblyId is persistent for ncbi we
    // use it as a way to only get new accessions added to the database since
    // the last time we checked
    let accessions = assembly_logs::table
        .inner_join(dataset_versions::table)
        .select(sql::<Nullable<Varchar>>("atom->>'AssemblyId'"))
        .filter(assembly_logs::atom.has_key("AssemblyId"))
        .order_by(assembly_logs::operation_id)
        .load::<Option<String>>(&mut conn)?;

    let mut accessions: Vec<String> = accessions.into_iter().filter_map(|i| i).collect();
    accessions.sort();
    accessions.dedup();

    Ok(accessions)
}

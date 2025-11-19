use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write, copy};
use std::path::Path;
use std::str::FromStr;

use flate2::bufread::GzDecoder;
use taxonomy::Taxonomy;
use tracing::{error, info};
use ureq::{Agent, http};

use super::errors::ExtractError;
use crate::readers::meta::{Attribution, Changelog, Collection, Dataset, Meta};


const TAXDUMP_URL: &'static str = "https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz";
const TAXDUMP_FILE: &'static str = "taxdump.tar.gz";


pub fn extract() -> Result<Option<String>, ExtractError> {
    let agent: Agent = Agent::new_with_defaults();
    let last_etag = etag()?;

    let request = http::Request::get(TAXDUMP_URL)
        // .header("If-None-Match", last_etag.clone().unwrap_or_default())
        .body(())
        .unwrap();

    info!(url = TAXDUMP_URL, "Requesting summary file");

    let mut response = agent.run(request).unwrap();
    let etag = response.headers().get("etag").and_then(|h| h.to_str().ok());
    let last_modified = response.headers().get("last-modified").and_then(|h| h.to_str().ok());

    if !response.status().is_success() {
        error!(status = response.status().canonical_reason(), "Request failed");
        return Err(ExtractError::RequestFailed);
    }

    // create meta file from response headers
    let meta = meta(etag.unwrap_or_default(), last_modified.unwrap_or_default())?;
    let etag = etag.map(|s| s.to_string());


    // only download the file if the etag from the response is different to
    // the last saved etag. this allows us to only download the file if it
    // has actually changed, allowing us to also run the check more frequently
    let filename = if last_etag != etag {
        info!(last_etag, etag, last_modified, "File changed, downloading.");

        let file = File::create(TAXDUMP_FILE)?;
        let mut writer = BufWriter::new(file);

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

        writer.flush()?;

        // update the etag file for future checks
        let etag_file = format!("{TAXDUMP_FILE}.etag");
        let mut file = File::create(etag_file)?;
        file.write_all(etag.unwrap_or_default().as_bytes())?;

        info!("Download finished");

        unpack()?;
        process()?;
        Some(package(meta)?)
    }
    else {
        info!(last_etag, etag, last_modified, "File unchanged, skipping.");
        None
    };

    Ok(filename)
}


pub fn process() -> Result<(), ExtractError> {
    let taxonomy = taxonomy::ncbi::load("taxdump").unwrap();
    let root: usize = taxonomy.root();

    let file = File::create("ncbi_taxonomy.csv.br")?;
    let output = brotli::CompressorWriter::new(file, 8092, 7, 22);
    let mut writer = csv::WriterBuilder::new().from_writer(output);

    writer.write_record(&["taxon_id", "canonical_name", "rank"]).unwrap();

    for node in taxonomy.descendants(root).unwrap() {
        writer
            .write_record(&[
                &node.to_string(),
                taxonomy.name(node).unwrap(),
                taxonomy.rank(node).unwrap().to_ncbi_rank(),
            ])
            .unwrap();
    }

    writer.flush().unwrap();
    Ok(())
}


pub fn unpack() -> Result<(), ExtractError> {
    let file = File::open(TAXDUMP_FILE)?;
    let input = BufReader::new(file);
    let mut output = File::create("taxdump.tar")?;

    let input_length = input.get_ref().metadata()?.len();

    let mut decoder = GzDecoder::new(input);
    copy(&mut decoder, &mut output)?;

    let mut archive = tar::Archive::new(File::open("taxdump.tar")?);
    archive.unpack("taxdump")?;

    info!(input_length, output_length = output.metadata()?.len(), file = TAXDUMP_FILE, "Deflated file");
    Ok(())
}


pub fn package(meta: Meta) -> Result<String, ExtractError> {
    let filename = format!("ncbi-taxonomy-{}.tar", meta.dataset.published_at.to_string());
    info!(?filename, "Packaging extract");

    // create the toml file for the package metadata
    let mut file = File::create("meta.toml")?;
    let toml = toml::to_string_pretty(&meta)?;
    file.write_all(toml.as_bytes())?;

    // create a tar archive containing everything the package needs
    let file = File::create(&filename)?;
    let mut archive = tar::Builder::new(file);

    archive.append_path("meta.toml")?;
    archive.append_path("ncbi_taxonomy.csv.br")?;
    archive.append_path("taxdump.tar.gz.etag")?;

    archive.finish()?;
    Ok(filename)
}


pub fn etag() -> Result<Option<String>, ExtractError> {
    let etag_file = format!("{TAXDUMP_FILE}.etag");
    let path = Path::new(&etag_file);
    if !path.exists() {
        return Ok(None);
    }

    let mut file = File::open(etag_file)?;
    let mut etag = String::new();
    file.read_to_string(&mut etag)?;
    let etag = etag.trim().to_string();

    if etag.is_empty() { Ok(None) } else { Ok(Some(etag)) }
}


pub fn meta(version: &str, published_at: &str) -> Result<Meta, ExtractError> {
    // parse and convert the http last-modified-at datetime into a toml datetime
    let published_at = chrono::DateTime::parse_from_rfc2822(published_at)?;
    let published_at = toml::value::Datetime::from_str(&published_at.to_rfc3339())?;

    let dataset = Dataset {
        id: "".into(),
        name: "NCBI: Taxonomy".into(),
        short_name: "NCBI: Taxa".into(),
        version: version.into(),
        published_at,
        url: "https://www.ncbi.nlm.nih.gov/genbank/".into(),
        schema: Some("http://arga.org.au/schemas/maps/ncbi/".into()),
    };

    let changelog = Changelog { notes: vec![] };

    let attribution = Attribution {
        citation: "Eric W Sayers, Mark Cavanaugh, Karen Clark, Kim D Pruitt, Stephen T Sherry, Linda Yankie, Ilene Karsch-Mizrachi, GenBank 2024 Update, Nucleic Acids Research, Volume 52, Issue D1, 5 January 2024, Pages D134–D137, https://doi.org/10.1093/nar/gkad903".into(),
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

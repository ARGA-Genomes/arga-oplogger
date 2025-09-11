use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use tracing::info;

use crate::errors::{Error, ParseError};
use crate::readers::meta::Meta;
use crate::{ProgressStream, loggers, upsert_meta};


#[derive(Debug)]
pub enum ImportType {
    Unknown,
    Names,
    Taxa,
    Publications,
    TaxonomicActs,
    NomenclaturalActs,

    Organisms,
    Tissues,
    Collections,
    Accessions,
    Subsamples,
    Sequences,

    AdminMedia,
}

impl From<String> for ImportType {
    fn from(value: String) -> Self {
        use ImportType::*;

        match value.as_str() {
            "names.csv.br" => Names,
            "taxa.csv.br" => Taxa,
            "publications.csv.br" => Publications,
            "taxonomic_acts.csv.br" => TaxonomicActs,
            "nomenclatural_acts.csv.br" => NomenclaturalActs,

            "organisms.csv.br" => Organisms,
            "tissues.csv.br" => Tissues,
            "collections.csv.br" => Collections,
            "accessions.csv.br" => Accessions,
            "subsamples.csv.br" => Subsamples,
            "sequences.csv.br" => Sequences,

            "admin_media.csv.br" => AdminMedia,
            _ => Unknown,
        }
    }
}


pub struct Archive {
    path: PathBuf,
}

impl Archive {
    pub fn new(path: PathBuf) -> Archive {
        Archive { path }
    }

    pub fn meta(&self) -> Result<Meta, Error> {
        let file = File::open(&self.path)?;
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

    pub fn import(&self) -> Result<(), Error> {
        let meta = self.meta()?;
        info!(name = meta.dataset.short_name, version = meta.dataset.version, "Upserting dataset");
        upsert_meta(meta.clone())?;

        let file = File::open(&self.path)?;
        let mut archive = tar::Archive::new(file);

        for entry in archive.entries_with_seek()? {
            let entry = entry?;
            let path = entry.header().path()?.to_str().unwrap_or_default().to_string();
            let size = entry.header().size()?;
            let import_type = ImportType::from(path.clone());

            let message = format!("Atomising {path} ({import_type:?})");
            let stream = ProgressStream::new(entry, size as usize, &message);

            match import_type {
                ImportType::Unknown => {}
                ImportType::Names => loggers::names::import_archive(stream)?,
                ImportType::Taxa => loggers::taxa::import(stream, &meta.dataset)?,
                ImportType::Publications => loggers::publications::import_archive(stream, &meta.dataset)?,
                ImportType::TaxonomicActs => loggers::taxonomic_acts::import(stream, &meta.dataset)?,
                ImportType::NomenclaturalActs => loggers::nomenclatural_acts::import_archive(stream, &meta.dataset)?,
                ImportType::Organisms => loggers::organisms::import_archive(stream, &meta.dataset)?,
                ImportType::Tissues => loggers::tissues::import_archive(stream, &meta.dataset)?,
                ImportType::Collections => loggers::collections::import_archive(stream, &meta.dataset)?,
                ImportType::Accessions => loggers::accessions::import_archive(stream, &meta.dataset)?,
                ImportType::Subsamples => loggers::subsamples::import_archive(stream, &meta.dataset)?,
                ImportType::Sequences => todo!(),

                ImportType::AdminMedia => loggers::admin_media::import_archive(stream, &meta.dataset)?,
            }
        }

        Ok(())
    }
}

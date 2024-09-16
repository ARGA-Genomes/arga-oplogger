use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use tracing::info;

use crate::errors::{Error, ParseError};
use crate::readers::meta::Meta;
use crate::{loggers, upsert_meta, ProgressStream};


#[derive(Debug)]
pub enum ImportType {
    Unknown,
    Taxa,
    TaxonomicActs,
    NomenclaturalActs,
    Collections,
    Sequences,
}

impl From<String> for ImportType {
    fn from(value: String) -> Self {
        use ImportType::*;

        match value.as_str() {
            "taxa.csv.br" => Taxa,
            "taxonomic_acts.csv.br" => TaxonomicActs,
            "nomenclatural_acts.csv.br" => NomenclaturalActs,
            "collections.csv.br" => Collections,
            "sequences.csv.br" => Sequences,
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

            info!(path, size, ?import_type);
            let stream = ProgressStream::new(entry, size as usize);

            match import_type {
                ImportType::Unknown => info!("Unknown type, skipping"),
                ImportType::Taxa => loggers::taxa::import(stream, &meta.dataset)?,
                ImportType::TaxonomicActs => todo!(),
                ImportType::NomenclaturalActs => todo!(),
                ImportType::Collections => todo!(),
                ImportType::Sequences => todo!(),
            }
        }

        Ok(())
    }
}

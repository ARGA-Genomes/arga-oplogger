use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use arga_core::crdt::{DataFrame, Version};
use arga_core::models::NomenclaturalActType;
use chrono::DateTime;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use tracing::info;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use super::parsing::{end_eq, parse_attribute, parse_attribute_opt, start_eq, ParseSection};
use super::sections::prelude::*;
use super::sections::treatment::Treatment;
use crate::errors::{Error, ParseError};
use crate::frames::{FrameReader, IntoFrame};
use crate::utils::FrameImportBars;
use crate::{nomenclatural_acts, publications, FrameProgress};


pub fn import_all(input_dir: PathBuf, dataset_version: Uuid) -> Result<(), Error> {
    info!("Enumerating files in '{input_dir:?}'");
    let files = xml_files(input_dir)?;

    let pool = crate::database::get_pool()?;

    for (idx, file) in files.iter().enumerate() {
        info!("Reading file {idx}: {file:?}");

        // TODO:
        // this is not very efficient at all as it results in reading and parsing the file twice
        // for both types of records. but since the importer logic and utilities are still in flux
        // it will do for now. ideally we can move away from an abstraction of frames and move
        // to a 'record + IntoFrame' to enable better chunking at the reader as well as at the
        // importer stages.
        {
            let fh = File::open(file)?;
            let reader = BufReader::new(fh);
            let document = DocumentReader::<publications::Record, _>::from_reader(reader, dataset_version)?;
            publications::import(document, pool.clone())?;
        }
        {
            let fh = File::open(file)?;
            let reader = BufReader::new(fh);
            let document = DocumentReader::<nomenclatural_acts::Record, _>::from_reader(reader, dataset_version)?;
            nomenclatural_acts::import(document, pool.clone())?;
        }
    }

    info!("Importing {} XML files", files.len());
    Ok(())
}

fn xml_files(base_dir: PathBuf) -> Result<Vec<PathBuf>, Error> {
    let mut files = vec![];

    // walk the base directory by recursively calling this function
    for entry in std::fs::read_dir(base_dir)? {
        let path = entry?.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "xml" {
                    files.push(path);
                }
            }
        }
        else if path.is_dir() {
            files.extend(xml_files(path)?);
        }
    }

    Ok(files)
}


#[derive(Debug, Clone)]
pub struct DocumentHeader {
    pub entity_id: String,
    pub id: String,
    pub title: String,
    pub authors: String,
    pub date_issued: String,
    pub publisher: String,
    pub language: Option<String>,
    pub doi: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}


pub struct DocumentReader<T, R: BufRead> {
    pub dataset_version_id: Uuid,
    pub last_version: Version,
    pub header: DocumentHeader,
    reader: Reader<R>,
    bars: FrameImportBars,
    phantom_record: std::marker::PhantomData<T>,
}


impl<T, R> DocumentReader<T, R>
where
    T: IntoFrame,
    T::Atom: Default,
    R: BufRead,
    (DocumentHeader, Treatment): TryIntoRecord<T>,
{
    pub fn from_reader(reader: R, dataset_version_id: Uuid) -> Result<DocumentReader<T, R>, Error> {
        let mut reader = Reader::from_reader(reader);
        reader.config_mut().trim_text(true);

        let bars = FrameImportBars::new(0);
        let header = Self::parse_header(&mut reader)?;

        Ok(DocumentReader {
            last_version: Version::new(),
            dataset_version_id,
            header,
            reader,
            bars,
            phantom_record: std::marker::PhantomData,
        })
    }

    fn parse_header(reader: &mut Reader<R>) -> Result<DocumentHeader, Error> {
        let mut buf = Vec::new();

        // the <document> element should be the root element
        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "document") => return DocumentHeader::parse(reader, &e),
                Event::Eof => break,
                _ => {}
            }
        }

        Err(ParseError::NotFound("<document> element not found".to_string()).into())
    }

    fn parse_next_element(&mut self) -> Result<Option<Treatment>, Error> {
        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf)? {
                // documents can contain either treatments or mods. since we only really
                // want treatments we skip the mods for now, but the info in here can help
                // flesh out the details of a treatment
                Event::Start(e) if start_eq(&e, "mods:mods") => parse_mods(&mut self.reader)?,

                // each treatment is considered a frame here, as each one should be
                // a nomenclatural act and that is all we are extracting right now
                Event::Start(e) if start_eq(&e, "treatment") => {
                    let treatment = Treatment::parse(&mut self.reader, &e)?;
                    return Ok(Some(treatment));
                }

                // nothing more to parse
                Event::End(e) if end_eq(&e, "document") => return Ok(None),
                Event::Eof => return Ok(None),

                _ => {}
            }
        }
    }

    /// Read the next treatment in the document as a frame.
    ///
    /// For plazi treatment bank XML files we can have one or more treatments. A treatment
    /// can be seen as roughly equivalent to a record for the types of data we want to import
    /// so we consider a treatment as a 'frame' which is also considered a 'record changeset'.
    pub fn next_frame(&mut self) -> Result<Option<DataFrame<T::Atom>>, Error> {
        match self.parse_next_element()? {
            Some(treatment) => {
                let record: T = (self.header.clone(), treatment).try_into_record()?;

                // We hash the entity_id to save on storage in the column
                let mut hasher = Xxh3::new();
                hasher.update(record.entity_hashable());
                let hash = hasher.digest().to_string();

                // create the frame and convert the record into operation logs
                let frame = DataFrame::create(hash, self.dataset_version_id, self.last_version);
                let frame = record.into_frame(frame);
                Ok(Some(frame))
            }
            // no more treatments, effectively end of file
            None => Ok(None),
        }
    }
}


impl<T: IntoFrame, R: BufRead> FrameReader for DocumentReader<T, R> {
    type Atom = T::Atom;
}


impl<T, R> Iterator for DocumentReader<T, R>
where
    T: IntoFrame,
    T::Atom: Default,
    R: BufRead,
    (DocumentHeader, Treatment): TryIntoRecord<T>,
{
    type Item = Result<DataFrame<T::Atom>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_frame() {
            Err(err) => Some(Err(err)),
            Ok(Some(val)) => Some(Ok(val)),
            Ok(None) => None,
        }
    }
}


impl<T, R: BufRead> FrameProgress for DocumentReader<T, R> {
    fn bars(&self) -> FrameImportBars {
        self.bars.clone()
    }
}


impl<T: BufRead> ParseSection<T> for DocumentHeader {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        Ok(DocumentHeader {
            entity_id: parse_attribute(reader, event, "masterDocTitle")?,
            id: parse_attribute(reader, event, "masterDocId")?,
            title: parse_attribute(reader, event, "masterDocTitle")?,
            authors: parse_attribute(reader, event, "docAuthor")?,
            date_issued: parse_attribute(reader, event, "docDate")?,
            publisher: parse_attribute(reader, event, "docOrigin")?,
            language: parse_attribute_opt(reader, event, "docLanguage")?,
            doi: parse_attribute_opt(reader, event, "ID-DOI")?,
            created_at: parse_attribute(reader, event, "checkinTime")?.parse::<i64>()?,
            updated_at: parse_attribute(reader, event, "updateTime")?.parse::<i64>()?,
        })
    }
}

fn parse_mods<T: BufRead>(reader: &mut Reader<T>) -> Result<(), Error> {
    let mut buf = Vec::new();

    // skip mods
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::End(e) if e.name().as_ref() == b"mods:mods" => break,
            _ => {}
        }
    }

    Ok(())
}


pub trait TryIntoRecord<T: IntoFrame> {
    fn try_into_record(self) -> Result<T, Error>;
}


impl TryIntoRecord<publications::Record> for (DocumentHeader, Treatment) {
    fn try_into_record(self) -> Result<publications::Record, Error> {
        let (document, _) = self;
        let created_at = DateTime::from_timestamp(document.created_at, 0);
        let updated_at = DateTime::from_timestamp(document.updated_at, 0);

        let record = publications::Record {
            entity_id: document.entity_id,
            title: document.title,
            authors: vec![document.authors],
            published_year: document.date_issued.parse::<i32>()?,
            source_url: document.id,
            published_date: None,
            language: document.language,
            publisher: Some(document.publisher),
            doi: document.doi,
            publication_type: None,
            citation: None,
            created_at,
            updated_at,
        };
        Ok(record)
    }
}


impl TryIntoRecord<nomenclatural_acts::Record> for (DocumentHeader, Treatment) {
    fn try_into_record(self) -> Result<nomenclatural_acts::Record, Error> {
        let (document, treatment) = self;

        struct Nomenclature {
            scientific_name: String,
            canonical_name: String,
            scientific_name_authorship: Option<String>,
            act: NomenclaturalActType,
            acted_on: String,
            authority_name: Option<String>,
            authority_year: Option<String>,
            base_authority_name: Option<String>,
            base_authority_year: Option<String>,
        }

        // return the first processed nomenclature subsection from the document
        // there could be more than one but because we can't tell which one is the
        // correct one right now we settle for the first
        let nomenclature = treatment.sections.into_iter().find_map(|section| match section {
            Section::Nomenclature(mut nomenclature) => match nomenclature.taxonomic_names.pop_front() {
                None => None,
                Some(taxon) => Some(Nomenclature {
                    scientific_name: taxon.scientific_name(),
                    canonical_name: taxon.canonical_name(),
                    scientific_name_authorship: taxon.scientific_name_authority(),
                    act: nomenclature.act,
                    acted_on: nomenclature.acted_on,
                    authority_name: taxon.authority_name,
                    authority_year: taxon.authority_year.map(|year| year.to_string()),
                    base_authority_name: taxon.base_authority_name,
                    base_authority_year: taxon.base_authority_year,
                }),
            },
            _ => None,
        });

        let nomenclature = nomenclature.ok_or(ParseError::NotFound("nomenclature subsection".to_string()))?;

        let record = nomenclatural_acts::Record {
            entity_id: document.entity_id,
            publication: document.title,
            publication_date: Some(document.date_issued),
            source_url: document.id,
            scientific_name: nomenclature.scientific_name,
            canonical_name: nomenclature.canonical_name,
            scientific_name_authorship: nomenclature.scientific_name_authorship,
            authority_name: nomenclature.authority_name,
            authority_year: nomenclature.authority_year,
            base_authority_name: nomenclature.base_authority_name,
            base_authority_year: nomenclature.base_authority_year,
            act: nomenclature.act,
            acted_on: Some(nomenclature.acted_on),
        };

        Ok(record)
    }
}

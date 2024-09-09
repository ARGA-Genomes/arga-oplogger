use std::io::BufRead;
use std::path::PathBuf;
use std::str::FromStr;

use arga_core::crdt::{Frame, Version};
use arga_core::models::{
    Action,
    DatasetVersion,
    NomenclaturalActAtom,
    NomenclaturalActOperation,
    NomenclaturalActType,
    NomenclaturalActTypeError,
};
use arga_core::schema;
use bigdecimal::BigDecimal;
use diesel::*;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use tracing::info;
use xxhash_rust::xxh3::Xxh3;

use super::parsing::prelude::*;
use super::sections::prelude::*;
use crate::database::get_pool;
use crate::errors::Error;


#[derive(Debug)]
pub enum Extent {
    Page { start: usize, end: usize },
}

#[derive(Debug)]
pub struct Document {
    pub treatment_id: String,
    pub title: String,
    pub authors: String,
    // pub authors: Vec<Author>,
    pub date_issued: String,
    pub publisher: String,
    // pub place: String,
    // pub extent: Extent,
    // pub classification: Classification,
    // pub identifiers: Vec<Identifiers>,
    pub treatments: Vec<Treatment>,
}

#[derive(Debug)]
pub struct Author {
    pub name: String,
    pub affiliation: String,
}


pub fn import(input_dir: PathBuf, dataset_version: DatasetVersion) -> Result<(), Error> {
    info!("Enumerating files in '{input_dir:?}'");
    let files = xml_files(input_dir)?;

    let mut last_version = Version::new();
    let mut operations: Vec<NomenclaturalActOperation> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
        info!("Reading file {idx}: {file:?}");

        for document in read_file(&file)? {
            let mut hasher = Xxh3::new();
            hasher.update(document.title.as_bytes());
            let hash = hasher.digest();

            let mut frame = NomenclaturalActFrame::create(dataset_version.id, hash.to_string(), last_version);

            for atom in Vec::<NomenclaturalActAtom>::from(document) {
                frame.push(atom);
            }

            last_version = frame.frame.current;
            operations.extend(frame.frame.operations);
        }
    }

    // import_operations(operations)?;

    info!("Importing {} XML files", files.len());
    Ok(())
}

fn read_file(path: &PathBuf) -> Result<Vec<Document>, Error> {
    let mut documents = Vec::new();

    let mut reader = Reader::from_file(path)?;
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if start_eq(&e, "document") => documents.push(Document::parse(&mut reader, &e)?),
            Event::Eof => break,
            _ => {}
        };
    }

    Ok(documents)
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


impl<T: BufRead> ParseSection<T> for Document {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut treatments = Vec::new();

        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "document") => break,

                Event::Start(e) if start_eq(&e, "mods:mods") => parse_mods(reader)?,
                Event::Start(e) if start_eq(&e, "treatment") => treatments.push(Treatment::parse(reader, &e)?),

                // (state, event) => panic!("Unknown element. current_state: {state:?}, event: {event:#?}"),
                _ => {}
            }
        }

        Ok(Document {
            treatments,
            treatment_id: parse_attribute(reader, event, "docId")?,
            title: parse_attribute(reader, event, "masterDocTitle")?,
            authors: parse_attribute(reader, event, "docAuthor")?,
            date_issued: parse_attribute(reader, event, "docDate")?,
            publisher: parse_attribute(reader, event, "docOrigin")?,
        })
    }
}


fn xml_files(base_dir: PathBuf) -> Result<Vec<PathBuf>, Error> {
    let mut files = vec![];

    // walk the base directory by recursively calling this function
    for entry in std::fs::read_dir(&base_dir)? {
        let path = entry?.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "xml" {
                    files.push(path);
                }
            }
        }
        else if path.is_dir() {
            files.extend(xml_files(path.into())?);
        }
    }

    Ok(files)
}


fn import_operations(operations: Vec<NomenclaturalActOperation>) -> Result<(), Error> {
    use schema::nomenclatural_act_logs::dsl::*;

    let pool = get_pool()?;
    let mut conn = pool.get()?;

    for chunk in operations.chunks(1000) {
        diesel::insert_into(nomenclatural_act_logs)
            .values(chunk)
            .execute(&mut conn)?;
    }

    Ok(())
}


impl From<Document> for Vec<NomenclaturalActAtom> {
    fn from(document: Document) -> Self {
        use NomenclaturalActAtom::*;

        let mut operations = Vec::new();

        for treatment in document.treatments {
            for section in treatment.sections {
                match section {
                    Section::Nomenclature(nomenclature) => {
                        if let Some(taxon) = nomenclature.taxon {
                            let atoms: Vec<NomenclaturalActAtom> = taxon.into();
                            operations.extend(atoms);
                            operations.push(Publication(document.title.clone()));
                            operations.push(PublicationDate(document.date_issued.clone()));
                            operations.push(SourceUrl(treatment.http_uri.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }

        operations
    }
}


impl From<Treatment> for Vec<NomenclaturalActAtom> {
    fn from(treatment: Treatment) -> Self {
        let mut operations = Vec::new();

        for section in treatment.sections {
            match section {
                Section::Nomenclature(nomenclature) => {
                    if let Some(taxon) = nomenclature.taxon {
                        let atoms: Vec<NomenclaturalActAtom> = taxon.into();
                        operations.extend(atoms);
                    }
                }
                _ => {}
            }
        }

        operations
    }
}


impl From<TaxonomicName> for Vec<NomenclaturalActAtom> {
    fn from(value: TaxonomicName) -> Self {
        use NomenclaturalActAtom::*;

        let mut operations = Vec::new();

        match SpeciesName::try_from(value) {
            Ok(name) => {
                // match name.act {
                //     Some(NomenclaturalActType::SpeciesNova) => operations.push(ActedOn("Biota".to_string())),
                //     Some(NomenclaturalActType::CombinatioNova) => operations.push(ActedOn(name.genus.clone())),
                //     Some(NomenclaturalActType::RevivedStatus) => operations.push(ActedOn(name.full_name())),
                //     Some(NomenclaturalActType::GenusSpeciesNova) => operations.push(ActedOn(name.genus.clone())),
                //     Some(NomenclaturalActType::SubspeciesNova) => operations.push(ActedOn(name.full_name())),
                //     None => {}
                // };

                if let Some(act) = name.act {
                    operations.push(Act(act));
                }
                // operations.push(Rank(name.rank));

                // operations.push(Genus(name.genus));
                // operations.push(SpecificEpithet(name.specific_epithet));
                operations.push(ScientificName(name.scientific_name));
                operations.push(CanonicalName(name.canonical_name));

                if let Some(authority) = name.authority {
                    operations.push(AuthorityName(authority.name));
                    operations.push(AuthorityYear(authority.year));
                }

                if let Some(authority) = name.basionym_authority {
                    operations.push(BasionymAuthorityName(authority.name));
                    operations.push(BasionymAuthorityYear(authority.year));
                }
            }
            Err(err) => println!("{err:#?}"),
        }

        operations
    }
}


impl TaxonomicName {
    pub fn full_name(&self) -> String {
        let mut name = match self.rank.as_ref().map(|s| s.as_str()) {
            Some("genus") => format!("{}", self.genus.clone().unwrap()),
            Some("species") => {
                format!("{} {}", self.genus.clone().unwrap(), self.species.clone().unwrap_or("".to_string()))
                    .trim()
                    .to_string()
            }
            _ => self.name.to_string(),
        };

        if let Some(auth) = &self.base_authority_name {
            let basionym_year = self.base_authority_year.clone().unwrap_or("".to_string());
            let basionym_auth = format!("{auth} {}", basionym_year).trim().to_string();
            name = format!("{name} ({basionym_auth})");
        };

        let authority = self.authority.clone().unwrap_or("".to_string());
        name = format!("{name} {}", authority).trim().to_string();
        name
    }
}


#[derive(Debug)]
pub struct AuthorityName {
    pub name: String,
    pub year: String,
}

impl std::fmt::Display for AuthorityName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}, {}", self.name, self.year))
    }
}

#[derive(Debug)]
pub struct SpeciesName {
    pub act: Option<NomenclaturalActType>,
    pub rank: String,

    pub scientific_name: String,
    pub canonical_name: String,
    pub authority: Option<AuthorityName>,
    pub basionym_authority: Option<AuthorityName>,
}


impl TryFrom<TaxonomicName> for SpeciesName {
    type Error = SpeciesNameError;

    fn try_from(value: TaxonomicName) -> Result<Self, Self::Error> {
        // construct canonical name from parsed name, matched to a taxon
        let genus = value.genus.ok_or(SpeciesNameError::MissingGenus)?;
        let specific_epithet = value.species.ok_or(SpeciesNameError::MissingSpecificEpithet)?;

        let canonical_name = match value.subgenus {
            Some(subgenus) => format!("{} {subgenus} {}", genus, specific_epithet),
            None => format!("{} {}", genus, specific_epithet),
        };

        let authority = match (value.authority_name, value.authority_year) {
            (Some(name), Some(year)) => Some(AuthorityName {
                name,
                year: year.to_string(),
            }),
            _ => None,
        };

        let basionym_authority = match (value.base_authority_name, value.base_authority_year) {
            (Some(name), Some(year)) => Some(AuthorityName { name, year }),
            _ => None,
        };

        let act = match value.status {
            Some(status) => NomenclaturalActType::try_from(status.as_str()).ok(),
            None => None,
        };
        // let status = value.status.ok_or(SpeciesNameError::MissingStatus)?;
        // let act = NomenclaturalActType::try_from(status.as_str())?;
        let rank = value.rank.ok_or(SpeciesNameError::MissingRank)?;

        // construct scientific name
        let scientific_name = match (&authority, &basionym_authority) {
            (Some(auth), Some(base)) => format!("{canonical_name} ({base}) {auth}"),
            (Some(auth), None) => format!("{canonical_name} {auth}"),
            (None, Some(base)) => format!("{canonical_name} ({base})"),
            (None, None) => canonical_name.clone(),
        };

        Ok(SpeciesName {
            act,
            rank,
            scientific_name,
            canonical_name,
            authority,
            basionym_authority,
        })
    }
}

#[derive(Debug)]
pub enum SpeciesNameError {
    MissingGenus,
    MissingSpecificEpithet,
    MissingAuthority,
    InvalidAuthority,
    MissingStatus,
    InvalidStatus(NomenclaturalActTypeError),
    MissingRank,
}

impl From<NomenclaturalActTypeError> for SpeciesNameError {
    fn from(value: NomenclaturalActTypeError) -> Self {
        SpeciesNameError::InvalidStatus(value)
    }
}


pub struct NomenclaturalActFrame {
    dataset_version_id: uuid::Uuid,
    entity_id: String,
    frame: Frame<NomenclaturalActOperation>,
}

impl NomenclaturalActFrame {
    pub fn create(dataset_version_id: uuid::Uuid, entity_id: String, last_version: Version) -> NomenclaturalActFrame {
        let mut frame = Frame::new(last_version);

        frame.push(NomenclaturalActOperation {
            operation_id: frame.next.into(),
            parent_id: frame.current.into(),
            dataset_version_id,
            entity_id: entity_id.clone(),
            action: Action::Create,
            atom: NomenclaturalActAtom::Empty,
        });

        NomenclaturalActFrame {
            dataset_version_id,
            entity_id,
            frame,
        }
    }

    pub fn push(&mut self, atom: NomenclaturalActAtom) {
        let operation_id: BigDecimal = self.frame.next.into();
        let parent_id = self
            .frame
            .operations
            .last()
            .map(|op| op.operation_id.clone())
            .unwrap_or(operation_id.clone());

        let op = NomenclaturalActOperation {
            operation_id,
            parent_id,
            dataset_version_id: self.dataset_version_id,
            entity_id: self.entity_id.clone(),
            action: Action::Update,
            atom,
        };

        self.frame.push(op);
    }

    pub fn operations(&self) -> &Vec<NomenclaturalActOperation> {
        &self.frame.operations
    }
}

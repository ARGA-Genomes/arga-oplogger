use std::str::FromStr;

use crate::errors::{Error, ParseError};


#[derive(Debug)]
pub enum Identifiers {
    Doi(String),
    Isbn(String),
    Zenodo(String),
    GbifDataset(String),
    Issn(String),
    Zoobank(String),
    ClbDataset(String),
}


#[derive(Debug)]
pub enum Classification {
    Book,
    BookChapter,
    JournalArticle,
    JournalVolume,
    ProceedingsPaper,
    Proceedings,
    Url,
}

impl FromStr for Classification {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "book" => Ok(Self::Book),
            "book chapter" => Ok(Self::BookChapter),
            "journal article" => Ok(Self::JournalArticle),
            "journal volume" => Ok(Self::JournalVolume),
            "proceedings paper" => Ok(Self::ProceedingsPaper),
            "proceedings" => Ok(Self::Proceedings),
            "url" => Ok(Self::Url),
            val => Err(Error::Parsing(ParseError::InvalidValue(val.to_string()))),
        }
    }
}

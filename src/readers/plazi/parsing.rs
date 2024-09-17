pub use std::io::BufRead;
use std::str::FromStr;

use quick_xml::events::{BytesEnd, BytesStart};
use quick_xml::name::QName;
use quick_xml::Reader;

use super::formatting::prelude::Span;
use crate::errors::{Error, ParseError};


pub mod prelude {
    pub use std::io::BufRead;

    pub use quick_xml::events::{BytesStart, Event};
    pub use quick_xml::Reader;

    pub use super::{
        end_eq,
        parse_attribute,
        parse_attribute_opt,
        parse_attribute_string,
        parse_attribute_string_opt,
        start_eq,
        unwrap_element,
        ParseFormat,
        ParseSection,
    };
    pub use crate::errors::Error;
}


/// Parse a section and it's hierarchy
pub trait ParseSection<T>
where
    T: BufRead,
    Self: Sized,
{
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error>;
}

/// Parse a formatting element with its children
pub trait ParseFormat<T>
where
    T: BufRead,
    Self: Sized,
{
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error>;
}


pub fn name_eq(name: QName, target: &str) -> bool {
    name.as_ref() == target.as_bytes()
}

pub fn start_eq(event: &BytesStart, name: &str) -> bool {
    name_eq(event.name(), name)
}

pub fn end_eq(event: &BytesEnd, name: &str) -> bool {
    name_eq(event.name(), name)
}


pub fn parse_attribute<R>(reader: &Reader<R>, event: &BytesStart, name: &str) -> Result<String, Error> {
    match event.try_get_attribute(name)? {
        Some(value) => {
            let value = value.decode_and_unescape_value(reader.decoder())?;
            // remove unicode breakpoints
            // example: EF2B5D36DE955281B27A2E77DF660D0F.xml
            let value = value.trim_matches('\u{feff}');
            Ok(value.trim().to_string())
        }
        None => Err(Error::Parsing(ParseError::NotFound(name.to_string()))),
    }
}

pub fn parse_attribute_opt<R>(reader: &Reader<R>, event: &BytesStart, name: &str) -> Result<Option<String>, Error> {
    match event.try_get_attribute(name)? {
        Some(value) => Ok(Some(value.decode_and_unescape_value(reader.decoder())?.into_owned())),
        None => Ok(None),
    }
}

pub fn parse_attribute_string<R, T: FromStr>(reader: &Reader<R>, event: &BytesStart, name: &str) -> Result<T, Error> {
    let value = parse_attribute(reader, event, name)?;
    str::parse::<T>(&value).map_err(|_| Error::Parsing(ParseError::InvalidValue(value)))
}

pub fn parse_attribute_string_opt<R, T: FromStr>(
    reader: &Reader<R>,
    event: &BytesStart,
    name: &str,
) -> Result<Option<T>, Error> {
    let value = parse_attribute_opt(reader, event, name)?;
    match value {
        Some(v) => match str::parse::<T>(&v) {
            Ok(v) => Ok(Some(v)),
            Err(_) => Err(Error::Parsing(ParseError::InvalidValue(v))),
        },
        None => Ok(None),
    }
}

pub fn unwrap_element<T>(element: Option<T>, name: &str) -> Result<T, Error> {
    match element {
        Some(inner) => Ok(inner),
        None => Err(Error::Parsing(ParseError::NotFound(name.to_string()))),
    }
}

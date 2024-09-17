use super::formatting::prelude::*;
use super::parsing::prelude::*;


#[derive(Debug)]
pub struct CollectionCode {
    pub id: Option<String>,
    pub country: Option<String>,
    pub uri: Option<String>,
    pub name: Option<String>,
    pub collection_name: Option<String>,
}

#[derive(Debug)]
pub struct SpecimenCode {
    pub id: String,
    pub collection_code: String,
    pub children: Vec<Span>,
}

impl<T: BufRead> ParseFormat<T> for CollectionCode {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "collectionCode") => break,
                _ => {}
            }
        }

        Ok((
            CollectionCode {
                id: parse_attribute_string_opt(reader, event, "id")?,
                country: parse_attribute_string_opt(reader, event, "country")?,
                uri: parse_attribute_string_opt(reader, event, "httpUri")?,
                name: parse_attribute_string_opt(reader, event, "name")?,
                collection_name: parse_attribute_string_opt(reader, event, "collectionName")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}

impl<T: BufRead> ParseSection<T> for SpecimenCode {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "specimenCode") => break,
                _ => {}
            }
        }

        Ok(SpecimenCode {
            id: parse_attribute_string(reader, event, "id")?,
            collection_code: parse_attribute_string(reader, event, "collectionCode")?,
            children: stack.commit_and_pop_all(),
        })
    }
}

use super::formatting::prelude::*;
use super::parsing::prelude::*;


#[derive(Debug)]
pub struct CollectingCountry {
    pub id: Option<String>,
    pub name: String,
}

impl<T: BufRead> ParseFormat<T> for CollectingCountry {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "collectingCountry") => break,
                Event::Text(txt) => stack.push(Span::text(&txt.unescape()?.into_owned())),
                _ => {}
            }
        }

        Ok((
            CollectingCountry {
                id: parse_attribute_opt(reader, event, "id")?,
                name: parse_attribute_string(reader, event, "name")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}


#[derive(Debug)]
pub struct CollectingRegion;

impl<T: BufRead> ParseSection<T> for CollectingRegion {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "collectingRegion") => break,
                _ => {}
            }
        }

        Ok(CollectingRegion)
    }
}

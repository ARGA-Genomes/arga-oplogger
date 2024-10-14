use super::formatting::prelude::*;
use super::parsing::prelude::*;


#[derive(Debug)]
pub struct TypeStatus {
    pub id: String,
    pub r#type: Option<String>,
}

impl<T: BufRead> ParseFormat<T> for TypeStatus {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "typeStatus") => break,
                _ => {}
            }
        }

        Ok((
            TypeStatus {
                id: parse_attribute_string(reader, event, "id")?,
                r#type: parse_attribute_string_opt(reader, event, "type")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}

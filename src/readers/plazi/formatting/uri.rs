use super::parsing::prelude::*;
use super::prelude::*;


#[derive(Debug)]
pub struct Uri {
    pub page_number: Option<String>,
    pub page_id: Option<String>,
}

impl<T: BufRead> ParseFormat<T> for Uri {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "uuid") => {
                    let uuid = Uuid::parse(reader, &e)?;
                    stack.push(Span::uuid(&uuid.value));
                }

                // formatting issues
                // example: F75887D56F6F2224FF2CFD8B028FFA52.xml:
                Event::Start(e) if start_eq(&e, "emphasis") => {}
                Event::End(e) if end_eq(&e, "emphasis") => {}

                Event::Text(txt) => stack.push(Span::text(&txt.unescape()?.into_owned())),
                Event::End(e) if end_eq(&e, "uri") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((
            Uri {
                page_id: parse_attribute_opt(reader, event, "pageId")?,
                page_number: parse_attribute_opt(reader, event, "pageNumber")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}

use super::parsing::prelude::*;
use super::prelude::*;
use super::sections::prelude::*;


#[derive(Debug)]
pub struct PageBreakToken {
    pub id: Option<String>,
    pub page_number: String,
    pub page_id: Option<String>,
    pub start: Option<String>,
}

impl<T: BufRead> ParseFormat<T> for PageBreakToken {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Text(txt) => {
                    stack.push(Span::text(&txt.unescape()?.into_owned()));
                }
                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    stack.push(Span::normalized_token(&token.value));
                }

                // TODO: include taxonomic names as they can appear here
                // for some badly formatted docs
                // example: 933B3C9271DBE631F2AF55B072D5B2FA.xml
                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    TaxonomicName::parse(reader, &e)?;
                }

                // TODO: include subsections in the stack
                // example: 933B3C9271DBE631F2AF55B072D5B2FA.xml
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _section = SubSection::parse(reader, event)?;
                }

                Event::End(e) if end_eq(&e, "pageBreakToken") => {
                    break;
                }
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((
            PageBreakToken {
                id: parse_attribute_opt(reader, event, "id")?,
                page_number: parse_attribute(reader, event, "pageNumber")?,
                page_id: parse_attribute_opt(reader, event, "pageId")?,
                start: parse_attribute_opt(reader, event, "start")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}

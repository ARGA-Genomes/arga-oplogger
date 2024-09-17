use super::formatting::prelude::*;
use super::parsing::prelude::*;
use super::prelude::*;


#[derive(Debug)]
pub struct Authority {
    pub page_number: Option<String>,
    pub page_id: Option<String>,
    pub value: String,
}

impl<T: BufRead> ParseSection<T> for Authority {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut value = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // unnest authority elements for now
                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "authority") => {
                    let authority = Authority::parse(reader, &e)?;
                    value = Some(authority.value);
                }

                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "tnCandidate") => {}
                Event::End(e) if end_eq(&e, "tnCandidate") => {}

                // skip the citation since its likely a formatting error when
                // inside an authority block
                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "bibRefCitation") => {}
                Event::End(e) if end_eq(&e, "bibRefCitation") => {}

                // example: 1C90B8491123FC34BBFF579C461B2A5B.xml
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _ = SubSection::parse(reader, &e)?;
                }

                // example: 1570793047D4110BB81D1FD9C5A591BC.xml
                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    value = Some(token.value);
                }

                Event::Text(txt) => value = Some(txt.unescape()?.into_owned()),
                Event::End(e) if end_eq(&e, "authority") => break,
                Event::End(e) if end_eq(&e, "authorityName") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(Authority {
            page_id: parse_attribute_opt(reader, event, "pageId")?,
            page_number: parse_attribute_opt(reader, event, "pageNumber")?,
            value: unwrap_element(value, "authority")?,
        })
    }
}

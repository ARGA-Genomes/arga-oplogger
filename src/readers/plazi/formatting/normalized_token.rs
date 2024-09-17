use super::parsing::prelude::*;


#[derive(Debug)]
pub struct NormalizedToken {
    pub id: Option<String>,
    pub original_value: String,
    pub value: String,
}


impl<T: BufRead> ParseSection<T> for NormalizedToken {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut value = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // example: 7705F565923E7BFD5641CF137BCF0B39.xml
                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    value = Some(token.value);
                }

                Event::Text(txt) => value = Some(txt.unescape()?.into_owned()),
                Event::End(e) if end_eq(&e, "normalizedToken") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(NormalizedToken {
            id: parse_attribute_opt(reader, event, "id")?,
            original_value: parse_attribute(reader, event, "originalValue")?,
            value: unwrap_element(value, "normalizedToken")?,
        })
    }
}

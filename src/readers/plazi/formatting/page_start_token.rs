use super::parsing::prelude::*;


#[derive(Debug)]
pub struct PageStartToken {
    pub id: Option<String>,
    pub page_number: String,
    pub value: String,
}


impl<T: BufRead> ParseSection<T> for PageStartToken {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut value = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // formatting issues
                // example: F751989F99FA5995A8FEB280301A1327.xml
                Event::Start(e) if start_eq(&e, "pageStartToken") => {
                    let token = PageStartToken::parse(reader, &e)?;
                    value = Some(token.value);
                }

                Event::Text(txt) => value = Some(txt.unescape()?.into_owned()),
                Event::End(e) if end_eq(&e, "pageStartToken") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(PageStartToken {
            id: parse_attribute_opt(reader, event, "id")?,
            page_number: parse_attribute(reader, event, "pageNumber")?,
            value: unwrap_element(value, "pageStartToken")?,
        })
    }
}

use super::parsing::prelude::*;


#[derive(Debug)]
pub struct Uuid {
    pub page_number: Option<String>,
    pub page_id: Option<String>,
    pub value: String,
}

impl<T: BufRead> ParseSection<T> for Uuid {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut value = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // formatting issues
                // example: 320587A7EB521D7DFF27F9102B4EE6DF.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {}
                Event::End(e) if end_eq(&e, "collectingRegion") => {}

                // example: 4511E41DD832FFD1FDE8FD21F0B7F80C.xml
                Event::Start(e) if start_eq(&e, "collectionCode") => {}
                Event::End(e) if end_eq(&e, "collectionCode") => {}

                // example: D026802BEE23A261FF4CA038FB1BFD84.xml
                Event::Start(e) if start_eq(&e, "date") => {}
                Event::End(e) if end_eq(&e, "date") => {}

                Event::Text(txt) => value = Some(txt.unescape()?.into_owned()),
                Event::End(e) if end_eq(&e, "uuid") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(Uuid {
            page_id: parse_attribute_opt(reader, event, "pageId")?,
            page_number: parse_attribute_opt(reader, event, "pageNumber")?,
            value: unwrap_element(value, "uuid")?,
        })
    }
}

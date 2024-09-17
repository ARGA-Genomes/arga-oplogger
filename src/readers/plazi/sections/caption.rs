use super::parsing::prelude::*;


#[derive(Debug)]
pub struct Caption;

impl<T: BufRead> ParseSection<T> for Caption {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();
        let mut depth = 0;

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "caption") => depth += 1,
                Event::End(e) if end_eq(&e, "caption") => {
                    // also skip nested captions
                    // example: 2F489243A56BFFEDD3DAF88AB1FBF996.xml
                    if depth <= 0 {
                        break;
                    }
                    else {
                        depth -= 1;
                    }
                }
                _ => {}
            }
        }

        Ok(Caption)
    }
}

use super::parsing::prelude::*;


#[derive(Debug)]
pub struct MaterialsCitation;


impl<T: BufRead> ParseSection<T> for MaterialsCitation {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();
        let mut depth = 0;

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "materialsCitation") => depth += 1,
                Event::End(e) if end_eq(&e, "materialsCitation") => {
                    // example: 153287B6FD0FFFF1FC8CF8F2587CFB15.xml
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

        Ok(MaterialsCitation)
    }
}

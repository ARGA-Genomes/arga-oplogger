use super::parsing::prelude::*;


#[derive(Debug)]
pub struct Footnote;

impl<T: BufRead> ParseSection<T> for Footnote {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "footnote") => break,
                _ => {}
            }
        }

        Ok(Footnote)
    }
}

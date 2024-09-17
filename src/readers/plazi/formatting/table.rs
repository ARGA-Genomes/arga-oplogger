use super::parsing::prelude::*;
use super::prelude::*;
use super::sections::prelude::*;


#[derive(Debug)]
pub struct Table;


impl<T: BufRead> ParseFormat<T> for Table {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "th") => stack.push(Span::th()),
                Event::Start(e) if start_eq(&e, "tr") => stack.push(Span::tr()),
                Event::Start(e) if start_eq(&e, "td") => stack.push(Span::td()),
                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::Start(e) if start_eq(&e, "paragraph") => stack.push(Span::paragraph()),
                Event::Start(e) if start_eq(&e, "heading") => stack.push(Span::paragraph()),
                Event::End(e) if end_eq(&e, "th") => stack.commit_top(),
                Event::End(e) if end_eq(&e, "tr") => stack.commit_top(),
                Event::End(e) if end_eq(&e, "td") => stack.commit_top(),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),
                Event::End(e) if end_eq(&e, "paragraph") => stack.commit_top(),
                Event::End(e) if end_eq(&e, "heading") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "pageBreakToken") => {
                    let (attrs, children) = PageBreakToken::parse(reader, &e)?;
                    stack.push(Span::page_break_token(attrs, children));
                }

                Event::Start(e) if start_eq(&e, "tableNote") => {}
                Event::End(e) if end_eq(&e, "tableNote") => {}

                // TODO: include parsed details rather than an empty span
                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    let _taxon = TaxonomicName::parse(reader, &e)?;
                    stack.push(Span::taxonomic_name());
                }

                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    stack.push(Span::normalized_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "collectingCountry") => {
                    let (attrs, children) = CollectingCountry::parse(reader, &e)?;
                    stack.push(Span::collecting_country(attrs, children));
                }

                Event::Start(e) if start_eq(&e, "collectingCounty") => {}
                Event::End(e) if end_eq(&e, "collectingCounty") => {}

                // example: 6C3DA91C515E183EFF0EFC55EE0F1E5C.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {}
                Event::End(e) if end_eq(&e, "collectingRegion") => {}

                Event::Start(e) if start_eq(&e, "collectorName") => {}
                Event::End(e) if end_eq(&e, "collectorName") => {}

                // TODO: include labels as well. we skip all these for now
                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    let _label = TaxonomicNameLabel::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "quantity") => {
                    let _quantity = Quantity::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _subsection = SubSection::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let _citation = Citation::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "collectionCode") => {
                    let _ = CollectionCode::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "typeStatus") => {
                    let _ = TypeStatus::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    let _cit = MaterialsCitation::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "specimenCode") => {}
                Event::End(e) if end_eq(&e, "specimenCode") => {}

                // example: 5C3AAE49D87F3C5F63BE6F5FFAF327FF.xml
                Event::Start(e) if start_eq(&e, "specimenCount") => {}
                Event::End(e) if end_eq(&e, "specimenCount") => {}

                Event::Start(e) if start_eq(&e, "location") => {}
                Event::End(e) if end_eq(&e, "location") => {}

                // example: 282F878EFFA5FFB614C02171FD565F68.xml
                Event::Start(e) if start_eq(&e, "date") => {}
                Event::End(e) if end_eq(&e, "date") => {}

                // example: 6C3AE5AE713A54F87C76C60DD13CAA1D.xml
                Event::Start(e) if start_eq(&e, "key") => {}
                Event::End(e) if end_eq(&e, "key") => {}

                // example: 6C3AE5AE713A54F87C76C60DD13CAA1D.xml
                Event::Start(e) if start_eq(&e, "keyStep") => {}
                Event::End(e) if end_eq(&e, "keyStep") => {}

                // example: 6C3AE5AE713A54F87C76C60DD13CAA1D.xml
                Event::Start(e) if start_eq(&e, "keyLead") => {}
                Event::End(e) if end_eq(&e, "keyLead") => {}

                // example: 1C528797FF94FFBC6AA492C181612CAC.xml
                Event::Start(e) if start_eq(&e, "accessionNumber") => stack.push(Span::accession_number()),
                Event::End(e) if end_eq(&e, "accessionNumber") => stack.commit_top(),

                // example: 930C87E9FF9DFFEFB39EFACFFF6BA520.xml
                Event::Start(e) if start_eq(&e, "table") => {
                    Table::parse(reader, &e)?;
                }

                Event::Text(txt) => stack.push(Span::text(&txt.unescape()?.into_owned())),
                Event::End(e) if end_eq(&e, "table") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((Table, stack.commit_and_pop_all()))
    }
}


#[derive(Debug)]
pub struct TableNote;

impl<T: BufRead> ParseSection<T> for TableNote {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "tableNote") => break,
                _ => {}
            }
        }

        Ok(TableNote)
    }
}

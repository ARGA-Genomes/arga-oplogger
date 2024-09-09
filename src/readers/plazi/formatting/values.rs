use super::parsing::prelude::*;
use super::prelude::*;
use super::sections::prelude::*;


#[derive(Debug)]
pub struct Quantity;

impl<T: BufRead> ParseSection<T> for Quantity {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "quantity") => break,
                _ => {}
            }
        }

        Ok(Quantity)
    }
}


#[derive(Debug)]
pub struct Date;

impl<T: BufRead> ParseSection<T> for Date {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "date") => break,
                _ => {}
            }
        }

        Ok(Date)
    }
}


#[derive(Debug)]
pub struct FormattedValue;

impl<T: BufRead> ParseFormat<T> for FormattedValue {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // TODO: include subsections in the stack
                // example: 2F77A229F6E97F1EB2081B1C4F277ABE.xml
                Event::Start(e) if start_eq(&e, "subSection") => {
                    let _section = SubSection::parse(reader, event)?;
                }

                Event::Start(e) if start_eq(&e, "paragraph") => stack.push(Span::paragraph()),
                Event::End(e) if end_eq(&e, "paragraph") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "heading") => stack.push(Span::heading()),
                Event::End(e) if end_eq(&e, "heading") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "smallCapsWord") => stack.push(Span::small_caps()),
                Event::End(e) if end_eq(&e, "smallCapsWord") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "superScript") => stack.push(Span::superscript()),
                Event::End(e) if end_eq(&e, "superScript") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "keyLead") => stack.push(Span::key_lead()),
                Event::End(e) if end_eq(&e, "keyLead") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "keyStep") => stack.push(Span::key_step()),
                Event::End(e) if end_eq(&e, "keyStep") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "number") => stack.push(Span::number()),
                Event::End(e) if end_eq(&e, "number") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "table") => {
                    let (_table, children) = Table::parse(reader, &e)?;
                    stack.push(Span::Table(children));
                    stack.commit_top();
                }
                Event::End(e) if end_eq(&e, "table") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "treatmentCitationGroup") => {
                    stack.push(Span::treatment_citation_group())
                }
                Event::End(e) if end_eq(&e, "treatmentCitationGroup") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "treatmentCitation") => stack.push(Span::treatment_citation_group()),
                Event::End(e) if end_eq(&e, "treatmentCitation") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    // TODO: include citations in nomenclature block
                    let _cit = MaterialsCitation::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "pageStartToken") => {
                    let token = PageStartToken::parse(reader, &e)?;
                    stack.push(Span::page_start_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "pageBreakToken") => {
                    let (token, children) = PageBreakToken::parse(reader, &e)?;
                    stack.push(Span::page_break_token(token, children));
                }

                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let (attrs, children) = Citation::parse(reader, &e)?;
                    stack.push(Span::citation(attrs, children));
                }

                Event::Start(e) if start_eq(&e, "bibCitation") => {
                    let (attrs, children) = BibCitation::parse(reader, &e)?;
                    stack.push(Span::bib_citation(attrs, children));
                }

                Event::Start(e) if start_eq(&e, "bibRef") => {
                    let (_, children) = BibRef::parse(reader, &e)?;
                    stack.push(Span::bib_ref(children));
                }

                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    let _attrs = TaxonomicName::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    TaxonomicNameLabel::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "authorityName") => {
                    let authority = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&authority.value));
                }

                Event::Start(e) if start_eq(&e, "uri") => {
                    let (_uri, children) = Uri::parse(reader, &e)?;
                    stack.push(Span::uri(children));
                }

                Event::Start(e) if start_eq(&e, "collectionCode") => {
                    let (attributes, children) = CollectionCode::parse(reader, &e)?;
                    stack.push(Span::CollectionCode { attributes, children });
                }

                Event::Start(e) if start_eq(&e, "uuid") => {
                    let uuid = Uuid::parse(reader, &e)?;
                    stack.push(Span::uuid(&uuid.value));
                }

                // possible formatting issues
                // example: 3282FC3A3CE0265C0CA9723A898CF2A3.xml
                Event::Start(e) if start_eq(&e, "tr") => {}
                Event::End(e) if end_eq(&e, "tr") => {}

                // example: 3282FC3A3CE0265C0CA9723A898CF2A3.xml
                Event::Start(e) if start_eq(&e, "td") => {}
                Event::End(e) if end_eq(&e, "td") => {}

                // example: 8109941EFFECD4585815C433FE67FAE1.xml
                Event::Start(e) if start_eq(&e, "tableNote") => {}
                Event::End(e) if end_eq(&e, "tableNote") => {}

                // example: D5518791DC65FFE615CCFE0FA36AE647.xml
                Event::Start(e) if start_eq(&e, "treatmentHeading") => {}
                Event::End(e) if end_eq(&e, "treatmentHeading") => {}

                Event::Start(e) if start_eq(&e, "typeStatus") => {}
                Event::End(e) if end_eq(&e, "typeStatus") => {}

                Event::Start(e) if start_eq(&e, "figureCitation") => {}
                Event::End(e) if end_eq(&e, "figureCitation") => {}

                Event::Start(e) if start_eq(&e, "tableCitation") => {}
                Event::End(e) if end_eq(&e, "tableCitation") => {}

                // example: 3212E320FF8FFF9E1FC7957AA435AEDF.xml
                Event::Start(e) if start_eq(&e, "docIssue") => {}
                Event::End(e) if end_eq(&e, "docIssue") => {}

                // example: 5E1287EBFFFAFFE8FF91BC03FF4CF9F8.xml
                Event::Start(e) if start_eq(&e, "docAuthor") => {}
                Event::End(e) if end_eq(&e, "docAuthor") => {}

                Event::Start(e) if start_eq(&e, "geoCoordinate") => {}
                Event::End(e) if end_eq(&e, "geoCoordinate") => {}

                Event::Start(e) if start_eq(&e, "quantity") => {}
                Event::End(e) if end_eq(&e, "quantity") => {}

                Event::Start(e) if start_eq(&e, "date") => {}
                Event::End(e) if end_eq(&e, "date") => {}

                // example: 2F4D87AFF92EFF845A97B4918263A116.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {}
                Event::End(e) if end_eq(&e, "collectingRegion") => {}

                Event::Start(e) if start_eq(&e, "collectingCountry") => {}
                Event::End(e) if end_eq(&e, "collectingCountry") => {}

                // example: EF3E87CA7D34EE49FAFA79194930F820.xml
                Event::Start(e) if start_eq(&e, "collectingCounty") => {}
                Event::End(e) if end_eq(&e, "collectingCounty") => {}

                // example: EF3E87CA7D34EE49FAFA79194930F820.xml
                Event::Start(e) if start_eq(&e, "collectingMunicipality") => {}
                Event::End(e) if end_eq(&e, "collectingMunicipality") => {}

                // example: F74B87EFF70CFF85E5E78FEEFD47546E.xml
                Event::Start(e) if start_eq(&e, "collectingDate") => {}
                Event::End(e) if end_eq(&e, "collectingDate") => {}

                // example: EF3E87CA7D34EE49FAFA79194930F820.xml
                Event::Start(e) if start_eq(&e, "location") => {}
                Event::End(e) if end_eq(&e, "location") => {}

                Event::Start(e) if start_eq(&e, "collectorName") => {}
                Event::End(e) if end_eq(&e, "collectorName") => {}

                Event::Start(e) if start_eq(&e, "specimenCount") => {}
                Event::End(e) if end_eq(&e, "specimenCount") => {}

                // example: 321387DE8D500864FDC787BAA9530652.xml
                Event::Start(e) if start_eq(&e, "accessionNumber") => {}
                Event::End(e) if end_eq(&e, "accessionNumber") => {}

                // example: EF6B32047275315C535517791DD1F7C4.xml
                Event::Start(e) if start_eq(&e, "potBibRef") => {}
                Event::End(e) if end_eq(&e, "potBibRef") => {}

                // example: A160333CFFA0FF95F287AFB9A1ACA60F.xml
                Event::Start(e) if start_eq(&e, "collectedFrom") => {}
                Event::End(e) if end_eq(&e, "collectedFrom") => {}

                // example: A10D4838C343374025E7FEA61928FDDA.xml
                Event::Start(e) if start_eq(&e, "elevation") => {}
                Event::End(e) if end_eq(&e, "elevation") => {}

                // example: D557D228F43BFFB9D58FFE83FAE8FF31.xml
                Event::Start(e) if start_eq(&e, "docTitle") => {}
                Event::End(e) if end_eq(&e, "docTitle") => {}

                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    stack.push(Span::normalized_token(&token.value));
                }

                Event::Text(txt) => {
                    let txt = txt.unescape()?.into_owned();
                    stack.push(Span::text(&txt));
                }

                // TODO: this might just be a formatting issue. could be worth
                // unnesting subsections in the nomenclature section to get more
                // details
                // example: EF0787806245345A07D3FB14FCCD5142.xml
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _subsection = SubSection::parse(reader, &e)?;
                }

                // example: EF63878E9A1FFFC2FF6F4E2EFD25FDF4.xml
                Event::Start(e) if start_eq(&e, "caption") => {
                    let _caption = Caption::parse(reader, &e)?;
                }

                Event::End(e) if end_eq(&e, "subSubSection") => break,
                Event::End(e) if end_eq(&e, "subSection") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((FormattedValue, stack.commit_and_pop_all()))
    }
}

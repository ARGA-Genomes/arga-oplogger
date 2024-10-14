use super::formatting::prelude::*;
use super::parsing::prelude::*;
use super::prelude::*;


#[derive(Debug)]
pub struct Citation {
    pub id: Option<String>,
    pub author: Option<String>,
    pub reference_id: Option<String>,
    pub reference: Option<String>,
    pub classification: Option<Classification>,
    pub year: Option<String>,
}

impl<T: BufRead> ParseFormat<T> for Citation {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    stack.push(Span::normalized_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "figureCitation") => stack.push(Span::figure_citation()),
                Event::End(e) if end_eq(&e, "figureCitation") => stack.commit_top(),

                // example: EF0FA6473571C259FFB0FCB8D18558F0.xml
                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),

                // example: 522D5760FFDBBE26FF0BFE68FBA20E6E.xml
                Event::Start(e) if start_eq(&e, "smallCapsWord") => stack.push(Span::small_caps()),
                Event::End(e) if end_eq(&e, "smallCapsWord") => stack.commit_top(),

                // example: 43368780FA42184FBA83FEA4FB67F976.xml
                Event::Start(e) if start_eq(&e, "subScript") => stack.push(Span::subscript()),
                Event::End(e) if end_eq(&e, "subScript") => stack.commit_top(),

                // ignore tags that appear to be an error from format scanning
                // example: EF7587ECFFE9FD39FF464EB61360F9BD.xml
                Event::Start(e) if start_eq(&e, "collectingCountry") => continue,
                Event::End(e) if end_eq(&e, "collectingCountry") => continue,

                // example: 2F4D87AFF92EFF845A97B4918263A116.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => continue,
                Event::End(e) if end_eq(&e, "collectingRegion") => continue,

                // example: 8126F14B207BFFD8459E6E740D71FE49.xml
                Event::Start(e) if start_eq(&e, "number") => continue,
                Event::End(e) if end_eq(&e, "number") => continue,

                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "tnCandidate") => continue,
                Event::End(e) if end_eq(&e, "tnCandidate") => continue,

                // example: 2125D91F1B36296D7ED9C253F0C7F83F.xml
                Event::Start(e) if start_eq(&e, "date") => continue,
                Event::End(e) if end_eq(&e, "date") => continue,

                // example: F95287BA7F01AD62F1FE3A6C504FFF52.xml
                Event::Start(e) if start_eq(&e, "year") => stack.push(Span::year()),
                Event::End(e) if end_eq(&e, "year") => stack.commit_top(),

                // example: F95287BA7F01AD62F1FE3A6C504FFF52.xml
                Event::Start(e) if start_eq(&e, "author") => continue,
                Event::End(e) if end_eq(&e, "author") => continue,

                // example: 4E4F5B3FFFB9871AFF75CAD8FC69F44D.xml
                Event::Start(e) if start_eq(&e, "specimenCount") => continue,
                Event::End(e) if end_eq(&e, "specimenCount") => continue,

                // example: 4EA4CE0CE793C1B1C697E80A0E92743C.xml
                Event::Start(e) if start_eq(&e, "authority") => {
                    let authority = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&authority.value));
                }

                // example: EF3540029A44FFDBFCEEFCD4FB71AF6D.xml
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _section = SubSection::parse(reader, &e)?;
                }

                // example: EF3540029A4AFFD5FF54FC2CFE13AF2E.xml
                Event::Start(e) if start_eq(&e, "quantity") => {
                    let _quantity = Quantity::parse(reader, &e)?;
                }

                // example: 2125D91F1B36296D7ED9C253F0C7F83F.xml
                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    let _ = TaxonomicName::parse(reader, &e)?;
                }

                // example: 465D8796FF8D9431E5A450CFFD29FA91.xml
                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    let _ = MaterialsCitation::parse(reader, &e)?;
                }

                // example: 4C1F8782FFDCE222FF5E631AFD3BF8B8.xml
                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let (attrs, children) = Citation::parse(reader, &e)?;
                    stack.push(Span::citation(attrs, children));
                }

                // example: 436E87B5BE715557FF67FA66FE384CF8.xml
                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => stack.push(Span::taxonomic_name()),
                Event::End(e) if end_eq(&e, "taxonomicNameLabel") => stack.commit_top(),

                // format leakage it seems
                // example: 2910754FA809272FFF146BC58230F81F.xml
                Event::Start(e) if start_eq(&e, "tr") => stack.push(Span::tr()),
                Event::End(e) if end_eq(&e, "tr") => stack.commit_top(),
                Event::Start(e) if start_eq(&e, "td") => stack.push(Span::td()),
                Event::End(e) if end_eq(&e, "td") => stack.commit_top(),

                Event::Text(txt) => {
                    let text = txt.unescape()?.into_owned();
                    stack.push(Span::text(&text));
                }
                Event::End(e) if end_eq(&e, "bibRefCitation") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((
            Citation {
                id: parse_attribute_opt(reader, event, "id")?,
                author: parse_attribute_opt(reader, event, "author")?,
                reference_id: parse_attribute_opt(reader, event, "refId")?,
                reference: parse_attribute_opt(reader, event, "refString")?,
                classification: parse_attribute_string_opt(reader, event, "type")?,
                year: parse_attribute_string_opt(reader, event, "year")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}


#[derive(Debug)]
pub struct BibCitation {
    pub id: Option<String>,
    pub author: Option<String>,
    pub volume: Option<String>,
    pub journal: Option<String>,
    pub issue: Option<String>,
    pub year: Option<usize>,
}

impl<T: BufRead> ParseFormat<T> for BibCitation {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // example: 2F0B59336E14301B1509F9C7E39FFE65.xml
                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),

                // example: 42443B2CFFEEFF8BFE8BFC480E0B9495.xml
                Event::Start(e) if start_eq(&e, "title") => stack.push(Span::title()),
                Event::End(e) if end_eq(&e, "title") => stack.commit_top(),

                // example: D512126ADB21FFB12C4ADCBDFB14066B.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {}
                Event::End(e) if end_eq(&e, "collectingRegion") => {}

                // example: D512126ADB0DFF9D2C41DD40FE1405E2.xml
                Event::Start(e) if start_eq(&e, "collectingCountry") => {}
                Event::End(e) if end_eq(&e, "collectingCountry") => {}

                // example: D512126ADB15FF862DB4D28FF8E60D05.xml
                Event::Start(e) if start_eq(&e, "quantity") => {}
                Event::End(e) if end_eq(&e, "quantity") => {}

                // example: 42443B2CFFEEFF8BFE8BFC480E0B9495.xml
                Event::Start(e) if start_eq(&e, "author") => {}
                Event::End(e) if end_eq(&e, "author") => {}

                // example: 42443B2CFFEEFF8BFE8BFC480E0B9495.xml
                Event::Start(e) if start_eq(&e, "part") => {}
                Event::End(e) if end_eq(&e, "part") => {}

                // example: 42443B2CFFEEFF8BFE8BFC480E0B9495.xml
                Event::Start(e) if start_eq(&e, "pagination") => {}
                Event::End(e) if end_eq(&e, "pagination") => {}

                // example: 42443B2CFFEEFF8BFE8BFC480E0B9495.xml
                Event::Start(e) if start_eq(&e, "year") => {}
                Event::End(e) if end_eq(&e, "year") => {}

                // example: 42443B2CFFECFF89FE9AF7060AEC99A4.xml
                Event::Start(e) if start_eq(&e, "journalOrPublisher") => {}
                Event::End(e) if end_eq(&e, "journalOrPublisher") => {}

                // example: EF160F442723FFD81658FA47FB77FDB7.xml
                Event::Start(e) if start_eq(&e, "bibCitation") => {
                    let (attrs, children) = BibCitation::parse(reader, &e)?;
                    stack.push(Span::bib_citation(attrs, children));
                }

                // example: 2F0B59336E14301B1509F9C7E39FFE65.xml
                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let (attrs, children) = Citation::parse(reader, &e)?;
                    stack.push(Span::citation(attrs, children));
                }

                Event::Text(txt) => {
                    let text = txt.unescape()?.into_owned();
                    stack.push(Span::text(&text));
                }
                Event::End(e) if end_eq(&e, "bibCitation") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((
            BibCitation {
                id: parse_attribute_opt(reader, event, "id")?,
                author: parse_attribute_opt(reader, event, "author")?,
                volume: parse_attribute_opt(reader, event, "volume")?,
                journal: parse_attribute_opt(reader, event, "journal")?,
                issue: parse_attribute_opt(reader, event, "issue")?,
                year: parse_attribute_string_opt(reader, event, "year")?,
            },
            stack.commit_and_pop_all(),
        ))
    }
}


#[derive(Debug)]
pub struct BibRef;

impl<T: BufRead> ParseFormat<T> for BibRef {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<(Self, Vec<Span>), Error> {
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "title") => stack.push(Span::title()),
                Event::End(e) if end_eq(&e, "title") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),

                // TODO: process this
                // example: 326F0056A10462291A87FF19413D422D.xml
                Event::Start(e) if start_eq(&e, "journalOrPublisher") => stack.push(Span::journal_or_publisher()),
                Event::End(e) if end_eq(&e, "journalOrPublisher") => stack.commit_top(),

                // example: 427E87F1A11E0515FF3F67BDFE913C06.xml
                Event::Start(e) if start_eq(&e, "author") => {}
                Event::End(e) if end_eq(&e, "author") => {}

                // example: 427E87F1A11E0515FF3F67BDFE913C06.xml
                Event::Start(e) if start_eq(&e, "editor") => {}
                Event::End(e) if end_eq(&e, "editor") => {}

                // example: 427E87F1A11E0515FF3F67BDFE913C06.xml
                Event::Start(e) if start_eq(&e, "volumeTitle") => {}
                Event::End(e) if end_eq(&e, "volumeTitle") => {}

                // example: 427E87F1A11E0515FF6362BDFE453906.xml
                Event::Start(e) if start_eq(&e, "publicationUrl") => {}
                Event::End(e) if end_eq(&e, "publicationUrl") => {}

                // example: 427E87F1A11E0515FE8C651DFD423B86.xml
                Event::Start(e) if start_eq(&e, "bookContentInfo") => {}
                Event::End(e) if end_eq(&e, "bookContentInfo") => {}

                // example: 326F0056A10462291A87FF19413D422D.xml
                Event::Start(e) if start_eq(&e, "year") => stack.push(Span::year()),
                Event::End(e) if end_eq(&e, "year") => stack.commit_top(),

                // example: 326F0056A10462291A87FF19413D422D.xml
                Event::Start(e) if start_eq(&e, "part") => stack.push(Span::part()),
                Event::End(e) if end_eq(&e, "part") => stack.commit_top(),

                // example: 326F0056A10462291A87FF19413D422D.xml
                Event::Start(e) if start_eq(&e, "pagination") => stack.push(Span::pagination()),
                Event::End(e) if end_eq(&e, "pagination") => stack.commit_top(),

                // example: D02201375E04D10F35AD84FA802C39A2.xml
                Event::Start(e) if start_eq(&e, "materialsCitation") => stack.push(Span::materials_citation()),
                Event::End(e) if end_eq(&e, "materialsCitation") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "authority") => {
                    let authority = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&authority.value));
                }

                Event::Start(e) if start_eq(&e, "typeStatus") => {
                    let (attrs, children) = TypeStatus::parse(reader, &e)?;
                    stack.push(Span::type_status(attrs, children));
                }

                Event::Start(e) if start_eq(&e, "collectingCountry") => {
                    let (attrs, children) = CollectingCountry::parse(reader, &e)?;
                    stack.push(Span::collecting_country(attrs, children));
                }

                // example: D02201375E04D10F35AD84FA802C39A2.xml
                Event::Start(e) if start_eq(&e, "collectorName") => {}
                Event::End(e) if end_eq(&e, "collectorName") => {}

                // example: F95287BA7F01AD62F1FE3A6C504FFF52.xml
                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let (attrs, children) = Citation::parse(reader, &e)?;
                    stack.push(Span::citation(attrs, children));
                }

                Event::Text(txt) => {
                    let text = txt.unescape()?.into_owned();
                    stack.push(Span::Text(text));
                }
                Event::End(e) if end_eq(&e, "bibRef") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok((BibRef, stack.commit_and_pop_all()))
    }
}

use super::formatting::prelude::*;
use super::parsing::prelude::*;
use super::prelude::*;


#[derive(Debug)]
pub struct Nomenclature {
    pub page_number: Option<i32>,
    pub taxon: Option<TaxonomicName>,
    pub taxon_label: Option<String>,
}


impl<T: BufRead> ParseSection<T> for Nomenclature {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut taxon = None;
        let mut taxon_label = None;

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

                // example: 43368780FA42184FBA83FEA4FB67F976.xml
                Event::Start(e) if start_eq(&e, "subScript") => stack.push(Span::subscript()),
                Event::End(e) if end_eq(&e, "subScript") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "keyLead") => stack.push(Span::key_lead()),
                Event::End(e) if end_eq(&e, "keyLead") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "keyStep") => stack.push(Span::key_step()),
                Event::End(e) if end_eq(&e, "keyStep") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "number") => stack.push(Span::number()),
                Event::End(e) if end_eq(&e, "number") => stack.commit_top(),

                // example: D076D419B54AFF8AFF23F04D24C3FD4F.xml
                Event::Start(e) if start_eq(&e, "determinerName") => stack.push(Span::determiner_name()),
                Event::End(e) if end_eq(&e, "determinerName") => stack.commit_top(),

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

                // Event::Start(e) if start_eq(&e, "materialsCitation") => stack.push(Span::materials_citation()),
                // Event::End(e) if end_eq(&e, "materialsCitation") => stack.commit_top(),
                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    // TODO: include citations in nomenclature block
                    let _cit = MaterialsCitation::parse(reader, &e)?;
                }
                Event::Start(e) if start_eq(&e, "pageStartToken") => {
                    let token = PageStartToken::parse(reader, &e)?;
                    stack.push(Span::page_start_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "pageBreakToken") => {
                    let (attrs, children) = PageBreakToken::parse(reader, &e)?;
                    stack.push(Span::page_break_token(attrs, children));
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
                    taxon = Some(TaxonomicName::parse(reader, &e)?);
                }

                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    let label = TaxonomicNameLabel::parse(reader, &e)?;
                    taxon_label = Some(label.value);
                }

                Event::Start(e) if start_eq(&e, "authorityName") => {
                    let authority = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&authority.value));
                }

                // example: 4C5032F3D49D7D095003B51C407CBD47.xml
                Event::Start(e) if start_eq(&e, "authority") => {
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

                Event::Start(e) if start_eq(&e, "specimenCode") => {
                    let span = SpecimenCode::parse(reader, &e)?;
                    stack.push(Span::SpecimenCode(span));
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

                // example: 1577035E8033FF90EDFCCBEE1C6B080C.xml
                Event::Start(e) if start_eq(&e, "vernacularName") => {}
                Event::End(e) if end_eq(&e, "vernacularName") => {}

                // example: 1577035E80ACFF0FEDFCCC6E18CA0DFC.xml
                Event::Start(e) if start_eq(&e, "locationDeviation") => {}
                Event::End(e) if end_eq(&e, "locationDeviation") => {}

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

        Ok(Nomenclature {
            page_number: parse_attribute_string_opt(reader, event, "pageNumber")?,
            taxon,
            taxon_label,
        })
    }
}

use super::formatting::prelude::*;
use super::parsing::prelude::*;
use super::prelude::*;
use crate::utils::titleize_first_word;


#[derive(Debug)]
pub struct TaxonomicName {
    pub id: Option<String>,

    pub authorship: Option<String>,
    pub authority_name: Option<String>,
    pub authority_year: Option<usize>,
    pub base_authority_name: Option<String>,
    pub base_authority_year: Option<String>,

    pub rank: Option<String>,
    pub status: Option<String>,
    pub kingdom: Option<String>,
    pub phylum: Option<String>,
    pub class: Option<String>,
    pub family: Option<String>,
    pub order: Option<String>,
    pub genus: Option<String>,
    pub subgenus: Option<String>,
    pub species: Option<String>,
    pub subspecies: Option<String>,

    // pub canonical_name: String,
    pub name: Span,
    pub taxon_label: Option<TaxonomicNameLabel>,
}

#[derive(Debug)]
pub struct TaxonomicNameLabel {
    pub value: String,
}


impl TaxonomicName {
    pub fn canonical_name(&self) -> String {
        let name = match self.rank.as_deref() {
            Some("genus") => self.genus.clone().unwrap_or_default().to_string(),
            Some("species") => {
                let genus = self.genus.clone().unwrap_or_default();
                let specific_epithet = self.species.clone().unwrap_or_default();
                let subspecific_epithet = self.subspecies.clone().unwrap_or_default();

                let name = match &self.subgenus {
                    Some(subgenus) => format!("{genus} ({subgenus}) {specific_epithet} {subspecific_epithet}"),
                    None => format!("{genus} {specific_epithet} {subspecific_epithet}"),
                };
                name.trim().to_string()
            }
            _ => self.name.to_string(),
        };

        titleize_first_word(&name.to_lowercase())
    }

    pub fn scientific_name_authority(&self) -> Option<String> {
        let auth = match (&self.authority_name, &self.authority_year) {
            (Some(name), Some(year)) => Some(format!("{name} {year}")),
            (Some(name), None) => Some(name.clone()),
            _ => None,
        };

        let base = match (&self.base_authority_name, &self.base_authority_year) {
            (Some(name), Some(year)) => Some(format!("{name} {year}")),
            (Some(name), None) => Some(name.clone()),
            _ => None,
        };

        match (auth, base) {
            (Some(auth), Some(base)) => Some(format!("({base}) {auth}")),
            (Some(auth), None) => Some(auth),
            (None, Some(base)) => Some(format!("({base})")),
            (None, None) => None,
        }
    }

    pub fn scientific_name(&self) -> String {
        let mut name = self.canonical_name();
        let authority = self.scientific_name_authority().unwrap_or_default();

        // if the document specifies the authorship then use it, otherwise use the
        // derived authority details
        let authorship = self.authorship.as_ref().unwrap_or(&authority);
        name = format!("{name} {}", authorship).trim().to_string();
        name
    }
}


impl<T: BufRead> ParseSection<T> for TaxonomicName {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut taxon_label = None;
        let mut stack = SpanStack::new();
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(e) if start_eq(&e, "paragraph") => stack.push(Span::paragraph()),
                Event::End(e) if end_eq(&e, "paragraph") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "emphasis") => stack.push(Span::emphasis()),
                Event::End(e) if end_eq(&e, "emphasis") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "title") => stack.push(Span::title()),
                Event::End(e) if end_eq(&e, "title") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "heading") => stack.push(Span::heading()),
                Event::End(e) if end_eq(&e, "heading") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "smallCapsWord") => stack.push(Span::small_caps()),
                Event::End(e) if end_eq(&e, "smallCapsWord") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "year") => stack.push(Span::year()),
                Event::End(e) if end_eq(&e, "year") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "authority") => {
                    let auth = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&auth.value));
                }
                Event::End(e) if end_eq(&e, "authority") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "taxonNameAuthority") => stack.push(Span::taxon_name_authority()),
                Event::End(e) if end_eq(&e, "taxonNameAuthority") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "journalOrPublisher") => stack.push(Span::journal_or_publisher()),
                Event::End(e) if end_eq(&e, "journalOrPublisher") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    let label = TaxonomicNameLabel::parse(reader, &e)?;
                    taxon_label = Some(label);
                }

                // TODO: include this in the tree as it can contain information
                // on varieties
                // example: 1CB6C95AA98C4964F3B70B2F58E3641D.xml
                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    let _ = TaxonomicName::parse(reader, &e)?;
                }

                Event::Start(e) if start_eq(&e, "authorityName") => {
                    let auth = Authority::parse(reader, &e)?;
                    stack.push(Span::authority(&auth.value));
                }
                Event::End(e) if end_eq(&e, "authorityName") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "author") => stack.push(Span::author()),
                Event::End(e) if end_eq(&e, "author") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "normalizedToken") => {
                    let token = NormalizedToken::parse(reader, &e)?;
                    stack.push(Span::normalized_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "pageStartToken") => {
                    let token = PageStartToken::parse(reader, &e)?;
                    stack.push(Span::page_start_token(&token.value));
                }

                Event::Start(e) if start_eq(&e, "pageBreakToken") => {
                    let (token, children) = PageBreakToken::parse(reader, &e)?;
                    stack.push(Span::page_break_token(token, children));
                }

                Event::Start(e) if start_eq(&e, "treatmentCitation") => stack.push(Span::treatment_citation()),
                Event::End(e) if end_eq(&e, "treatmentCitation") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "materialsCitation") => stack.push(Span::materials_citation()),
                Event::End(e) if end_eq(&e, "materialsCitation") => stack.commit_top(),

                Event::Start(e) if start_eq(&e, "figureCitation") => stack.push(Span::figure_citation()),
                Event::End(e) if end_eq(&e, "figureCitation") => stack.commit_top(),

                Event::Text(txt) => {
                    let text = Some(txt.unescape()?.into_owned());
                    if let Some(text) = &text {
                        stack.push(Span::text(text));
                    }
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

                // possible format scanning issues
                // example: EF03B66BB047FFD10EBEF8BCA576FD6B.xml
                Event::Start(e) if start_eq(&e, "collectingCountry") => {}
                Event::End(e) if end_eq(&e, "collectingCountry") => {}

                // example: 2F4D87AFF928FF825A97B1A081FEA6C0.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {}
                Event::End(e) if end_eq(&e, "collectingRegion") => {}

                // example: 32568792FFC7FFC64686639CFECE9977.xml
                Event::Start(e) if start_eq(&e, "collectionCode") => {}
                Event::End(e) if end_eq(&e, "collectionCode") => {}

                // example: A15F5D59163C3936FF7CFDDE5B5EFE3B.xml
                Event::Start(e) if start_eq(&e, "collectorName") => {}
                Event::End(e) if end_eq(&e, "collectorName") => {}

                // example: 325587EA8A6FFFBCFFDA408FFA74FB90.xml
                Event::Start(e) if start_eq(&e, "quantity") => {}
                Event::End(e) if end_eq(&e, "quantity") => {}

                // example: 8126F14B207FFFDF459E6D990D42F82E.xml
                Event::Start(e) if start_eq(&e, "number") => {}
                Event::End(e) if end_eq(&e, "number") => {}

                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "tnCandidate") => {}
                Event::End(e) if end_eq(&e, "tnCandidate") => {}

                // example: A14087A9FFFBFF88FEDB738727F6BB33.xml
                Event::Start(e) if start_eq(&e, "pageNumber") => {}
                Event::End(e) if end_eq(&e, "pageNumber") => {}

                // example: D53CF518FFAFFFA7FE0C32884CA2F733.xml
                Event::Start(e) if start_eq(&e, "pageTitle") => {}
                Event::End(e) if end_eq(&e, "pageTitle") => {}

                // example: D55C878B992EB524FF0D37CF5D1E14A0.xml
                Event::Start(e) if start_eq(&e, "td") => {}
                Event::End(e) if end_eq(&e, "td") => {}

                // example: F70587F2FFECF8190DC26CDFFE2AAB2E.xml
                Event::Start(e) if start_eq(&e, "th") => {}
                Event::End(e) if end_eq(&e, "th") => {}

                // example: 465E87E9BD08FFFA25EAFDA2FDB79A71.xml
                Event::Start(e) if start_eq(&e, "tr") => {}
                Event::End(e) if end_eq(&e, "tr") => {}

                // example: 152AD429FFDEFFCC9AFD95B1352CFE6F.xml
                Event::Start(e) if start_eq(&e, "location") => {}
                Event::End(e) if end_eq(&e, "location") => {}

                // TODO: does this actually work? this example has a java debug output
                // example: 81B1464F9C1A5277DCFE40B791148055.xml
                Event::Start(e) if start_eq(&e, "misspelling") => {}
                Event::End(e) if end_eq(&e, "misspelling") => {}

                // example: 1C528797FF94FFBC6AA492C181612CAC.xml
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let _ = SubSection::parse(reader, &e)?;
                }

                // example: 2F6F736E1D37FF92FF079CBAFDFC9E2D.xml
                Event::Start(e) if start_eq(&e, "typeStatus") => {
                    let (attrs, children) = TypeStatus::parse(reader, &e)?;
                    stack.push(Span::type_status(attrs, children));
                }

                Event::End(e) if end_eq(&e, "taxonomicName") => {
                    stack.commit_top();
                    break;
                }
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(TaxonomicName {
            id: parse_attribute_opt(reader, event, "id")?,
            authorship: parse_attribute_opt(reader, event, "authority")?,
            authority_name: parse_attribute_opt(reader, event, "authorityName")?,
            authority_year: parse_attribute_string_opt(reader, event, "authorityYear")?,
            base_authority_name: parse_attribute_opt(reader, event, "baseAuthorityName")?,
            base_authority_year: parse_attribute_opt(reader, event, "baseAuthorityYear")?,
            rank: parse_attribute_opt(reader, event, "rank")?,
            status: parse_attribute_opt(reader, event, "status")?,
            kingdom: parse_attribute_opt(reader, event, "kingdom")?,
            phylum: parse_attribute_opt(reader, event, "phylum")?,
            class: parse_attribute_opt(reader, event, "class")?,
            family: parse_attribute_opt(reader, event, "family")?,
            order: parse_attribute_opt(reader, event, "order")?,
            genus: parse_attribute_opt(reader, event, "genus")?,
            subgenus: parse_attribute_opt(reader, event, "subGenus")?,
            species: parse_attribute_opt(reader, event, "species")?,
            subspecies: parse_attribute_opt(reader, event, "subSpecies")?,
            name: unwrap_element(stack.pop(), "text")?,
            taxon_label,
        })
    }
}

impl<T: BufRead> ParseSection<T> for TaxonomicNameLabel {
    fn parse(reader: &mut Reader<T>, _event: &BytesStart) -> Result<Self, Error> {
        let mut value = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                // TODO: allow formatting in label value
                // example: 2F5D87EBFF96924FFF1305023432F993.xml
                Event::Start(e) if start_eq(&e, "emphasis") => {}
                Event::End(e) if end_eq(&e, "emphasis") => {}

                // formatting issues
                // example: 362787948A230C6DFF0AFA90503A8E23.xml
                Event::Start(e) if start_eq(&e, "date") => {}
                Event::End(e) if end_eq(&e, "date") => {}

                // example: 282F878EFFA5FFB614C02171FD565F68.xml
                Event::Start(e) if start_eq(&e, "th") => {}
                Event::End(e) if end_eq(&e, "th") => {}

                // example: A1E143B3EDA2CDB64348FE3821AD81A2.xml
                Event::Start(e) if start_eq(&e, "pageBreakToken") => {}
                Event::End(e) if end_eq(&e, "pageBreakToken") => {}

                // example: 28D2FB482F68DE0E7E9C17B4DF682C7B.xml
                Event::Start(e) if start_eq(&e, "authority") => {}
                Event::End(e) if end_eq(&e, "authority") => {}

                // example: F71F87A2FFBAFF936DCD9190FC0F56CE.xml
                Event::Start(e) if start_eq(&e, "taxonomicName") => {}
                Event::End(e) if end_eq(&e, "taxonomicName") => {}

                // example: E3F707E313B1D27B55EB2471F5909DA1.xml
                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    let label = TaxonomicNameLabel::parse(reader, &e)?;
                    value = Some(label.value);
                }

                Event::Text(txt) => value = Some(txt.unescape()?.into_owned()),
                Event::End(e) if end_eq(&e, "taxonomicNameLabel") => break,
                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(TaxonomicNameLabel {
            value: unwrap_element(value, "taxonomicNameLabel")?,
        })
    }
}

use super::formatting::prelude::*;
use super::parsing::prelude::*;
use super::prelude::*;


#[derive(Debug)]
pub struct Treatment {
    pub lsid: String,
    pub http_uri: String,
    pub sections: Vec<Section>,
}

impl<T: BufRead> ParseSection<T> for Treatment {
    fn parse(reader: &mut Reader<T>, event: &BytesStart) -> Result<Self, Error> {
        let mut sections = Vec::new();

        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::End(e) if end_eq(&e, "treatment") => break,

                Event::Start(e) if start_eq(&e, "subSection") => {
                    let subsection = SubSection::parse(reader, &e)?;
                    sections.push(subsection.section);
                }
                Event::Start(e) if start_eq(&e, "subSubSection") => {
                    let subsection = SubSection::parse(reader, &e)?;
                    sections.push(subsection.section);
                }

                // ignore captions
                Event::Start(e) if start_eq(&e, "caption") => {
                    let _caption = Caption::parse(reader, &e)?;
                }

                // formatting elements wrapping subsections. we want to unwrap these and ignore the formatting.
                // by continuing with the loop we basically pretend it doesn't exist
                Event::Start(e) if start_eq(&e, "title") => continue,
                Event::End(e) if end_eq(&e, "title") => continue,

                // example: EF160F44273BFFD31658FB0EFE3EFA4C.xml
                Event::Start(e) if start_eq(&e, "heading") => continue,
                Event::End(e) if end_eq(&e, "heading") => continue,

                // example: EF3E87CA7D28EE55FAFA7B754F62FAE1.xml
                Event::Start(e) if start_eq(&e, "emphasis") => continue,
                Event::End(e) if end_eq(&e, "emphasis") => continue,

                // example: 8C5287D6FF9F163CF0BFFD2DFD2B234B.xml
                Event::Start(e) if start_eq(&e, "treatmentHeading") => continue,
                Event::End(e) if end_eq(&e, "treatmentHeading") => continue,

                // example: EF51B220FFD2FFFDFF24FDB01FDDF821.xml
                Event::Start(e) if start_eq(&e, "treatmentCitationGroup") => continue,
                Event::End(e) if end_eq(&e, "treatmentCitationGroup") => continue,

                // example: 322B187EB4030737E00EC9CEFF49FE7C.xml
                Event::Start(e) if start_eq(&e, "treatmentCitation") => continue,
                Event::End(e) if end_eq(&e, "treatmentCitation") => continue,

                // example: EF3540029A4EFFD1FCEEF926FA67AC57.xml
                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    let _cit = MaterialsCitation::parse(reader, &e)?;
                }

                // example: 427E87F1A11E0515FF3F67BDFE913C06.xml
                Event::Start(e) if start_eq(&e, "bibRef") => {
                    let _ = BibRef::parse(reader, &e)?;
                }

                // example: EF3E87CA7D28EE55FAFA7B754F62FAE1.xml
                Event::Start(e) if start_eq(&e, "collectingRegion") => {
                    let _ = CollectingRegion::parse(reader, &e)?;
                }

                // example: 813A87B0FFF7693FFF73689BD1D0AAC9.xml
                Event::Start(e) if start_eq(&e, "collectingCountry") => {
                    let _ = CollectingCountry::parse(reader, &e)?;
                }

                // example: 9C548790FFEEFFAFFCC9447EFC8FFE92.xml
                Event::Start(e) if start_eq(&e, "collectingMunicipality") => continue,
                Event::End(e) if end_eq(&e, "collectingMunicipality") => continue,

                // example: 9C548790FFEEFFAFFCC9447EFC8FFE92.xml
                Event::Start(e) if start_eq(&e, "collectingCounty") => continue,
                Event::End(e) if end_eq(&e, "collectingCounty") => continue,

                // example: EF3E87CA7D28EE55FAFA7B754F62FAE1.xml
                Event::Start(e) if start_eq(&e, "bibRefCitation") => {
                    let _ = Citation::parse(reader, &e)?;
                }

                // example: 2F2FE66B9402FFDA55F8B8821A2FFCD6.xml
                Event::Start(e) if start_eq(&e, "figureCitation") => continue,
                Event::End(e) if end_eq(&e, "figureCitation") => continue,

                // example: 1C528797FF91FFB66AA4958E84CC2FB5.xml
                Event::Start(e) if start_eq(&e, "tableCitation") => continue,
                Event::End(e) if end_eq(&e, "tableCitation") => continue,

                // example: 465D8796FF8D9431E5A450CFFD29FA91.xml
                Event::Start(e) if start_eq(&e, "materialsCitation") => {
                    let _ = MaterialsCitation::parse(reader, &e)?;
                }

                // example: EF0787806241345B052DF9D6FD7555B9.xml
                Event::Start(e) if start_eq(&e, "paragraph") => continue,
                Event::End(e) if end_eq(&e, "paragraph") => continue,

                // TODO: we should really include this so that names are properly unicode
                // example: EF8916089F64C48AA35BD3B9EF64FA27.xml
                Event::Start(e) if start_eq(&e, "normalizedToken") => continue,
                Event::End(e) if end_eq(&e, "normalizedToken") => continue,

                // TODO: determine the significance of this element
                // example: 2FAF6637D84E52BD655BBF005CD15CC0.xml
                Event::Start(e) if start_eq(&e, "t_e_m_p") => continue,
                Event::End(e) if end_eq(&e, "t_e_m_p") => continue,

                // example: 294D4935FFA25F35FD6481C9FB6EFEAB.xml
                Event::Start(e) if start_eq(&e, "key") => continue,
                Event::End(e) if end_eq(&e, "key") => continue,

                // example: 81094C75FFA7FFD1FF59FB30FA328EAB.xml
                Event::Start(e) if start_eq(&e, "keyStep") => continue,
                Event::End(e) if end_eq(&e, "keyStep") => continue,

                // example: EF41F251FF84FFE7C1D95612FA5FFF16.xml
                // example: 294D4935FFA25F35FD6481C9FB6EFEAB.xml
                Event::Start(e) if start_eq(&e, "keyLead") => continue,
                Event::End(e) if end_eq(&e, "keyLead") => continue,

                // example: EF7587ECFFEDFD3BFF46495F101CFE19.xml
                Event::Start(e) if start_eq(&e, "tableNote") => {
                    let _table_note = TableNote::parse(reader, &e);
                }

                // example: EF4C87F8FFA9FFD0FF78EC2DFDF4607E.xml
                Event::Start(e) if start_eq(&e, "table") => {
                    let _table = Table::parse(reader, &e);
                }

                // example: EF19F029890BFFE2FF28FC7CE1C49A3E.xml
                Event::Start(e) if start_eq(&e, "footnote") => {
                    let _ = Footnote::parse(reader, &e);
                }

                // example: 2F489243A56BFFEDD3DAF88AB1FBF996.xml
                Event::Start(e) if start_eq(&e, "typeStatus") => {
                    let _ = TypeStatus::parse(reader, &e);
                }

                // example: 323C84B0F3202350D570A92384BF9F31.xml
                Event::Start(e) if start_eq(&e, "tr") => continue,
                Event::End(e) if end_eq(&e, "tr") => continue,

                // example: 815D710FFFAAFFFCA5A63657FCD9FD73.xml
                Event::Start(e) if start_eq(&e, "taxonomicName") => {
                    let _ = TaxonomicName::parse(reader, &e)?;
                }

                // example: 1C528797FF91FFB66AA4958E84CC2FB5.xml
                Event::Start(e) if start_eq(&e, "taxonomicNameLabel") => {
                    let _ = TaxonomicNameLabel::parse(reader, &e)?;
                }

                // example: 815D710FFFA2FFF5A5A631BAFE15FC53.xml
                Event::Start(e) if start_eq(&e, "quantity") => {
                    let _ = Quantity::parse(reader, &e)?;
                }

                // example: 813A87B0FFF7693FFF73689BD1D0AAC9.xml
                Event::Start(e) if start_eq(&e, "specimenCount") => continue,
                Event::End(e) if end_eq(&e, "specimenCount") => continue,

                // example: 9C548790FFEEFFAFFCC9447EFC8FFE92.xml
                Event::Start(e) if start_eq(&e, "location") => continue,
                Event::End(e) if end_eq(&e, "location") => continue,

                // example: 5CD9C3023BD8A33564093E0F65448904.xml
                Event::Start(e) if start_eq(&e, "pageBreakToken") => continue,
                Event::End(e) if end_eq(&e, "pageBreakToken") => continue,

                // example: 9C548790FFEEFFAFFCC9447EFC8FFE92.xml
                Event::Start(e) if start_eq(&e, "date") => {
                    Date::parse(reader, &e)?;
                }

                // example: 9C548790FFEEFFAFFCC9447EFC8FFE92.xml
                Event::Start(e) if start_eq(&e, "collectorName") => continue,
                Event::End(e) if end_eq(&e, "collectorName") => continue,

                // example: 5E5487FAFF85FFCCFCC6FAA58D68FE7B.xml
                Event::Start(e) if start_eq(&e, "collectionCode") => continue,
                Event::End(e) if end_eq(&e, "collectionCode") => continue,

                // example: 5E5487FAFF85FFCCFCC6FAA58D68FE7B.xml
                Event::Start(e) if start_eq(&e, "subScript") => continue,
                Event::End(e) if end_eq(&e, "subScript") => continue,

                // example: 2125D91F1B36296D7ED9C253F0C7F83F.xml
                Event::Start(e) if start_eq(&e, "docIdISSN") => continue,
                Event::Start(e) if start_eq(&e, "docID-ISSN") => continue,
                Event::End(e) if end_eq(&e, "docIdISSN") => continue,
                Event::End(e) if end_eq(&e, "docID-ISSN") => continue,

                // example: 42458787FFA4E5060CD6C805BDE50AD1.xml
                Event::Start(e) if start_eq(&e, "docAuthor") => continue,
                Event::End(e) if end_eq(&e, "docAuthor") => continue,

                // example: 42458787FFA4E5060CD6C805BDE50AD1.xml
                Event::Start(e) if start_eq(&e, "docAuthorAffiliation") => continue,
                Event::End(e) if end_eq(&e, "docAuthorAffiliation") => continue,

                // example: 42458787FFA4E5060CD6C805BDE50AD1.xml
                Event::Start(e) if start_eq(&e, "docAuthorEmail") => continue,
                Event::End(e) if end_eq(&e, "docAuthorEmail") => continue,

                // example: 42458787FFA4E5060CD6C805BDE50AD1.xml
                Event::Start(e) if start_eq(&e, "uri") => continue,
                Event::End(e) if end_eq(&e, "uri") => continue,

                // example: 42458787FFA4E5060CD6C805BDE50AD1.xml
                Event::Start(e) if start_eq(&e, "uuid") => continue,
                Event::End(e) if end_eq(&e, "uuid") => continue,

                // example: 1577035E80FBFF58EDFCCF2D1BF30FAC.xml
                Event::Start(e) if start_eq(&e, "elevation") => continue,
                Event::End(e) if end_eq(&e, "elevation") => continue,

                // example: 4E4F5B3FFFC58766FF75CC6FFC21F1DA.xml
                Event::Start(e) if start_eq(&e, "geoCoordinate") => continue,
                Event::End(e) if end_eq(&e, "geoCoordinate") => continue,

                // example: EF654433374BFFFC1F4C73A3FDF1FEDB.xml
                Event::Text(_e) => continue,

                event => panic!("Unknown element. event: {event:#?}"),
            }
        }

        Ok(Treatment {
            lsid: parse_attribute(reader, event, "LSID")?,
            http_uri: parse_attribute(reader, event, "httpUri")?,
            sections,
        })
    }
}

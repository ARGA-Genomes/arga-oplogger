use std::collections::HashMap;

use crate::errors::TransformError;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, PublicationField};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize, Hash, Eq, PartialEq)]
pub struct Publication {
    pub entity_id: Option<String>,
    pub title: Option<String>,
    pub authors: Option<String>,
    pub published_year: Option<String>,
    pub published_date: Option<String>,
    pub language: Option<String>,
    pub publisher: Option<String>,
    pub doi: Option<String>,
    pub publication_type: Option<String>,

    pub citation: Option<String>,
    pub source_url: Option<String>,
}


pub fn get_all(dataset: &Dataset) -> Result<Vec<Publication>, TransformError> {
    use rdf::Publication::*;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/collecting",
        "http://arga.org.au/schemas/maps/tsi/tissues",
        "http://arga.org.au/schemas/maps/tsi/registrations",
        "http://arga.org.au/schemas/maps/tsi/extractions",
    ];
    let graph = dataset.graph(&graphs);

    let data: HashMap<Literal, Vec<PublicationField>> = resolve_data(
        &graph,
        &[
            // Title,
            // Authors,
            // PublishedYear,
            // PublishedDate,
            // Language,
            // Publisher,
            // PublicationType,
            EntityId, Doi, Citation, SourceUrl,
        ],
    )?;

    let mut publications = Vec::new();
    for (_entity_id, fields) in data {
        let mut publication = Publication::default();

        for field in fields {
            match field {
                PublicationField::EntityId(val) => publication.entity_id = Some(val),
                PublicationField::Title(val) => publication.title = Some(val),
                PublicationField::Authors(val) => publication.authors = Some(val),
                PublicationField::PublishedYear(val) => publication.published_year = Some(val),
                PublicationField::PublishedDate(val) => publication.published_date = Some(val),
                PublicationField::Language(val) => publication.language = Some(val),
                PublicationField::Publisher(val) => publication.publisher = Some(val),
                PublicationField::Doi(val) => publication.doi = Some(val),
                PublicationField::PublicationType(val) => publication.publication_type = Some(val),
                PublicationField::Citation(val) => publication.citation = Some(val),
                PublicationField::SourceUrl(val) => publication.source_url = Some(val),
            }
        }

        publications.push(publication);
    }

    publications.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
    publications.dedup();

    Ok(publications)
}

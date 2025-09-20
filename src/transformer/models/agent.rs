use std::collections::HashMap;

use crate::errors::TransformError;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{Extraction, ExtractionField, Literal};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize, Hash, Eq, PartialEq)]
pub struct Agent {
    pub entity_id: String,
    pub full_name: String,
    pub orcid: Option<String>,
}


pub fn get_all(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let mut agents = get_extraction_agents(dataset)?;
    agents.extend(get_material_extraction_agents(dataset)?);
    agents.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
    agents.dedup();
    Ok(agents)
}

pub fn get_extraction_agents(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/extractions",
    ];
    let graph = dataset.graph(&graphs);

    let data: HashMap<Literal, Vec<ExtractionField>> = resolve_data(
        &graph,
        &[
            Extraction::ExtractedBy,
            Extraction::ExtractedByOrcid,
            Extraction::ExtractedByEntityId,
        ],
    )?;

    let mut agents = Vec::new();
    for (_entity_id, fields) in data {
        let mut agent = Agent {
            entity_id: "".to_string(),
            ..Default::default()
        };

        for field in fields {
            match field {
                ExtractionField::ExtractedBy(val) => agent.full_name = val,
                ExtractionField::ExtractedByOrcid(val) => agent.orcid = Some(val),
                ExtractionField::ExtractedByEntityId(val) => agent.entity_id = val,
                _ => {}
            }
        }

        agents.push(agent);
    }

    Ok(agents)
}


pub fn get_material_extraction_agents(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/extractions",
    ];
    let graph = dataset.graph(&graphs);

    let data: HashMap<Literal, Vec<ExtractionField>> = resolve_data(
        &graph,
        &[
            Extraction::MaterialExtractedBy,
            Extraction::MaterialExtractedByOrcid,
            Extraction::MaterialExtractedByEntityId,
        ],
    )?;

    let mut agents = Vec::new();
    for (entity_id, fields) in data {
        let Literal::String(entity_id) = entity_id;

        let mut agent = Agent::default();

        for field in fields {
            match field {
                ExtractionField::MaterialExtractedBy(val) => agent.full_name = val,
                ExtractionField::MaterialExtractedByOrcid(val) => agent.orcid = Some(val),
                ExtractionField::MaterialExtractedByEntityId(val) => agent.entity_id = val,
                _ => {}
            }
        }

        agents.push(agent);
    }

    Ok(agents)
}

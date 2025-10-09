use std::collections::HashMap;

use crate::errors::TransformError;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{
    DataProduct,
    DataProductField,
    Extraction,
    ExtractionField,
    Library,
    LibraryField,
    Literal,
};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize, Hash, Eq, PartialEq)]
pub struct Agent {
    pub entity_id: String,
    pub full_name: String,
    pub orcid: Option<String>,
}


pub fn get_all(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let mut agents = get_custodian_agents(dataset)?;
    agents.extend(get_extraction_agents(dataset)?);
    agents.extend(get_material_extraction_agents(dataset)?);
    agents.extend(get_prepared_agents(dataset)?);
    agents.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
    agents.dedup();
    Ok(agents)
}


pub fn get_custodian_agents(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let iris = dataset.scope(&["data_products"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);

    let data: HashMap<Literal, Vec<DataProductField>> = resolve_data(
        &graph,
        &[
            DataProduct::Custodian,
            DataProduct::CustodianOrcid,
            DataProduct::CustodianEntityId,
        ],
    )?;

    let mut agents = Vec::new();
    for (_idx, fields) in data {
        let mut agent = Agent::default();

        for field in fields {
            match field {
                DataProductField::Custodian(val) => agent.full_name = val,
                DataProductField::CustodianOrcid(val) => agent.orcid = Some(val),
                DataProductField::CustodianEntityId(val) => agent.entity_id = val,
                _ => {}
            }
        }

        agents.push(agent);
    }

    Ok(agents)
}


pub fn get_extraction_agents(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let iris = dataset.scope(&["extractions"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);

    let data: HashMap<Literal, Vec<ExtractionField>> = resolve_data(
        &graph,
        &[
            Extraction::ExtractedBy,
            Extraction::ExtractedByOrcid,
            Extraction::ExtractedByEntityId,
        ],
    )?;

    let mut agents = Vec::new();
    for (_idx, fields) in data {
        let mut agent = Agent::default();

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
    let iris = dataset.scope(&["extractions"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);

    let data: HashMap<Literal, Vec<ExtractionField>> = resolve_data(
        &graph,
        &[
            Extraction::MaterialExtractedBy,
            Extraction::MaterialExtractedByOrcid,
            Extraction::MaterialExtractedByEntityId,
        ],
    )?;

    let mut agents = Vec::new();
    for (_idx, fields) in data {
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


pub fn get_prepared_agents(dataset: &Dataset) -> Result<Vec<Agent>, TransformError> {
    let iris = dataset.scope(&["library"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);

    let data: HashMap<Literal, Vec<LibraryField>> =
        resolve_data(&graph, &[Library::PreparedBy, Library::PreparedByEntityId])?;

    let mut agents = Vec::new();
    for (_idx, fields) in data {
        let mut agent = Agent::default();

        for field in fields {
            match field {
                LibraryField::PreparedBy(val) => agent.full_name = val,
                LibraryField::PreparedByEntityId(val) => agent.entity_id = val,
                _ => {}
            }
        }

        agents.push(agent);
    }

    Ok(agents)
}

use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, ProjectField};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct Project {
    pub entity_id: String,
    pub project_id: Option<String>,

    pub scientific_name: Option<String>,
    pub initiative: Option<String>,
    pub initiative_theme: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub data_context: Option<String>,
    pub data_types: Option<String>,
    pub data_assay_types: Option<String>,
    pub partners: Option<String>,

    pub curator: Option<String>,
    pub curator_orcid: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Project>, Error> {
    use rdf::Project::*;

    let models = dataset.scope(&["project"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);

    info!("Resolving data");
    let data: HashMap<Literal, Vec<ProjectField>> = resolver.resolve(
        &[
            EntityId,
            ProjectId,
            ScientificName,
            Initiative,
            InitiativeTheme,
            Title,
            Description,
            DataContext,
            DataTypes,
            DataAssayTypes,
            Partners,
            Curator,
            CuratorOrcid,
        ],
        &scope,
    )?;


    let mut projects = Vec::new();

    for (_idx, fields) in data {
        let mut project = Project::default();

        for field in fields {
            match field {
                ProjectField::EntityId(val) => project.entity_id = val,
                ProjectField::ProjectId(val) => project.project_id = Some(val),
                ProjectField::ScientificName(val) => project.scientific_name = Some(val),
                ProjectField::Initiative(val) => project.initiative = Some(val),
                ProjectField::InitiativeTheme(val) => project.initiative_theme = Some(val),
                ProjectField::Title(val) => project.title = Some(val),
                ProjectField::Description(val) => project.description = Some(val),
                ProjectField::DataContext(val) => project.data_context = Some(val),
                ProjectField::DataTypes(val) => project.data_types = Some(val),
                ProjectField::DataAssayTypes(val) => project.data_assay_types = Some(val),
                ProjectField::Partners(val) => project.partners = Some(val),
                ProjectField::Curator(val) => project.curator = Some(val),
                ProjectField::CuratorOrcid(val) => project.curator_orcid = Some(val),
            }
        }

        projects.push(project);
    }

    Ok(projects)
}

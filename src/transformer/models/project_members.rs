use std::collections::HashMap;

use tracing::{info, instrument};

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, ProjectMemberField};
use crate::transformer::resolver::Resolver;


#[derive(Debug, Default, serde::Serialize)]
pub struct ProjectMember {
    pub entity_id: String,
    pub project_id: Option<String>,
    pub name: Option<String>,
    pub orcid: Option<String>,
    pub organisation: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<ProjectMember>, Error> {
    use rdf::ProjectMember::*;

    let models = dataset.scope(&["project_member"]);
    let mut scope = Vec::new();
    for model in models.iter() {
        scope.push(iref::Iri::new(model).unwrap());
    }

    let resolver = Resolver::new(dataset);

    info!("Resolving data");
    let data: HashMap<Literal, Vec<ProjectMemberField>> =
        resolver.resolve(&[EntityId, ProjectId, Name, Orcid, Organisation], &scope)?;


    let mut members = Vec::new();

    for (_idx, fields) in data {
        let mut member = ProjectMember::default();

        for field in fields {
            match field {
                ProjectMemberField::EntityId(val) => member.entity_id = val,
                ProjectMemberField::ProjectId(val) => member.project_id = Some(val),
                ProjectMemberField::Name(val) => member.name = Some(val),
                ProjectMemberField::Orcid(val) => member.orcid = Some(val),
                ProjectMemberField::Organisation(val) => member.organisation = Some(val),
            }
        }

        members.push(member);
    }

    Ok(members)
}

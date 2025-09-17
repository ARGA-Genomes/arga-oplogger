use std::collections::HashMap;

use tracing::instrument;

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, Literal, OrganismField};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize)]
pub struct Organism {
    pub entity_id: String,
    pub organism_id: Option<String>,
    pub scientific_name: Option<String>,
    pub sex: Option<String>,
    pub genotypic_sex: Option<String>,
    pub phenotypic_sex: Option<String>,
    pub life_stage: Option<String>,
    pub reproductive_condition: Option<String>,
    pub behavior: Option<String>,
    pub live_state: Option<String>,
    pub remarks: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<Organism>, Error> {
    use rdf::Organism::*;

    let graphs = vec![
        "http://arga.org.au/schemas/maps/tsi/",
        "http://arga.org.au/schemas/maps/tsi/organisms",
    ];
    let graph = dataset.graph(&graphs);

    let data: HashMap<Literal, Vec<OrganismField>> = resolve_data(
        &graph,
        &[
            OrganismId,
            ScientificName,
            Sex,
            GenotypicSex,
            PhenotypicSex,
            LifeStage,
            ReproductiveCondition,
            Behavior,
            LiveState,
            Remarks,
        ],
    )?;


    let mut records = Vec::new();

    for (entity_id, fields) in data {
        let Literal::String(entity_id) = entity_id;

        let mut record = Organism {
            entity_id,
            ..Default::default()
        };

        for field in fields {
            match field {
                OrganismField::OrganismId(val) => record.organism_id = Some(val),
                OrganismField::ScientificName(val) => record.scientific_name = Some(val),
                OrganismField::Sex(val) => record.sex = Some(val),
                OrganismField::GenotypicSex(val) => record.genotypic_sex = Some(val),
                OrganismField::PhenotypicSex(val) => record.phenotypic_sex = Some(val),
                OrganismField::LifeStage(val) => record.life_stage = Some(val),
                OrganismField::ReproductiveCondition(val) => record.reproductive_condition = Some(val),
                OrganismField::Behavior(val) => record.behavior = Some(val),
                OrganismField::LiveState(val) => record.live_state = Some(val),
                OrganismField::Remarks(val) => record.remarks = Some(val),
            }
        }

        records.push(record);
    }

    Ok(records)
}

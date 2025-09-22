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
    pub publication_id: Option<String>,
    pub curator_id: Option<String>,

    pub scientific_name: Option<String>,
    pub sex: Option<String>,
    pub genotypic_sex: Option<String>,
    pub phenotypic_sex: Option<String>,
    pub life_stage: Option<String>,
    pub reproductive_condition: Option<String>,
    pub behavior: Option<String>,
    pub live_state: Option<String>,
    pub remarks: Option<String>,

    pub identified_by: Option<String>,
    pub identification_date: Option<String>,
    pub disposition: Option<String>,
    pub first_observed_at: Option<String>,
    pub last_known_alive_at: Option<String>,

    pub biome: Option<String>,
    pub habitat: Option<String>,
    pub bioregion: Option<String>,
    pub ibra_imcra: Option<String>,

    pub latitude: Option<String>,
    pub longitude: Option<String>,
    pub coordinate_system: Option<String>,
    pub location_source: Option<String>,
    pub holding: Option<String>,
    pub holding_id: Option<String>,
    pub holding_permit: Option<String>,

    pub created_at: Option<String>,
    pub updated_at: Option<String>,
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
            IdentifiedBy,
            // IdentificationDate,
            Disposition,
            // FirstObservedAt,
            // LastKnownAliveAt,
            Biome,
            Habitat,
            Bioregion,
            IbraImcra,
            Latitude,
            Longitude,
            CoordinateSystem,
            LocationSource,
            Holding,
            HoldingId,
            HoldingPermit,
            // CreatedAt,
            // UpdatedAt,
            Doi,
            Citation,
            PublicationEntityId,
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
                OrganismField::IdentifiedBy(val) => record.identified_by = Some(val),
                OrganismField::IdentificationDate(val) => record.identification_date = Some(val),
                OrganismField::Disposition(val) => record.disposition = Some(val),
                OrganismField::FirstObservedAt(val) => record.first_observed_at = Some(val),
                OrganismField::LastKnownAliveAt(val) => record.last_known_alive_at = Some(val),
                OrganismField::Biome(val) => record.biome = Some(val),
                OrganismField::Habitat(val) => record.habitat = Some(val),
                OrganismField::Bioregion(val) => record.bioregion = Some(val),
                OrganismField::IbraImcra(val) => record.ibra_imcra = Some(val),
                OrganismField::Latitude(val) => record.latitude = Some(val),
                OrganismField::Longitude(val) => record.longitude = Some(val),
                OrganismField::CoordinateSystem(val) => record.coordinate_system = Some(val),
                OrganismField::LocationSource(val) => record.location_source = Some(val),
                OrganismField::Holding(val) => record.holding = Some(val),
                OrganismField::HoldingId(val) => record.holding_id = Some(val),
                OrganismField::HoldingPermit(val) => record.holding_permit = Some(val),
                OrganismField::CreatedAt(val) => record.created_at = Some(val),
                OrganismField::UpdatedAt(val) => record.updated_at = Some(val),

                OrganismField::PublicationEntityId(val) => record.publication_id = Some(val),

                OrganismField::Doi(_) => {}
                OrganismField::Citation(_) => {}
                OrganismField::Curator(_) => {}
                OrganismField::CuratorOrcid(_) => {}
            }
        }

        records.push(record);
    }

    Ok(records)
}

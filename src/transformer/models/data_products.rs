use std::collections::HashMap;

use tracing::instrument;

use crate::errors::Error;
use crate::transformer::dataset::Dataset;
use crate::transformer::rdf::{self, DataProductField, Literal};
use crate::transformer::resolver::resolve_data;


#[derive(Debug, Default, serde::Serialize)]
pub struct DataProduct {
    pub entity_id: String,
    pub organism_id: Option<String>,
    pub extract_id: Option<String>,
    pub sequence_run_id: Option<String>,
    pub publication_id: Option<String>,
    pub custodian: Option<String>,

    pub sequence_sample_id: Option<String>,
    pub sequence_analysis_id: Option<String>,
    pub notes: Option<String>,
    pub context: Option<String>,
    pub r#type: Option<String>,
    pub file_type: Option<String>,
    pub url: Option<String>,
    pub licence: Option<String>,
    pub access: Option<String>,
}


#[instrument(skip_all)]
pub fn get_all(dataset: &Dataset) -> Result<Vec<DataProduct>, Error> {
    use rdf::DataProduct::*;

    let iris = dataset.scope(&["data_products"]);
    let iris = iris.iter().map(|i| i.as_str()).collect();
    let graph = dataset.graph(&iris);

    let data: HashMap<Literal, Vec<DataProductField>> = resolve_data(
        &graph,
        &[
            EntityId,
            OrganismId,
            ExtractId,
            SequenceRunId,
            SequenceSampleId,
            SequenceAnalysisId,
            Notes,
            Context,
            Type,
            FileType,
            Url,
            Licence,
            Access,
            Custodian,
            CustodianOrcid,
            Citation,
            SourceUrl,
            CustodianEntityId,
            PublicationEntityId,
        ],
    )?;


    let mut products = Vec::new();

    for (_idx, fields) in data {
        let mut product = DataProduct::default();

        for field in fields {
            match field {
                DataProductField::EntityId(val) => product.entity_id = val,
                DataProductField::OrganismId(val) => product.organism_id = Some(val),
                DataProductField::ExtractId(val) => product.extract_id = Some(val),
                DataProductField::SequenceRunId(val) => product.sequence_run_id = Some(val),
                DataProductField::SequenceSampleId(val) => product.sequence_sample_id = Some(val),
                DataProductField::SequenceAnalysisId(val) => product.sequence_analysis_id = Some(val),
                DataProductField::Notes(val) => product.notes = Some(val),
                DataProductField::Context(val) => product.context = Some(val),
                DataProductField::Type(val) => product.r#type = Some(val),
                DataProductField::FileType(val) => product.file_type = Some(val),
                DataProductField::Url(val) => product.url = Some(val),
                DataProductField::Licence(val) => product.licence = Some(val),
                DataProductField::Access(val) => product.access = Some(val),
                DataProductField::CustodianEntityId(val) => product.custodian = Some(val),
                DataProductField::PublicationEntityId(val) => product.publication_id = Some(val),

                DataProductField::Custodian(_val) => {}
                DataProductField::CustodianOrcid(_val) => {}
                DataProductField::Citation(_val) => {}
                DataProductField::SourceUrl(_val) => {}
            }
        }

        products.push(product);
    }

    Ok(products)
}

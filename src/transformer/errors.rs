#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error("A mapping for entity_id must exist for all data transforms")]
    MissingEntityId,

    #[error("Cannot find the header '{0}'")]
    NoHeader(String),

    #[error("The IRI used in the mapping is invalid")]
    InvalidMappingIri(String),

    #[error(transparent)]
    InvalidIri(#[from] iref::InvalidIri<String>),

    #[error(transparent)]
    Parse(#[from] sophia::iri::InvalidIri),

    #[error(transparent)]
    Index(#[from] sophia::inmem::index::TermIndexFullError),

    #[error("Inserting quads failed")]
    Insert(String),

    #[error("Invalid field triple. Fields must be an IRI with a literal value")]
    Field {
        field: Option<crate::transformer::rdf::Value>,
        value: Option<crate::transformer::rdf::Value>,
    },

    #[error(transparent)]
    Resolve(#[from] ResolveError),
}


#[derive(thiserror::Error, Debug)]
pub enum ResolveError {
    #[error("Could not find the IRI {0}")]
    IriNotFound(String),

    #[error("Unsupported mapping {0:?}")]
    UnsupportedMapping(super::rdf::Map),
}

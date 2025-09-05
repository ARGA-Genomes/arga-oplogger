#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("an error occurred with the database connection")]
    Database(#[from] diesel::result::Error),

    #[error("an error occurred getting a database connection")]
    Pool(#[from] diesel::r2d2::PoolError),

    #[error("an error occurred parsing the file")]
    Csv(#[from] csv::Error),

    #[error(transparent)]
    Parsing(#[from] ParseError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    XmlParser(#[from] quick_xml::Error),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    NomenclaturalActType(#[from] NomenclaturalActTypeError),

    #[error(transparent)]
    Lookup(#[from] LookupError),

    #[error(transparent)]
    Reduce(#[from] ReduceError),

    #[error(transparent)]
    Transform(#[from] TransformError),
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error(transparent)]
    DateFormat(#[from] chrono::ParseError),

    #[error("invalid archive: could not find {0}")]
    FileNotFound(String),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error("cannot find element: {0}")]
    NotFound(String),
}


#[derive(thiserror::Error, Debug)]
pub enum NomenclaturalActTypeError {
    #[error("taxonomic name in nomenclatural section must have a status")]
    TaxonomicStatusNotFound,
    #[error("unrecognized taxonomic status: {0}")]
    InvalidNomenclaturalActType(String),
}

#[derive(thiserror::Error, Debug)]
pub enum LookupError {
    #[error("cannot find source in database: {0}")]
    Source(String),

    #[error("cannot find dataset in database: {0}")]
    Dataset(String),

    #[error("cannot find name in database: {0}")]
    Name(String),
}

#[derive(thiserror::Error, Debug)]
pub enum ReduceError {
    #[error("The entity is incomplete and missing a required atom: entity_id: {0}, atom: {1}")]
    MissingAtom(String, String),
}


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

    #[error(transparent)]
    Sparql(#[from] sophia::sparql::SparqlWrapperError<sophia::inmem::index::TermIndexFullError>),

    #[error("Invalid field triple. Fields must be an IRI with a literal value")]
    Field {
        field: Option<crate::transformer::mapped::Value>,
        value: Option<crate::transformer::mapped::Value>,
    },
}

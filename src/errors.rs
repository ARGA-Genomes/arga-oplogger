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
    #[error("The entity is incomplete and missing an required atom: entity_id: {0}, atom: {1}")]
    MissingAtom(String, String),
}

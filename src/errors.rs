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

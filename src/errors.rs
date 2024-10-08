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
    Lookup(#[from] LookupError),
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
}

#[derive(thiserror::Error, Debug)]
pub enum LookupError {
    #[error("cannot find source in database: {0}")]
    Source(String),
}

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
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error(transparent)]
    DateFormat(#[from] chrono::ParseError),
}

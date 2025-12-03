#[derive(thiserror::Error, Debug)]
pub enum ExtractError {
    #[error("The request failed")]
    RequestFailed,

    #[error("Unexpected response structure")]
    UnknownResponse,

    #[error(transparent)]
    File(#[from] std::io::Error),

    #[error(transparent)]
    ChronoDateParsing(#[from] chrono::ParseError),

    #[error(transparent)]
    TomlDateParsing(#[from] toml::value::DatetimeParseError),

    #[error(transparent)]
    TomlSerialize(#[from] toml::ser::Error),

    #[error(transparent)]
    JsonSerialize(#[from] serde_json::Error),

    #[error(transparent)]
    Http(#[from] ureq::Error),
}

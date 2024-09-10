use serde::Deserialize;


#[derive(Debug, Deserialize)]
pub struct Meta {
    pub dataset: Dataset,
    pub changelog: Changelog,
    pub attribution: Attribution,
    pub collection: Collection,
}

#[derive(Debug, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    pub short_name: String,
    pub version: String,
    /// RFC 3339
    pub published_at: toml::value::Datetime,
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Changelog {
    pub notes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Attribution {
    pub citation: String,
    pub source_url: String,
    pub license: String,
    pub rights_holder: String,
}

#[derive(Debug, Deserialize)]
pub struct Collection {
    pub name: String,
    pub author: String,
    pub license: String,
    pub rights_holder: String,
    pub access_rights: String,
}

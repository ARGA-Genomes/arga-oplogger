use arga_core::models;
use arga_core::models::AccessRightsStatus;
use arga_core::models::DataReuseStatus;
use arga_core::models::SourceContentType;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct Meta {
    pub dataset: Dataset,
    pub changelog: Changelog,
    pub attribution: Attribution,
    pub collection: Collection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    pub short_name: String,
    pub version: String,
    /// RFC 3339
    pub published_at: toml::value::Datetime,
    pub url: String,
    pub reuse_pill: Option<DataReuseStatus>,
    pub access_pill: Option<AccessRightsStatus>,
    pub publication_year: Option<i16>,
    pub content_type: Option<SourceContentType>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Changelog {
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Attribution {
    pub citation: String,
    pub source_url: String,
    pub license: String,
    pub rights_holder: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Collection {
    pub name: String,
    pub author: String,
    pub license: String,
    pub rights_holder: String,
    pub access_rights: String,
    pub reuse_pill: Option<DataReuseStatus>,
    pub access_pill: Option<AccessRightsStatus>,
    pub content_type: Option<SourceContentType>,
}

impl From<Meta> for models::Source {
    fn from(meta: Meta) -> Self {
        models::Source {
            id: Uuid::new_v4(),
            name: meta.collection.name,
            author: meta.collection.author,
            rights_holder: meta.collection.rights_holder,
            access_rights: meta.collection.access_rights,
            license: meta.collection.license,
            reuse_pill: meta.collection.reuse_pill,
            access_pill: meta.collection.access_pill,
            content_type: meta.collection.content_type,
        }
    }
}

impl From<Meta> for models::Dataset {
    fn from(meta: Meta) -> Self {
        models::Dataset {
            id: Uuid::new_v4(),
            source_id: Uuid::default(),
            global_id: meta.dataset.id,
            name: meta.dataset.name,
            short_name: Some(meta.dataset.short_name),
            description: None,
            url: Some(meta.dataset.url),
            citation: Some(meta.attribution.citation),
            license: Some(meta.attribution.license),
            rights_holder: Some(meta.attribution.rights_holder),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            reuse_pill: meta.dataset.reuse_pill,
            access_pill: meta.dataset.access_pill,
            publication_year: meta.dataset.publication_year,
            content_type: meta.dataset.content_type,
        }
    }
}

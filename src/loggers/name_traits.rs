use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;

use arga_core::models::{AttributeCategory, AttributeValueType, NameAttribute};
use arga_core::{models, schema};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use csv::StringRecord;
use diesel::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::FrameProgress;
use crate::database::PgPool;
use crate::errors::Error;
use crate::readers::meta;
use crate::utils::{new_progress_bar, parse_naive_date_time};


#[derive(Debug, Clone)]
pub enum AttributeValue {
    Boolean(bool),
    Integer(i64),
    Decimal(BigDecimal),
    String(String),
    Timestamp(NaiveDateTime),
}


/// The CSV traits to import directly.
/// This is deserializeable with the serde crate and enforces expectations
/// about what fields are mandatory and the format they should be in.
#[derive(Debug, Clone, Default)]
struct Record {
    scientific_name: Option<String>,
    canonical_name: Option<String>,
    dataset_id: Option<String>,
    attributes: HashMap<String, AttributeValue>,
}


/// Import media if they are not already in the table
pub fn import(pool: PgPool, records: &[models::NameAttribute]) -> Result<(), Error> {
    use schema::name_attributes::dsl::*;
    let mut conn = pool.get()?;

    let mut total_imported = 0;
    let bar = new_progress_bar(records.len(), "Importing name traits");

    for chunk in records.chunks(1_000) {
        let inserted = diesel::insert_into(name_attributes)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;

        total_imported += inserted;
        bar.inc(1_000);
    }

    bar.finish();
    info!(total = records.len(), total_imported, "Name trait import finished");
    Ok(())
}


pub fn import_archive<S: Read + FrameProgress>(stream: S) -> Result<(), Error> {
    let bars = stream.bars();

    let mut pool = crate::database::get_pool()?;
    let input = brotli::Decompressor::new(stream, 4096);

    let attrs = extract(input, &mut pool)?;
    import(pool, &attrs)?;

    Ok(())
}

pub fn extract<S: Read + FrameProgress>(stream: S, pool: &mut PgPool) -> Result<Vec<NameAttribute>, Error> {
    info!("Loading names");
    let names = crate::database::name_lookup(pool)?;
    let datasets = crate::database::dataset_lookup(pool)?;

    let bars = stream.bars();

    let mut reader = csv::Reader::from_reader(stream);
    let headers = reader.headers()?.clone();

    let mut attrs: Vec<NameAttribute> = Vec::new();

    for row in reader.into_records() {
        let row = row?;
        let record = decompose_row(&headers, row)?;

        let name = record.scientific_name.clone().or(record.clone().canonical_name);

        let Some(name_id) = names.get(&name.clone().unwrap_or_default())
        else {
            tracing::warn!(?name, "cannot find name. skipping");
            continue;
        };

        let Some(dataset_id) = datasets.get(&record.dataset_id.clone().unwrap_or_default())
        else {
            tracing::warn!(?record.dataset_id, "cannot find dataset. skipping");
            continue;
        };

        let extracted = into_name_attributes(dataset_id, name_id, record)?;
        attrs.extend(extracted);
        bars.frames.inc(1);
    }

    bars.finish();
    Ok(attrs)
}


fn decompose_row(headers: &StringRecord, row: StringRecord) -> Result<Record, Error> {
    let mut record = Record {
        scientific_name: None,
        canonical_name: None,
        dataset_id: None,
        attributes: HashMap::new(),
    };

    for (header, field) in headers.iter().zip(row.into_iter()) {
        match header {
            "scientific_name" => {
                record.scientific_name = Some(field.to_string());
            }
            "canonical_name" => {
                record.canonical_name = Some(field.to_string());
            }
            "dataset_id" => {
                record.dataset_id = Some(field.to_string());
            }
            header => {
                if !field.trim().is_empty() {
                    record.attributes.insert(header.to_string(), infer_type(field));
                }
            }
        };
    }

    Ok(record)
}


fn infer_type(value: &str) -> AttributeValue {
    if value.to_lowercase() == "true" {
        AttributeValue::Boolean(true)
    }
    else if value.to_lowercase() == "false" {
        AttributeValue::Boolean(false)
    }
    else if let Ok(timestamp) = parse_naive_date_time(value) {
        AttributeValue::Timestamp(timestamp)
    }
    else if let Ok(integer) = str::parse::<i64>(value) {
        AttributeValue::Integer(integer)
    }
    else if let Ok(decimal) = BigDecimal::from_str(value) {
        AttributeValue::Decimal(decimal)
    }
    else {
        AttributeValue::String(value.to_string())
    }
}


// fn extract_attributes(dataset_id: &Uuid, name_id: &Uuid, records: Vec<Record>) -> Result<Vec<NameAttribute>, Error> {
//     info!(total = records.len(), "Extracting name attributes");

//     let attrs: Result<Vec<Vec<NameAttribute>>, Error> = records
//         .into_par_iter()
//         .map(|row| {
//             let attributes = into_name_attribute(&dataset_id, &name_id, row)?;
//             Ok(attributes)
//         })
//         .collect();

//     let attrs = attrs?;
//     let attrs: Vec<NameAttribute> = attrs.into_iter().flatten().collect();

//     info!(attributes = attrs.len(), "Extracting name attributes finished");
//     Ok(attrs)
// }


fn into_name_attributes(dataset_id: &Uuid, name_id: &Uuid, row: Record) -> Result<Vec<NameAttribute>, Error> {
    let mut attributes = Vec::new();

    for (key, attr) in row.attributes {
        attributes.push(NameAttribute {
            id: Uuid::new_v4(),
            dataset_id: dataset_id.clone(),
            name_id: name_id.clone(),
            name: key,
            category: AttributeCategory::BushfireRecovery,
            value_type: match &attr {
                AttributeValue::Boolean(_) => AttributeValueType::Boolean,
                AttributeValue::Integer(_) => AttributeValueType::Integer,
                AttributeValue::Decimal(_) => AttributeValueType::Decimal,
                AttributeValue::String(_) => AttributeValueType::String,
                AttributeValue::Timestamp(_) => AttributeValueType::Timestamp,
            },
            value_bool: match attr {
                AttributeValue::Boolean(val) => Some(val),
                _ => None,
            },
            value_int: match attr {
                AttributeValue::Integer(val) => Some(val),
                _ => None,
            },
            value_decimal: match &attr {
                AttributeValue::Decimal(val) => Some(val.clone()),
                _ => None,
            },
            value_str: match &attr {
                AttributeValue::String(val) => Some(val.clone()),
                _ => None,
            },
            value_timestamp: match &attr {
                AttributeValue::Timestamp(val) => Some(val.clone()),
                _ => None,
            },
        });
    }

    Ok(attributes)
}

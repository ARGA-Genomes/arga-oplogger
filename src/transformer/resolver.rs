use std::collections::HashMap;

use sophia::api::MownStr;
use sophia::api::prelude::*;
use sophia::api::term::{BnodeId, SimpleTerm};
use tracing::{info, trace, warn};

use crate::errors::{ResolveError, TransformError};
use crate::transformer::dataset::PartialGraph;
use crate::transformer::rdf::{
    Condition,
    IntoIriTerm,
    Literal,
    Map,
    Mapping,
    MappingCondition,
    Rdfs,
    ToIriOwned,
    try_from_iri,
};


pub type FieldMap = HashMap<iref::IriBuf, Vec<Map>>;
pub type ValueMap = HashMap<iref::IriBuf, Vec<Literal>>;
pub type RecordMap = HashMap<Literal, ValueMap>;


#[tracing::instrument(skip_all)]
pub fn load_records(
    graph: &PartialGraph,
    map: &FieldMap,
    field_iris: &Vec<&iref::Iri>,
) -> Result<RecordMap, TransformError> {
    info!("Resolving field terms");
    // get the predicate terms to find matching triples for. in our case the predicate
    // is the mapped field name with the subject being the record entity_id and the object
    // being the value of the field.
    let terms = resolve_field_terms(&field_iris, &map)?;
    let terms = Vec::from_iter(terms);

    let mut conditions: Vec<(&iref::Iri, &Condition)> = Vec::new();

    // the field names in the matched triples will be the specific source model field which means
    // we need to build a simple map to get the field type that it is mapped to
    let mut reverse_map: HashMap<iref::IriBuf, Vec<iref::IriBuf>> = HashMap::new();
    for (key, maps) in map.iter() {
        for field in maps {
            let iris = match field {
                Map::Same(iri) => vec![iri.clone()],
                Map::Combines(iris) => iris.clone(),
                Map::Hash(iri) => vec![iri.clone()],
                Map::HashFirst(iris) => iris.clone(),
                Map::When(_iri, _condition) => vec![],
            };

            for mapped_from in iris {
                reverse_map.entry(mapped_from).or_default().push(key.clone());
            }

            if let Map::When(iri, condition) = field {
                conditions.push((iri.as_iri(), condition));
            }
        }
    }

    // get the data and use the reverse map to associate the record with a list of fields
    let mut records = RecordMap::new();

    for triple in graph.triples_matching(Any, terms.as_slice(), Any) {
        let [s, p, o] = triple?;

        let subject = match s {
            SimpleTerm::LiteralDatatype(value, _type) => Literal::String(value.to_string()),
            _ => unimplemented!(),
        };

        let mapped_to_iri = match p {
            SimpleTerm::Iri(iri) => match reverse_map.get(&iri.to_iri_owned()?) {
                Some(iris) => Ok(iris),
                None => Err(ResolveError::IriNotFound(iri.to_string())),
            }?,
            _ => unimplemented!(),
        };

        let value = match o {
            SimpleTerm::LiteralDatatype(value, _type) => Literal::String(value.to_string()),
            _ => unimplemented!(),
        };


        // copy the resolved data to all iris that are mapped to it. its
        // possible to map the same source iri to multiple model iris which
        // means we have to clone the data into all of them
        let record = records.entry(subject).or_default();
        for iri in mapped_to_iri {
            record.entry(iri.clone()).or_default().push(value.clone());
        }
    }

    let records = records
        .into_iter()
        .filter(|(_idx, record)| {
            for (iri, cond) in &conditions {
                if let Some(values) = record.get(*iri) {
                    for value in values {
                        if !cond.check(value) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .collect();

    Ok(records)
}


#[tracing::instrument(skip_all)]
pub fn resolve_data<'a, T, R>(graph: &PartialGraph, fields: &'a [T]) -> Result<HashMap<Literal, Vec<R>>, TransformError>
where
    T: Into<&'a iref::Iri> + TryFrom<&'a iref::Iri> + std::fmt::Debug,
    R: From<(T, Literal)> + Clone,
    &'a iref::Iri: From<&'a T>,
{
    info!("Resolving fields");
    // resolve the full mapping plan for all fields
    let map = resolve_fields(graph, fields)?;

    // get the iri for all fields to resolve
    let field_iris: Vec<&iref::Iri> = fields.iter().map(|f| f.into()).collect();


    let records = load_records(graph, &map, &field_iris)?;

    let mut data: HashMap<Literal, Vec<R>> = HashMap::new();

    // get the transform plan for the field and add that to the final result
    for field_iri in field_iris {
        let Some(mapping) = map.get(field_iri)
        else {
            warn!("Field mapping not found: {field_iri}");
            continue;
        };

        for (entity_id, fields) in records.iter() {
            for field_map in mapping {
                let result = match field_map {
                    // no transformation necessary so just copy the value as is
                    Map::Same(_iri) => fields.get(field_iri),
                    Map::Hash(_iri) => fields.get(field_iri),

                    // iterate over all the values in the list and return the
                    // first non empty value
                    Map::HashFirst(iris) => {
                        let mut value = None;
                        for iri in iris {
                            if let Some(val) = fields.get(iri) {
                                value = Some(val);
                                break;
                            }
                        }
                        value
                    }

                    // iterate over all values in the list and combine them
                    // into a single string. we specifically want to elide fields
                    // that don't have a value here as that's the requirement for Combines
                    Map::Combines(iris) => {
                        let mut to_combine: Vec<&str> = Vec::new();

                        for iri in iris {
                            // a field can be mapped to multiple source fields so we
                            // need to handle that scenario here. this can lead to pretty
                            // strange bugs due to the order being random so if there is
                            // a more than one value we fail with an ambiguity error.
                            //
                            // the reason why this matter for Combines is because we can't
                            // tell which value is from which graph leaving us no possible way
                            // to combine values isolated within their graphs
                            if let Some(values) = fields.get(iri) {
                                let present: Vec<&String> = values
                                    .iter()
                                    .filter_map(|v| match v {
                                        // only return strings with actual data
                                        Literal::String(val) => match val.is_empty() {
                                            true => None,
                                            false => Some(val),
                                        },
                                    })
                                    .collect();

                                let value = if present.len() > 1 {
                                    Err(ResolveError::AmbiguousMapping(iri.clone(), values.clone()))
                                }
                                else {
                                    Ok(present.first().cloned())
                                }?;

                                if let Some(val) = value {
                                    to_combine.push(val);
                                }
                            }
                        }

                        Some(&vec![Literal::String(to_combine.join(" "))])
                    }

                    // ignore when conditions as records should already be
                    // filtered out at this stage
                    Map::When(_iri, _condition) => None,
                };


                // add all the fields even if there are multiple of the same.
                // uniqueness or disambiguation is a job outside this function
                if let Some(result) = result {
                    for value in result {
                        let mapped_from = T::try_from(field_iri)
                            .map_err(|_| TransformError::InvalidMappingIri(field_iri.to_string()))?;
                        let field: R = (mapped_from, value.clone()).into();
                        data.entry(entity_id.clone()).or_default().push(field);
                    }
                }
            }
        }
    }

    Ok(data)
}


#[tracing::instrument(skip_all)]
pub fn resolve_fields<'a, T>(graph: &PartialGraph, fields: &'a [T]) -> Result<FieldMap, TransformError>
where
    T: Into<&'a iref::Iri> + std::fmt::Debug,
    &'a iref::Iri: From<&'a T>,
{
    let mut resolved = FieldMap::new();

    // build iris from the fields otherwise we run into various lifetime issues
    let iris: Vec<&iref::Iri> = fields.iter().map(|f| f.into()).collect();

    // convert the fields into a simple term for the iri
    let mut terms: Vec<SimpleTerm> = Vec::new();
    for iri in iris.iter() {
        terms.push(iri.into_iri_term()?);
    }

    trace!(?terms, "Matching triples");
    for triple in graph.triples_matching(terms.as_slice(), Any, Any) {
        let [s, p, o] = triple?;

        let pred: Mapping = p.try_into()?;

        let map = match pred {
            Mapping::Same => match o {
                SimpleTerm::Iri(iri_ref) => Map::Same(iri_ref.to_iri_owned()?),
                _ => unimplemented!(),
            },
            Mapping::Hash => match o {
                SimpleTerm::Iri(iri_ref) => Map::Hash(iri_ref.to_iri_owned()?),
                _ => unimplemented!(),
            },
            Mapping::HashFirst => match o {
                SimpleTerm::BlankNode(bnode_id) => {
                    let mut iris = Vec::new();
                    collect_iris(&mut iris, &graph, bnode_id)?;
                    Map::HashFirst(iris)
                }
                _ => unimplemented!(),
            },
            Mapping::Combines => match o {
                SimpleTerm::BlankNode(bnode_id) => {
                    let mut iris = Vec::new();
                    collect_iris(&mut iris, &graph, bnode_id)?;
                    Map::Combines(iris)
                }
                _ => unimplemented!(),
            },

            Mapping::When => match o {
                SimpleTerm::Triple(triple) => {
                    let [cond_s, cond_p, cond_o] = triple.spo();

                    let subject = match cond_s {
                        SimpleTerm::Iri(iri_ref) => iri_ref.to_iri_owned()?,
                        _ => unimplemented!(),
                    };

                    let condition = match MappingCondition::try_from(cond_p)? {
                        MappingCondition::Is => Condition::Is(Literal::try_from(cond_o)?),
                    };

                    Map::When(subject, condition)
                }
                _ => unimplemented!(),
            },
        };

        match s {
            SimpleTerm::Iri(iri_ref) => resolved.entry(iri_ref.to_iri_owned()?).or_default().push(map),
            _ => unimplemented!(),
        };
    }


    Ok(resolved)
}


/// Collect all the IRIs in a linked list specified by rdfs
#[tracing::instrument(skip_all)]
pub fn collect_iris(
    iris: &mut Vec<iref::IriBuf>,
    graph: &PartialGraph,
    node: &BnodeId<MownStr<'_>>,
) -> Result<(), TransformError> {
    for triple in graph.triples_matching([node], Any, Any) {
        let [_s, p, o] = triple?;
        let pred: Rdfs = p.try_into()?;

        match pred {
            Rdfs::First => match o {
                SimpleTerm::Iri(iri_ref) => iris.push(iri_ref.to_iri_owned()?),
                _ => unimplemented!(),
            },

            Rdfs::Rest => match o {
                SimpleTerm::BlankNode(bnode_id) => collect_iris(iris, graph, bnode_id)?,
                SimpleTerm::Iri(iri_ref) => match try_from_iri::<_, Rdfs>(iri_ref)? {
                    Rdfs::Nil => return Ok(()),
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            },

            Rdfs::Nil => return Ok(()),
        }
    }

    Ok(())
}


#[tracing::instrument(skip_all)]
pub fn resolve_field_terms<'a>(
    fields: &Vec<&iref::Iri>,
    map: &'a FieldMap,
) -> Result<std::collections::HashSet<SimpleTerm<'a>>, TransformError> {
    let mut terms = std::collections::HashSet::new();

    for field_iri in fields {
        // get all the mapping referenced by the field
        let Some(mapping) = map.get(*field_iri)
        else {
            continue;
        };

        // because a field can be mapped to many other fields due to
        // it being present for different graphs we want to make sure to
        // get all of them when determining the terms
        for field_map in mapping {
            match field_map {
                Map::Same(mapping) => {
                    terms.insert(mapping.into_iri_term()?);
                }
                Map::Hash(mapping) => {
                    terms.insert(mapping.into_iri_term()?);
                }
                Map::HashFirst(iris) => {
                    // rather than resolving all the fields in the HashFirst mapping
                    // we iterate over it here since we only want to support the :same
                    // operator otherwise the complexity will drive deeper than it needs to be
                    for iri in iris {
                        let mapping = match map.get(iri) {
                            Some(mapping) => Ok(mapping),
                            None => Err(ResolveError::IriNotFound(iri.to_string())),
                        }?;

                        for field_map in mapping {
                            match field_map {
                                Map::Same(mapping) => Ok(terms.insert(mapping.into_iri_term()?)),
                                unsupported => Err(ResolveError::UnsupportedMapping(unsupported.clone())),
                            }?;
                        }
                    }
                }
                Map::Combines(iris) => {
                    // we have the same requirements here as HashFirst
                    for iri in iris {
                        let mapping = match map.get(iri) {
                            Some(mapping) => Ok(mapping),
                            None => Err(ResolveError::IriNotFound(iri.to_string())),
                        }?;

                        for field_map in mapping {
                            match field_map {
                                Map::Same(mapping) => Ok(terms.insert(mapping.into_iri_term()?)),
                                unsupported => Err(ResolveError::UnsupportedMapping(unsupported.clone())),
                            }?;
                        }
                    }
                }
                Map::When(iri, _condition) => {
                    terms.insert(iri.into_iri_term()?);
                }
            }
        }
    }

    Ok(terms)
}

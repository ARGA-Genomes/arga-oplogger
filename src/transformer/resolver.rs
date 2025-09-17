use std::collections::HashMap;

use sophia::api::MownStr;
use sophia::api::prelude::*;
use sophia::api::term::{BnodeId, SimpleTerm};

use crate::errors::{ResolveError, TransformError};
use crate::transformer::dataset::PartialGraph;
use crate::transformer::rdf::{IntoIriTerm, Literal, Map, Mapping, Rdfs, ToIriOwned, try_from_iri};


pub type FieldMap = HashMap<iref::IriBuf, Map>;


#[tracing::instrument(skip_all)]
pub fn resolve_data<'a, T, R>(graph: &PartialGraph, fields: &'a [T]) -> Result<HashMap<Literal, Vec<R>>, TransformError>
where
    T: Into<&'a iref::Iri> + TryFrom<&'a iref::Iri> + std::fmt::Debug,
    R: From<(T, Literal)> + Clone,
    &'a iref::Iri: From<&'a T>,
{
    // resolve the full mapping plan for all fields
    let map = resolve_fields(graph, fields)?;

    // get the iri for all fields to resolve
    let field_iris: Vec<&iref::Iri> = fields.iter().map(|f| f.into()).collect();


    // get the predicate terms to find matching triples for. in our case the predicate
    // is the mapped field name with the subject being the record entity_id and the object
    // being the value of the field.
    let mut terms = Vec::new();

    for field_iri in &field_iris {
        match map.get(*field_iri) {
            Some(Map::Same(mapping)) => terms.push(mapping.into_iri_term()?),
            Some(Map::HashFirst(iris)) => {
                for iri in iris {
                    match map.get(iri) {
                        Some(Map::Same(mapping)) => Ok(terms.push(mapping.into_iri_term()?)),
                        Some(unsupported) => Err(ResolveError::UnsupportedMapping(unsupported.clone())),
                        None => Err(ResolveError::IriNotFound(iri.to_string())),
                    }?;
                }
            }
            _ => {}
        }
    }

    // the field names in the matched triples will be the specific source model field which means
    // we need to build a simple map to get the field type that it is mapped to
    let mut reverse_map: HashMap<&iref::IriBuf, &iref::IriBuf> = HashMap::new();
    for (key, value) in map.iter() {
        if let Map::Same(mapped_from) = value {
            reverse_map.insert(mapped_from, key);
        }
    }

    // get the data and use the reverse map to associate the entity_id of the record to a list of fields
    let mut mapped: HashMap<Literal, HashMap<iref::IriBuf, Literal>> = HashMap::new();

    for triple in graph.triples_matching(Any, terms.as_slice(), Any) {
        let [s, p, o] = triple?;
        let subject = match s {
            SimpleTerm::LiteralDatatype(value, _type) => Literal::String(value.to_string()),
            _ => unimplemented!(),
        };

        let mapped_to_iri = match p {
            SimpleTerm::Iri(iri) => match reverse_map.get(&iri.to_iri_owned()?) {
                Some(iri) => Ok(*iri),
                None => Err(ResolveError::IriNotFound(iri.to_string())),
            }?,
            _ => unimplemented!(),
        };

        let value = match o {
            SimpleTerm::LiteralDatatype(value, _type) => Literal::String(value.to_string()),
            _ => unimplemented!(),
        };


        mapped.entry(subject).or_default().insert(mapped_to_iri.clone(), value);
    }


    let mut data: HashMap<Literal, Vec<R>> = HashMap::new();

    // get the transform plan for the field and add that to the final result
    for field_iri in field_iris {
        let mapping = map
            .get(field_iri)
            .ok_or(ResolveError::IriNotFound(field_iri.to_string()))?;

        for (entity_id, fields) in mapped.iter() {
            let result = match mapping {
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
            };

            if let Some(result) = result {
                let mapped_from =
                    T::try_from(field_iri).map_err(|_| TransformError::InvalidMappingIri(field_iri.to_string()))?;
                let field: R = (mapped_from, result.clone()).into();
                data.entry(entity_id.clone()).or_default().push(field);
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
        };

        match s {
            SimpleTerm::Iri(iri_ref) => resolved.insert(iri_ref.to_iri_owned()?, map),
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

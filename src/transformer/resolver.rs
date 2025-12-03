use std::collections::HashMap;

use sophia::api::MownStr;
use sophia::api::prelude::*;
use sophia::api::term::matcher::GraphNameMatcher;
use sophia::api::term::{BnodeId, GraphName, SimpleTerm};
use tracing::{debug, info, trace, warn};

use crate::errors::{ResolveError, TransformError};
use crate::transformer::rdf::{
    Condition,
    FromCondition,
    IntoIriTerm,
    Literal,
    Map,
    Mapping,
    MappingCondition,
    Rdfs,
    ToIri,
    ToIriOwned,
    try_from_iri,
};


pub type FieldMap = HashMap<iref::IriBuf, Vec<Map>>;
pub type ValueMap = HashMap<iref::IriBuf, Vec<Literal>>;
pub type RecordMap = HashMap<Literal, ValueMap>;


pub struct Resolver<'a> {
    dataset: &'a super::dataset::Dataset,
}

impl Resolver<'_> {
    pub fn new(dataset: &super::dataset::Dataset) -> Resolver<'_> {
        Resolver { dataset }
    }

    /// Load all records within the specified scope and resolve the specified fields
    #[tracing::instrument(skip_all)]
    pub fn resolve<'a, T, R>(
        &self,
        fields: &'a [T],
        scope: &[&iref::Iri],
    ) -> Result<HashMap<Literal, Vec<R>>, TransformError>
    where
        T: Into<&'a iref::Iri> + TryFrom<&'a iref::Iri> + std::fmt::Debug,
        R: From<(T, Literal)> + Clone,
        &'a iref::Iri: From<&'a T>,
    {
        info!(?fields, ?scope, "Resolving fields");

        // get the iri for all fields to resolve
        let field_iris: Vec<&iref::Iri> = fields.iter().map(|f| f.into()).collect();
        let map = self.field_map(&field_iris, scope)?;

        let records = self.records(&field_iris, scope)?;

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
                        Map::Same(_iri) => fields.get(field_iri),
                        Map::Hash(_iri) => fields.get(field_iri),
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
                        Map::When(_iri, _condition) => None,
                        Map::From { .. } => None,
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

    /// Get records container the specified fields in the specified models
    #[tracing::instrument(skip_all)]
    pub fn records(&self, fields: &[&iref::Iri], scope: &[&iref::Iri]) -> Result<RecordMap, TransformError> {
        let map = self.field_map(fields, scope)?;

        let mut conditions: Vec<(&iref::Iri, &Condition)> = Vec::new();
        let mut linked: Vec<(&iref::Iri, &iref::Iri, &iref::Iri)> = Vec::new();
        let mut linked_fields: Vec<&iref::IriBuf> = Vec::new();

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
                    Map::From { .. } => vec![],
                };

                for mapped_from in iris {
                    reverse_map.entry(mapped_from).or_default().push(key.clone());
                }

                if let Map::When(iri, condition) = field {
                    conditions.push((iri.as_iri(), condition));
                }

                if let Map::From { graph, via } = field {
                    linked.push((key.as_iri(), graph.as_iri(), via.as_iri()));
                    linked_fields.push(via);
                }
            }
        }


        // get the predicate terms to find matching triples for. in our case the predicate
        // is the mapped field name with the subject being the record entity_id and the object
        // being the value of the field.
        let terms = resolve_field_terms(&fields.to_vec(), &map)?;
        let terms = Vec::from_iter(terms);
        debug!(?terms, "resolved terms");


        // get the data and use the reverse map to associate the record with a list of fields
        let mut records = RecordMap::new();

        // a lookup that associates a link value with record rows that also contain the
        // same value. this lets us iterate through the linked data and extend all records
        // associated with it in this map
        let mut record_links: HashMap<&iref::Iri, HashMap<Literal, Vec<Literal>>> = HashMap::new();


        let scope: Vec<&str> = scope.iter().map(|s| s.as_str()).collect();

        for quad in self
            .dataset
            .source
            .quads_matching(Any, terms.as_slice(), Any, GraphIri(&scope))
        {
            let (g, [s, p, o]) = quad?;

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
            let record = records.entry(subject.clone()).or_default();
            for iri in mapped_to_iri {
                if linked_fields.contains(&iri) {
                    // add the record row index with the value of the linked field
                    // as the key for looking up when resolving the linked dataset
                    record_links
                        .entry(iri.as_iri())
                        .or_default()
                        .entry(value.clone())
                        .or_default()
                        .push(subject.clone());
                }

                record.entry(iri.clone()).or_default().push(value.clone());
            }
        }


        for (key, graph, via) in linked {
            debug!(?key, ?via, ?graph, "getting linked dataset matches");
            let models = self.dataset.get_source_from_model(graph)?;
            let mut models: Vec<&iref::Iri> = models.iter().map(|m| m.as_ref()).collect();
            models.push(graph);

            let linked_data = self.records(&[&key, &via], models.as_slice())?;

            for (_k, values) in linked_data {
                // get the first key value assigned to the through field
                if let Some(keys) = values.get(via) {
                    // look up rows that have matching values to the 'via' field
                    // and extend it with the values on the linked dataset.
                    let via_key = keys.first().unwrap().clone();
                    let rows = record_links.get(&via).and_then(|map| map.get(&via_key));
                    if let Some(rows) = rows {
                        for idx in rows {
                            records.entry(idx.clone()).or_default().extend(values.clone());
                        }
                    }
                }
            }
        }


        // filter records that dont match the condition placed on it
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

    /// Get the field mapping for the specified fields
    #[tracing::instrument(skip_all)]
    pub fn field_map(&self, fields: &[&iref::Iri], scope: &[&iref::Iri]) -> Result<FieldMap, TransformError> {
        let mut resolved = FieldMap::new();

        // convert the fields into a simple term for the iri
        let mut terms: Vec<SimpleTerm> = Vec::new();
        for iri in fields.iter() {
            terms.push(iri.into_iri_term()?);
        }

        // convert the scope iri's in graph name matchers
        let mut scope_terms = Vec::new();
        for iri in scope.iter() {
            scope_terms.push(Some(iri.into_iri_term()?));
        }

        trace!(?terms, ?scope, "Matching triples");
        for quad in self
            .dataset
            .source
            .quads_matching(terms.as_slice(), Any, Any, scope_terms.as_slice())
        {
            let (g, [s, p, o]) = quad?;
            let graph = match g {
                Some(SimpleTerm::Iri(iri_ref)) => iri_ref.to_iri()?,
                _ => unimplemented!(),
            };

            // parse the predicate as a valid mapping term
            let predicate: Mapping = p.try_into()?;

            let map = match predicate {
                // allows an iri to refer to a different iri. this is the most
                // common option to map a common domain from one model to another
                Mapping::Same => match o {
                    SimpleTerm::Iri(iri_ref) => Map::Same(iri_ref.to_iri_owned()?),
                    _ => unimplemented!(),
                },
                // the same as Same except that it indicates that it should hash
                // the resolved value
                Mapping::Hash => match o {
                    SimpleTerm::Iri(iri_ref) => Map::Hash(iri_ref.to_iri_owned()?),
                    _ => unimplemented!(),
                },
                // hash the first field that has a valid value
                Mapping::HashFirst => match o {
                    SimpleTerm::BlankNode(bnode_id) => {
                        let mut iris = Vec::new();
                        self.collect_iris(&mut iris, bnode_id, graph)?;
                        Map::HashFirst(iris)
                    }
                    _ => unimplemented!(),
                },
                // combines all field values into one
                Mapping::Combines => match o {
                    SimpleTerm::BlankNode(bnode_id) => {
                        let mut iris = Vec::new();
                        self.collect_iris(&mut iris, bnode_id, graph)?;
                        Map::Combines(iris)
                    }
                    _ => unimplemented!(),
                },
                // a filter condition to only return data if met
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

                // a directive to load the data from another graph
                Mapping::From => match o {
                    SimpleTerm::Triple(triple) => {
                        let [cond_s, cond_p, cond_o] = triple.spo();

                        let graph = match cond_s {
                            SimpleTerm::Iri(iri_ref) => iri_ref.to_iri_owned()?,
                            _ => unimplemented!(),
                        };

                        let via = match FromCondition::try_from(cond_p)? {
                            FromCondition::Via => match cond_o {
                                SimpleTerm::Iri(iri_ref) => iri_ref.to_iri_owned()?,
                                _ => unimplemented!(),
                            },
                        };

                        Map::From { graph, via }
                    }
                    _ => unimplemented!(),
                },
            };


            // add the map to the common domain model
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
        &self,
        iris: &mut Vec<iref::IriBuf>,
        node: &BnodeId<MownStr<'_>>,
        graph: &iref::Iri,
    ) -> Result<(), TransformError> {
        for quad in self
            .dataset
            .source
            .quads_matching([node], Any, Any, GraphIriName(&graph))
        {
            let (_g, [_s, p, o]) = quad?;
            let pred: Rdfs = p.try_into()?;

            match pred {
                Rdfs::First => match o {
                    SimpleTerm::Iri(iri_ref) => iris.push(iri_ref.to_iri_owned()?),
                    _ => unimplemented!(),
                },

                Rdfs::Rest => match o {
                    SimpleTerm::BlankNode(bnode_id) => self.collect_iris(iris, bnode_id, graph)?,
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
}


#[tracing::instrument(skip_all)]
pub fn resolve_field_terms<'a>(
    fields: &Vec<&iref::Iri>,
    map: &'a FieldMap,
) -> Result<std::collections::HashSet<SimpleTerm<'a>>, TransformError> {
    let mut terms = std::collections::HashSet::new();

    debug!(?map, ?fields, "resolving field terms");

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
                Map::From { via, .. } => {
                    terms.insert(via.into_iri_term()?);
                }
            }
        }
    }

    Ok(terms)
}


#[derive(Clone, Copy)]
pub struct GraphIri<'a>(&'a Vec<&'a str>);

impl<'a> GraphNameMatcher for GraphIri<'a> {
    type Term = SimpleTerm<'static>;

    fn matches<T2: Term + ?Sized>(&self, graph_name: GraphName<&T2>) -> bool {
        match graph_name {
            // only include matching graph names
            Some(t) => match t.as_simple() {
                SimpleTerm::Iri(iri) => self.0.contains(&iri.as_str()),
                _ => false,
            },
            // always include the default graph
            None => false,
        }
    }
}

#[derive(Clone, Copy)]
pub struct GraphIriName<'a>(&'a iref::Iri);

impl<'a> GraphNameMatcher for GraphIriName<'a> {
    type Term = SimpleTerm<'static>;

    fn matches<T2: Term + ?Sized>(&self, graph_name: GraphName<&T2>) -> bool {
        match graph_name {
            // only include matching graph names
            Some(t) => match t.as_simple() {
                SimpleTerm::Iri(iri) => self.0.eq(&iri.as_str()),
                _ => false,
            },
            // always include the default graph
            None => false,
        }
    }
}

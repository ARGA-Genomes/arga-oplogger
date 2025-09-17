use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use sophia::api::dataset::Dataset as DatasetTrait;
use sophia::api::graph::adapter::PartialUnionGraph;
use sophia::api::ns::Namespace;
use sophia::api::prelude::*;
use sophia::api::term::matcher::GraphNameMatcher;
use sophia::api::term::{GraphName, SimpleTerm};
use sophia::inmem::dataset::FastDataset;
use sophia::turtle::parser::trig;
use tracing::info;

use crate::errors::{Error, TransformError};


pub type PartialGraph<'a> = PartialUnionGraph<&'a FastDataset, GraphIri<'a>>;


pub struct Dataset {
    source: FastDataset,
    map: String,
}


impl Dataset {
    pub fn new(map_iri: &str) -> Dataset {
        let source = FastDataset::new();

        Dataset {
            source,
            map: map_iri.to_string(),
        }
    }

    pub fn graph<'a>(&'a self, graphs: &'a Vec<&'a str>) -> PartialGraph<'a> {
        let selector = GraphIri(&graphs);
        self.source.partial_union_graph(selector)
    }

    pub fn load_trig<R: std::io::Read>(&mut self, buf: BufReader<R>) -> Result<(), TransformError> {
        let quads = trig::parse_bufread(buf);
        self.source
            .insert_all(quads)
            .map_err(|e| TransformError::Insert(e.to_string()))?;
        Ok(())
    }

    pub fn load_trig_path(&mut self, path: &str) -> Result<(), Error> {
        info!(path, "Loading TriG file");
        let file = File::open(path)?;
        let buf = BufReader::new(file);
        self.load_trig(buf)?;
        Ok(())
    }

    pub fn load_csv<R: std::io::Read>(&mut self, reader: R, source_model: &str) -> Result<(), Error> {
        let mut reader = csv::Reader::from_reader(reader);
        let header_row = reader.headers()?.to_owned();

        // build a header map to get an index for a specific header name.
        // this is used to derive the entity_id for each record
        let mut headers = HashMap::new();
        for (idx, header) in header_row.iter().enumerate() {
            headers.insert(header.to_string(), idx);
        }

        // a single source can be represented as different data models so we get
        // all the ways to represent the source and load it up
        let mut models = Vec::new();
        for model in self.get_models(source_model)? {
            let entity_id_map = self.get_entity_id_map(&model.to_string())?;
            models.push((model, entity_id_map));
        }

        let prefix = Iri::new(self.map.as_str()).map_err(TransformError::from)?;
        let namespace = Namespace::new(prefix).map_err(TransformError::from)?;

        // add all the records into the dataset with the corresponding header
        // as the predicate and the record id as the subject
        for record in reader.records() {
            let record = record?;

            for (model, entity_id_map) in &models {
                // get the unique record id for this row. we use it to associate
                // all the other fields with it
                let record_id = match entity_id_map {
                    Transform::Hash(ref hasher) => hasher.hash(|field| {
                        let idx = headers.get(field).ok_or(TransformError::NoHeader(field.to_string()))?;
                        let value = record.get(*idx).ok_or(TransformError::NoHeader(field.to_string()))?;
                        Ok(value.to_string())
                    })?,
                };

                // (row_id, header, value)
                // (literal, iri, literal)
                // eg. (spec123, http://.../scientific_name, My species)
                for (idx, value) in record.iter().enumerate() {
                    let header = header_row.get(idx).unwrap();
                    let header_iri = namespace.get(header).map_err(TransformError::from)?;

                    self.source
                        .insert(record_id.as_str(), header_iri, value, Some(model))
                        .map_err(TransformError::from)?;
                }
            }
        }

        Ok(())
    }

    pub fn load_csv_path(&mut self, path: &str) -> Result<(), Error> {
        let file = File::open(path)?;
        let buf = BufReader::new(file);
        self.load_csv(buf, "")?;
        Ok(())
    }

    pub fn get_entity_id_map(&self, graph: &str) -> Result<Transform, TransformError> {
        let base = Iri::new(super::prefix::MAPPING)?.to_base();
        let mapping = Namespace::new(base)?;

        let namespace = Namespace::new(super::prefix::NAMES)?;
        let entity_id = namespace.get("entity_id")?;

        let selector = vec![graph];
        let graph = self.graph(&selector);
        let [_s, p, o] = graph
            .triples_matching([entity_id], Any, Any)
            .next()
            .ok_or(TransformError::MissingEntityId)??;

        let transform = match (o, p) {
            (SimpleTerm::Iri(object), SimpleTerm::Iri(pred)) if mapping.get("hash")? == pred => {
                let fields = vec![Iri::new(object.to_string())?];
                Transform::Hash(FieldHasher { fields })
            }
            _ => unimplemented!(),
        };

        Ok(transform)
    }

    // given a source model, return all the models that it can be transformed into
    pub fn get_models(&self, source_model: &str) -> Result<Vec<Iri<String>>, TransformError> {
        let base = Iri::new(super::prefix::MAPPING)?.to_base();
        let mapping = Namespace::new(base)?;
        let predicate = mapping.get("models")?;

        let prefix = Iri::new(self.map.as_str())?;
        let namespace = Namespace::new(prefix)?;

        let source_ns = Namespace::new(namespace.get("model/")?.to_string())?;
        let source = source_ns.get(source_model)?;

        let mut models = Vec::new();
        for quad in self.source.quads_matching([source], [predicate], Any, Any) {
            let (_g, [_s, _p, o]) = quad?;
            match o {
                SimpleTerm::Iri(iri) => models.push(Iri::new(iri.to_string())?),
                _ => {}
            };
        }

        Ok(models)
    }
}


#[derive(Debug)]
pub struct FieldHasher {
    fields: Vec<Iri<String>>,
}

impl FieldHasher {
    pub fn hash<F>(&self, f: F) -> Result<String, TransformError>
    where
        F: Fn(&str) -> Result<String, TransformError>,
    {
        let mut values = Vec::new();
        for field in self.fields.iter() {
            // the field name is encoded in the map specific iri as the
            // last component. so we get the relative name from the map
            // base iri to get the csv record header name
            //
            // FIXME: use basic string split because relativize doesn't always work
            // https://github.com/oxigraph/oxiri/issues/54
            let (_path, name) = field
                .rsplit_once("/")
                .ok_or(TransformError::InvalidMappingIri(field.to_string()))?;

            let value = f(&name)?;
            values.push(value);
        }

        Ok(values.join(":"))
    }
}

#[derive(Debug)]
pub enum Transform {
    Hash(FieldHasher),
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
            None => true,
        }
    }
}

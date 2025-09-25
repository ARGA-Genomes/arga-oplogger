use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use iref::IriBuf;
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
    pub source: FastDataset,
    map: String,
    schema: IriBuf,
}


impl Dataset {
    pub fn new(map_iri: &str) -> Result<Dataset, TransformError> {
        let source = FastDataset::new();

        Ok(Dataset {
            source,
            map: map_iri.to_string(),
            schema: IriBuf::new(map_iri.to_string())?,
        })
    }

    pub fn scope(&self, models: &[&str]) -> Vec<String> {
        let mut iris: Vec<String> = models.iter().map(|g| format!("{}{}", self.map, g)).collect();

        // also include any source model data based on the model mapping in the schema
        for model in models {
            for iri in self.get_source_models(&model).unwrap() {
                iris.push(format!("{iri}/"));
            }
        }
        iris
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

    /// Loads a CSV dataset as a TriG model.
    ///
    /// This will iterate through all records in the stream and build header
    /// IRIs that can be used as a predicate for each row. The data itself is
    /// loaded into the `http://arga.org.au/model/{source_model}` where source model
    /// is the name provided when loading the CSV, usually the name of the file.
    ///
    /// A record would thus look like this as triples:
    /// ```
    ///   1  http://arga.org.au/schemas/maps/bpa/tissue_id    Liver01
    ///   1  http://arga.org.au/schemas/maps/bpa/tissue_type  Liver
    ///   2  http://arga.org.au/schemas/maps/bpa/tissue_id    Spleen02
    ///   2  http://arga.org.au/schemas/maps/bpa/tissue_type  Spleen
    /// ```
    pub fn load_csv<R: std::io::Read>(&mut self, reader: R, source_model: &str) -> Result<(), Error> {
        // get the source data namespace for all loaded data
        let source = format!("http://arga.org.au/model/{source_model}/");
        let source = Iri::new(source).map_err(TransformError::from)?;
        let schema = Namespace::new(self.schema.as_str()).map_err(TransformError::from)?;

        let mut reader = csv::Reader::from_reader(reader);
        let header_row = reader.headers()?.to_owned();

        // build a header map to get an index for a specific header name.
        // this is used to derive the entity_id for each record
        let mut headers = HashMap::new();
        for (idx, header) in header_row.iter().enumerate() {
            headers.insert(header.to_string(), idx);
        }

        // add all the records into the dataset with the corresponding header
        // as the predicate and the record id as the subject
        for (record_index, record) in reader.records().enumerate() {
            let record = record?;

            // (idx, header, value) = (literal, iri, literal)
            // eg. (123, http://.../scientific_name, My species)
            for (idx, value) in record.iter().enumerate() {
                // don't create a triple for empty data as the absence
                // of a triple is good enough. this improves performance since
                // we wont create indices for empty data and avoids a stack overflow
                // in the sophia library when there are too many columns
                if value.trim().is_empty() {
                    continue;
                }

                let header = header_row.get(idx).expect("CSV field count not consistent");
                let header_iri = schema.get(header).map_err(TransformError::from)?;

                self.source
                    .insert(record_index, header_iri, value, Some(&source))
                    .map_err(TransformError::from)?;
            }
        }

        Ok(())
    }

    pub fn get_source_models(&self, model: &str) -> Result<Vec<Iri<String>>, TransformError> {
        let base = Iri::new("http://arga.org.au/schemas/mapping/")?.to_base();
        let mapping = Namespace::new(base)?;
        let predicate = mapping.get("models")?;

        let prefix = Iri::new(self.map.as_str())?;
        let namespace = Namespace::new(prefix)?;
        let model = namespace.get(model)?;

        let mut sources = Vec::new();
        for quad in self.source.quads_matching(Any, [predicate], [model], Any) {
            let (_g, [s, _p, _o]) = quad?;
            match s {
                SimpleTerm::Iri(iri) => sources.push(Iri::new(iri.to_string())?),
                _ => {}
            };
        }

        Ok(sources)
    }
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

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use iref::IriBuf;
use rayon::prelude::*;
use sophia::api::dataset::Dataset as DatasetTrait;
use sophia::api::graph::adapter::PartialUnionGraph;
use sophia::api::ns::Namespace;
use sophia::api::prelude::*;
use sophia::api::term::matcher::GraphNameMatcher;
use sophia::api::term::{GraphName, SimpleTerm};
use sophia::inmem::dataset::FastDataset;
use sophia::turtle::parser::trig;
use tracing::{debug, info};

use crate::errors::{Error, TransformError};
use crate::transformer::rdf::IntoIriTerm;


pub type PartialGraph<'a> = PartialUnionGraph<&'a FastDataset, GraphIri<'a>>;


pub struct Dataset {
    // pub store: oxigraph::store::Store,
    pub source: FastDataset,
    pub map: String,
    schema: IriBuf,
}


impl Dataset {
    pub fn new(map_iri: &str) -> Result<Dataset, TransformError> {
        let source = FastDataset::new();
        // let store = oxigraph::store::Store::open("./triples.db").unwrap();

        Ok(Dataset {
            // store,
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
                iris.push(format!("{iri}"));
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

    // pub fn load_csv_oxi<R: std::io::Read>(&mut self, reader: R, source_model: &str) -> Result<(), Error> {
    //     let base = format!("http://arga.org.au/model/{source_model}/");
    //     let source = oxigraph::model::NamedNode::new(base.clone()).unwrap();

    //     // let store = oxigraph::store::Store::new().unwrap();
    //     let store = oxigraph::store::Store::open("./triples.db").unwrap();
    //     let mut loader = store.bulk_loader();

    //     let mut reader = csv::Reader::from_reader(reader);
    //     let header_row = reader.headers()?.to_owned();

    //     let mut header_iris = Vec::new();
    //     // build a header map to get an index for a specific header name.
    //     // this is used to derive the entity_id for each record
    //     let mut headers = HashMap::new();
    //     for (idx, header) in header_row.iter().enumerate() {
    //         let header_iri = oxigraph::model::NamedNode::new(format!("{base}{header}")).unwrap();
    //         header_iris.push(header_iri);
    //         headers.insert(header.to_string(), idx);
    //     }

    //     let mut rows: Vec<(oxigraph::model::NamedNode, Vec<oxigraph::model::Literal>)> = Vec::new();
    //     let mut quads: Vec<oxigraph::model::Quad> = Vec::new();

    //     // add all the records into the dataset with the corresponding header
    //     // as the predicate and the record id as the subject
    //     for (record_index, record) in reader.records().enumerate() {
    //         let record = record?;

    //         let index_iri = oxigraph::model::NamedNode::new(format!("{base}{record_index}")).unwrap();
    //         let mut values: Vec<oxigraph::model::Literal> = Vec::new();

    //         // (idx, header, value) = (literal, iri, literal)
    //         // eg. (123, http://.../scientific_name, My species)
    //         for (idx, value) in record.iter().enumerate() {
    //             // don't create a triple for empty data as the absence
    //             // of a triple is good enough. this improves performance since
    //             // we wont create indices for empty data and avoids a stack overflow
    //             // in the sophia library when there are too many columns
    //             if value.trim().is_empty() {
    //                 continue;
    //             }

    //             let header = header_iris.get(idx).unwrap();
    //             let quad = oxigraph::model::Quad::new(
    //                 oxigraph::model::NamedOrBlankNode::NamedNode(index_iri.clone()),
    //                 header.clone(),
    //                 oxigraph::model::Term::Literal(value.trim().into()),
    //                 source.clone(),
    //             );
    //             quads.push(quad);

    //             // values.push(value.trim().into());

    //             // let header = header_row.get(idx).expect("CSV field count not consistent");
    //             // let header_iri = oxigraph::model::NamedNode::new(format!("{base}{header}")).unwrap();
    //             // let index_iri = oxigraph::model::NamedNode::new(format!("{base}{record_index}")).unwrap();

    //             // let quad = oxigraph::model::Quad::new(
    //             //     oxigraph::model::NamedOrBlankNode::NamedNode(index_iri.into()),
    //             //     header_iri,
    //             //     oxigraph::model::Term::Literal(value.into()),
    //             //     source.clone(),
    //             // );
    //             // store.insert(&quad).unwrap();
    //         }


    //         if quads.len() > 1_000_000 {
    //             loader.load_quads(quads).unwrap();
    //             quads = Vec::new();
    //         }

    //         // loader.load_quads(quads).unwrap();
    //         // let index_iri = oxigraph::model::NamedNode::new(format!("{base}{record_index}")).unwrap();
    //         // rows.push((index_iri, values));
    //     }

    //     loader.commit().unwrap();


    //     // let quads: Vec<Vec<oxigraph::model::Quad>> = rows
    //     //     .into_par_iter()
    //     //     .map(|(row_index, values)| {
    //     //         let mut row = Vec::new();

    //     //         for (idx, value) in values.into_iter().enumerate() {
    //     //             if value.value().is_empty() {
    //     //                 continue;
    //     //             }

    //     //             let header = header_iris.get(idx).unwrap();
    //     //             let quad = oxigraph::model::Quad::new(
    //     //                 oxigraph::model::NamedOrBlankNode::NamedNode(row_index.clone()),
    //     //                 header.clone(),
    //     //                 oxigraph::model::Term::Literal(value),
    //     //                 source.clone(),
    //     //             );
    //     //             row.push(quad);
    //     //         }

    //     //         row
    //     //     })
    //     //     .collect();


    //     // for (row_index, values) in rows.into_iter() {
    //     //     for (idx, value) in values.into_iter().enumerate() {
    //     //         if value.value().is_empty() {
    //     //             continue;
    //     //         }

    //     //         let header = header_iris.get(idx).unwrap();
    //     //         let quad = oxigraph::model::Quad::new(
    //     //             oxigraph::model::NamedOrBlankNode::NamedNode(row_index.clone()),
    //     //             header.clone(),
    //     //             oxigraph::model::Term::Literal(value),
    //     //             source.clone(),
    //     //         );

    //     //         store.insert(&quad).unwrap();
    //     //     }
    //     // }


    //     info!("Finished loading");
    //     Ok(())
    // }

    pub fn load<R: std::io::Read>(&mut self, reader: R, source_model: &str) -> Result<usize, Error> {
        if source_model.ends_with(".jsonl") {
            Ok(self.load_jsonl(reader, source_model)?)
        }
        else {
            self.load_csv(reader, source_model)
        }
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
    pub fn load_csv<R: std::io::Read>(&mut self, reader: R, source_model: &str) -> Result<usize, Error> {
        // get the source data namespace for all loaded data
        let source = format!("http://arga.org.au/source/{source_model}");
        let source = Iri::new(source).map_err(TransformError::from)?;
        let schema = Namespace::new(self.schema.as_str()).map_err(TransformError::from)?;

        let mut reader = if source_model == "assembly_summary_genbank.txt" {
            csv::ReaderBuilder::new().delimiter(b'\t').from_reader(reader)
        }
        else {
            csv::ReaderBuilder::new().from_reader(reader)
        };

        let header_row = reader.headers()?.to_owned();

        // build a header map to get a specific header iri from the column index
        let mut headers = Vec::new();
        for header in header_row.iter() {
            let header_iri = schema.get(header).map_err(TransformError::from)?;
            headers.push(header_iri);
        }

        let mut total = 0;

        // add all the records into the dataset with the corresponding header
        // as the predicate and the record id as the subject
        for (record_index, record) in reader.records().enumerate() {
            let record = record?;
            total = total + 1;

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

                // this is infallible as the csv reader will fail early if the
                // column count isn't the same for all rows
                let header = headers[idx];

                self.source
                    .insert(record_index, header, value, Some(&source))
                    .map_err(TransformError::from)?;
            }
        }

        Ok(total)
    }

    pub fn load_jsonl<R: std::io::Read>(&mut self, reader: R, source: &str) -> Result<usize, TransformError> {
        // get the source data namespace for all loaded data
        let source = format!("http://arga.org.au/source/{source}");
        let source = Iri::new(source).map_err(TransformError::from)?;
        let schema = Namespace::new(self.schema.as_str()).map_err(TransformError::from)?;

        let buf = BufReader::new(reader);
        let mut total = 0;

        for (record_index, line) in buf.lines().enumerate() {
            let record = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&line?)?;
            total = total + 1;

            for (key, value) in record {
                let header = schema.get(&key)?;

                match value {
                    serde_json::Value::Bool(value) => continue,
                    serde_json::Value::Number(value) => continue,
                    serde_json::Value::String(value) => {
                        if value.trim().is_empty() {
                            continue;
                        }
                        self.source
                            .insert(record_index, header, value.as_str(), Some(&source))?;
                    }
                    serde_json::Value::Array(values) => continue,
                    serde_json::Value::Object(map) => continue,
                    serde_json::Value::Null => continue,
                }
            }
        }

        Ok(total)
    }

    fn get_source_models(&self, model: &str) -> Result<Vec<Iri<String>>, TransformError> {
        let base = Iri::new("http://arga.org.au/schemas/mapping/")?.to_base();
        let mapping = Namespace::new(base)?;
        let predicate = mapping.get("transforms_into")?;

        let prefix = Iri::new(self.map.as_str())?;
        let namespace = Namespace::new(prefix)?;
        let model = namespace.get(model)?;

        info!(?predicate, ?model, "getting sources");

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

    pub fn get_source_from_model(&self, model: &iref::Iri) -> Result<Vec<iref::IriBuf>, TransformError> {
        debug!(?model, "getting source from model");

        let base = Iri::new("http://arga.org.au/schemas/mapping/")?.to_base();
        let mapping = Namespace::new(base)?;
        let predicate = mapping.get("transforms_into")?;

        let mut sources = Vec::new();
        for quad in self
            .source
            .quads_matching(Any, [predicate], [model.into_iri_term()?], Any)
        {
            let (_g, [s, _p, _o]) = quad?;
            match s {
                SimpleTerm::Iri(iri) => sources.push(iref::IriBuf::new(format!("{0}", iri.to_string()))?),
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

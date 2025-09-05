use sophia::api::ns::Namespace;
use sophia::api::prelude::*;
use sophia::api::sparql::Query;
use sophia::sparql::{SparqlQuery, SparqlWrapper};
use sophia::term::{ArcTerm, GenericLiteral};

use super::dataset::Dataset;
use crate::errors::{Error, TransformError};


const SAME: &'static str = "http://arga.org.au/schemas/mapping/same";
const JOIN: &'static str = "http://arga.org.au/schemas/mapping/join";
const LINKS: &'static str = "http://arga.org.au/schemas/mapping/links";
const VIA: &'static str = "http://arga.org.au/schemas/mapping/via";


#[derive(Debug, Clone)]
pub enum Value {
    Iri(String),
    Literal(Literal),
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
}

pub type SparqlRow = Vec<Option<Value>>;


pub struct Mapped {
    pub dataset: Dataset,
}

impl Mapped {
    pub fn new(prefix: &str, map: &str, data: &str) -> Result<Mapped, Error> {
        let mut dataset = Dataset::new(prefix);
        dataset.load_trig_path(map)?;
        dataset.load_csv_path(data)?;

        Ok(Mapped { dataset })
    }

    pub fn query(&self, query: &str, graphs: &Vec<&str>) -> Result<Vec<SparqlRow>, TransformError> {
        let graph = self.dataset.graph(graphs);
        let graph = graph.as_dataset();
        let dataset = SparqlWrapper(&graph);
        let query = SparqlQuery::parse(query)?;

        let mut rows = Vec::new();
        let bindings = dataset.query(&query)?.into_bindings();

        for binding in bindings {
            let binding = binding?;
            let mut row = Vec::new();

            for atom in binding {
                match atom {
                    Some(result) => match result.inner() {
                        ArcTerm::Literal(lit) => match lit {
                            GenericLiteral::Typed(t, _) => {
                                row.push(Some(Value::Literal(Literal::String(t.to_string()))))
                            }
                            _ => unimplemented!(),
                        },
                        ArcTerm::Iri(iri) => row.push(Some(Value::Iri(iri.to_string()))),
                        t => unimplemented!("Unsupported result type: {t:?}"),
                    },
                    None => row.push(None),
                }
            }

            rows.push(row);
        }

        Ok(rows)
    }

    pub fn get_values(&self, field: &str, graphs: &Vec<&str>) -> Result<Vec<(Literal, Literal)>, Error> {
        let rows = self.query(
            &format!(
                r#"
PREFIX : <http://arga.org.au/schemas/mapping/>
PREFIX names: <http://arga.org.au/schemas/names/>

SELECT ?s ?o WHERE {{
  names:{field} :same ?mapped.
  ?s ?mapped ?o.
}}
"#,
            ),
            graphs,
        )?;

        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let (Some(sub), Some(obj)) = (row.get(0).unwrap(), row.get(1).unwrap())
            else {
                continue;
            };

            match (sub, obj) {
                (Value::Literal(sub), Value::Literal(obj)) => {
                    results.push((sub.clone(), obj.clone()));
                }
                _ => {}
            }
        }

        Ok(results)
    }
}

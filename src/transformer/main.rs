mod dataset;
mod error;
mod mapped;


use std::collections::HashMap;

use error::Error;
use mapped::{Literal, Mapped};
use sophia::api::prelude::*;
use sophia::api::term::SimpleTerm;
use sophia::api::term::matcher::GraphNameMatcher;


fn main() -> Result<(), Error> {
    let dataset = Mapped::new(dataset::prefix::TSI, "rdf/specimens.ttl", "organisms.csv")?;
    let mut records: HashMap<String, Vec<(String, Literal)>> = HashMap::new();

    for (sub, obj) in dataset.get_values("sex")? {
        let Literal::String(sub) = sub;
        let atom = ("sex".to_string(), obj);
        records
            .entry(sub)
            .and_modify(|v| v.push(atom.clone()))
            .or_insert(vec![atom]);
    }

    for (sub, obj) in dataset.get_values("scientific_name")? {
        let Literal::String(sub) = sub;
        let atom = ("scientific_name".to_string(), obj);
        records
            .entry(sub)
            .and_modify(|v| v.push(atom.clone()))
            .or_insert(vec![atom]);
    }

    for (sub, obj) in dataset.get_values("live_state")? {
        let Literal::String(sub) = sub;
        let atom = ("live_state".to_string(), obj);
        records
            .entry(sub)
            .and_modify(|v| v.push(atom.clone()))
            .or_insert(vec![atom]);
    }

    for (sub, obj) in dataset.get_values("life_stage")? {
        let Literal::String(sub) = sub;
        let atom = ("life_stage".to_string(), obj);
        records
            .entry(sub)
            .and_modify(|v| v.push(atom.clone()))
            .or_insert(vec![atom]);
    }

    for (sub, obj) in dataset.get_values("remarks")? {
        let Literal::String(sub) = sub;
        let atom = ("remarks".to_string(), obj);
        records
            .entry(sub)
            .and_modify(|v| v.push(atom.clone()))
            .or_insert(vec![atom]);
    }

    println!("{records:#?}");

    Ok(())
}


#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Iri {
    pub field: String,
    pub base: String,
}

impl std::fmt::Display for Iri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.base, self.field)
    }
}

impl Iri {
    pub fn new(iri: &str) -> Result<Iri, Error> {
        let (prefix, suffix) = iri
            .rsplit_once("/")
            .ok_or_else(|| Error::InvalidMappingIri(iri.to_string()))?;
        Ok(Iri {
            base: format!("{}/", prefix),
            field: suffix.to_string(),
        })
    }
}


fn dump_graph<M: GraphNameMatcher + Copy>(graph: &dataset::PartialGraph) {
    println!("-----------------------------");
    for triple in graph.triples() {
        dump_triple(triple.unwrap());
    }
    println!("-----------------------------");
}


fn dump_triple(triple: [&SimpleTerm<'_>; 3]) {
    let [s, p, o] = triple;

    let subject = match s {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        SimpleTerm::BlankNode(bnode_id) => bnode_id.as_str(),
        SimpleTerm::LiteralDatatype(lit, _t) => lit,
        _ => unimplemented!(),
    };

    let predicate = match p {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        _ => unimplemented!(),
    };

    let object = match o {
        SimpleTerm::Iri(iri_ref) => iri_ref.as_str(),
        SimpleTerm::BlankNode(bnode_id) => bnode_id.as_str(),
        SimpleTerm::LiteralDatatype(lit, _t) => lit,
        _ => unimplemented!(),
    };

    println!("{subject} {predicate} {object}")
}

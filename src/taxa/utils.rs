use arga_core::models::{TaxonomicRank, TaxonomicStatus};
use serde::Deserialize;

use crate::errors::ParseError;


pub fn taxonomic_rank_from_str<'de, D>(deserializer: D) -> Result<TaxonomicRank, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    str_to_taxonomic_rank(&s).map_err(serde::de::Error::custom)
}

pub fn taxonomic_status_from_str<'de, D>(deserializer: D) -> Result<TaxonomicStatus, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    str_to_taxonomic_status(&s).map_err(serde::de::Error::custom)
}


pub fn str_to_taxonomic_rank(value: &str) -> Result<TaxonomicRank, ParseError> {
    use TaxonomicRank::*;

    match value.to_lowercase().as_str() {
        "domain" => Ok(Domain),
        "superkingdom" => Ok(Superkingdom),
        "kingdom" => Ok(Kingdom),
        "subkingdom" => Ok(Subkingdom),
        "infrakingdom" => Ok(Infrakingdom),
        "superphylum" => Ok(Superphylum),
        "phylum" => Ok(Phylum),
        "subphylum" => Ok(Subphylum),
        "infraphylum" => Ok(Infraphylum),
        "parvphylum" => Ok(Parvphylum),
        "gigaclass" => Ok(Gigaclass),
        "megaclass" => Ok(Megaclass),
        "superclass" => Ok(Superclass),
        "class" => Ok(Class),
        "subclass" => Ok(Subclass),
        "infraclass" => Ok(Infraclass),
        "subterclass" => Ok(Subterclass),
        "superorder" => Ok(Superorder),
        "order" => Ok(Order),
        "hyporder" => Ok(Hyporder),
        "minorder" => Ok(Minorder),
        "suborder" => Ok(Suborder),
        "infraorder" => Ok(Infraorder),
        "parvorder" => Ok(Parvorder),
        "epifamily" => Ok(Epifamily),
        "superfamily" => Ok(Superfamily),
        "family" => Ok(Family),
        "subfamily" => Ok(Subfamily),
        "supertribe" => Ok(Supertribe),
        "tribe" => Ok(Tribe),
        "subtribe" => Ok(Subtribe),
        "genus" => Ok(Genus),
        "subgenus" => Ok(Subgenus),
        "species" => Ok(Species),
        "subspecies" => Ok(Subspecies),
        "variety" => Ok(Variety),
        "subvariety" => Ok(Subvariety),
        "natio" => Ok(Natio),
        "mutatio" => Ok(Mutatio),
        "unranked" => Ok(Unranked),
        "higher taxon" => Ok(HigherTaxon),
        "aggregate genera" => Ok(AggregateGenera),
        "aggregate species" => Ok(AggregateSpecies),
        "cohort" => Ok(Cohort),
        "subcohort" => Ok(Subcohort),
        "division" => Ok(Division),
        "phylum (division)" => Ok(Division),
        "incertae sedis" => Ok(IncertaeSedis),
        "infragenus" => Ok(Infragenus),
        "section" => Ok(Section),
        "subsection" => Ok(Subsection),
        "subdivision" => Ok(Subdivision),
        "subphylum (subdivision)" => Ok(Subdivision),

        "regnum" => Ok(Regnum),
        "familia" => Ok(Familia),
        "classis" => Ok(Classis),
        "ordo" => Ok(Ordo),
        "varietas" => Ok(Varietas),
        "forma" => Ok(Forma),
        "subforma" => Ok(Subforma),
        "subclassis" => Ok(Subclassis),
        "superordo" => Ok(Superordo),
        "sectio" => Ok(Sectio),
        "subsectio" => Ok(Subsectio),
        "nothovarietas" => Ok(Nothovarietas),
        "subvarietas" => Ok(Subvarietas),
        "series" => Ok(Series),
        "subseries" => Ok(Subseries),
        "superspecies" => Ok(Superspecies),
        "infraspecies" => Ok(Infraspecies),
        "subfamilia" => Ok(Subfamilia),
        "subordo" => Ok(Subordo),
        "regio" => Ok(Regio),
        "special form" => Ok(SpecialForm),

        "" => Ok(Unranked),

        val => Err(ParseError::InvalidValue(val.to_string())),
    }
}


pub fn str_to_taxonomic_status(value: &str) -> Result<TaxonomicStatus, ParseError> {
    use TaxonomicStatus::*;

    match value.to_lowercase().as_str() {
        "valid" => Ok(Accepted),
        "valid name" => Ok(Accepted),
        "accepted" => Ok(Accepted),
        "accepted name" => Ok(Accepted),

        "undescribed" => Ok(Undescribed),
        "species inquirenda" => Ok(SpeciesInquirenda),
        "taxon inquirendum" => Ok(TaxonInquirendum),
        "manuscript name" => Ok(ManuscriptName),
        "hybrid" => Ok(Hybrid),

        "unassessed" => Ok(Unassessed),
        "unavailable name" => Ok(Unavailable),
        "uncertain" => Ok(Uncertain),
        "unjustified emendation" => Ok(UnjustifiedEmendation),

        "synonym" => Ok(Synonym),
        "junior synonym" => Ok(Synonym),
        "junior objective synonym" => Ok(Synonym),
        "junior subjective synonym" => Ok(Synonym),
        "later synonym" => Ok(Synonym),

        "homonym" => Ok(Homonym),
        "junior homonym" => Ok(Homonym),
        "unreplaced junior homonym" => Ok(Homonym),

        "invalid" => Ok(Unaccepted),
        "invalid name" => Ok(Unaccepted),
        "unaccepted" => Ok(Unaccepted),
        "unaccepted name" => Ok(Unaccepted),
        // "excluded" => Ok(Unaccepted),
        "informal" => Ok(Informal),
        "informal name" => Ok(Informal),

        "placeholder" => Ok(Placeholder),
        "temporary name" => Ok(Placeholder),

        "basionym" => Ok(Basionym),
        "nomenclatural synonym" => Ok(NomenclaturalSynonym),
        "taxonomic synonym" => Ok(TaxonomicSynonym),
        "replaced synonym" => Ok(ReplacedSynonym),

        "incorrect original spelling" => Ok(Misspelled),
        "misspelling" => Ok(Misspelled),

        "orthographic variant" => Ok(OrthographicVariant),
        "excluded" => Ok(Excluded),

        "misapplied" => Ok(Misapplied),
        "misapplication" => Ok(Misapplied),
        "alternative name" => Ok(AlternativeName),
        "alternative representation" => Ok(AlternativeName),

        "pro parte misapplied" => Ok(ProParteMisapplied),
        "pro parte taxonomic synonym" => Ok(ProParteTaxonomicSynonym),

        "doubtful misapplied" => Ok(DoubtfulMisapplied),
        "doubtful taxonomic synonym" => Ok(DoubtfulTaxonomicSynonym),
        "doubtful pro parte misapplied" => Ok(DoubtfulProParteMisapplied),
        "doubtful pro parte taxonomic synonym" => Ok(DoubtfulProParteTaxonomicSynonym),

        "nomen dubium" => Ok(NomenDubium),
        "nomen nudum" => Ok(NomenNudum),
        "nomen oblitum" => Ok(NomenOblitum),

        "interim unpublished" => Ok(InterimUnpublished),
        "superseded combination" => Ok(SupersededCombination),
        "superseded rank" => Ok(SupersededRank),
        "incorrect grammatical agreement of specific epithet" => Ok(IncorrectGrammaticalAgreementOfSpecificEpithet),

        val => Err(ParseError::InvalidValue(val.to_string())),
    }
}

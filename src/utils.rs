use std::time::Duration;

use arga_core::models::{NomenclaturalActType, TaxonomicRank, TaxonomicStatus};
use chrono::{DateTime, Utc};
use heck::ToTitleCase;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;

use crate::errors::ParseError;

pub static PROGRESS_TEMPLATE: &str = "[{elapsed_precise}] {bar:40.cyan/blue} {human_pos:>7}/{human_len:7} {msg}";
pub static SPINNER_TEMPLATE: &str = "[{elapsed_precise}] {spinner:2.cyan/blue} {msg}";
pub static SPINNER_TOTALS_TEMPLATE: &str = "{spinner:2.cyan/blue} {msg}: {human_pos}";
pub static BYTES_PROGRESS_TEMPLATE: &str = "[{elapsed_precise}] {bar:40.cyan/blue} {decimal_bytes:>7}/{decimal_total_bytes:7} @ {decimal_bytes_per_sec} [eta: {eta}] {msg}";


#[macro_export]
macro_rules! frame_push_opt {
    ($frame:ident, $discriminant:ident, $field:expr) => {
        if let Some(value) = $field {
            $frame.push($discriminant(value));
        }
    };
}


pub fn new_spinner(message: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(SPINNER_TEMPLATE).expect("Invalid spinner template");
    let spinner = ProgressBar::new_spinner()
        .with_message(message.to_string())
        .with_style(style);

    spinner.enable_steady_tick(Duration::from_millis(100));
    spinner
}

pub fn new_progress_bar(total: usize, message: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(PROGRESS_TEMPLATE).expect("Invalid progress bar template");
    ProgressBar::new(total as u64)
        .with_message(message.to_string())
        .with_style(style)
}

pub fn new_progress_bar_bytes(total: usize, message: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(BYTES_PROGRESS_TEMPLATE).expect("Invalid progress bar template");
    ProgressBar::new(total as u64)
        .with_message(message.to_string())
        .with_style(style)
}

pub fn new_spinner_totals(message: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(SPINNER_TOTALS_TEMPLATE).expect("Invalid spinner template");
    let spinner = ProgressBar::new_spinner()
        .with_message(message.to_string())
        .with_style(style);

    spinner.enable_steady_tick(Duration::from_millis(100));
    spinner
}


#[derive(Clone)]
pub struct FrameImportBars {
    _bars: MultiProgress,
    pub bytes: ProgressBar,
    pub operations: ProgressBar,
    pub inserted: ProgressBar,
    pub frames: ProgressBar,
}

impl FrameImportBars {
    pub fn new(total_bytes: usize) -> FrameImportBars {
        let bars = MultiProgress::new();
        let bytes = new_progress_bar_bytes(total_bytes, "Importing");
        let operations = new_spinner_totals("Total operations");
        let inserted = new_spinner_totals("Operations inserted");
        let frames = new_spinner_totals("Frames read");
        bars.add(bytes.clone());
        bars.add(operations.clone());
        bars.add(inserted.clone());
        bars.add(frames.clone());

        bytes.enable_steady_tick(Duration::from_millis(200));

        FrameImportBars {
            _bars: bars,
            bytes,
            operations,
            inserted,
            frames,
        }
    }

    pub fn finish(&self) {
        self.bytes.finish();
        self.operations.finish();
        self.inserted.finish();
        self.frames.finish();
    }
}


/// Convert the case of the first word to a title case.
/// This will also replace all unicode whitespaces with ASCII compatible whitespace
/// which means it also works as a sort of normalizer
pub fn titleize_first_word(text: &str) -> String {
    let mut converted: Vec<String> = Vec::new();
    let mut words = text.split_whitespace();

    if let Some(word) = words.next() {
        converted.push(word.to_title_case());
    }
    for word in words {
        converted.push(word.to_string());
    }

    converted.join(" ")
}

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

pub fn nomenclatural_act_from_str<'de, D>(deserializer: D) -> Result<NomenclaturalActType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    str_to_nomenclatural_act(&s).map_err(serde::de::Error::custom)
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

        "form" => Ok(Forma),
        "subform" => Ok(Subforma),
        "section zoology" => Ok(Section),
        "subsection zoology" => Ok(Subsection),
        "division zoology" => Ok(Division),
        "section botany" => Ok(Sectio),
        "subsection botany" => Ok(Subsectio),
        "nothovariety" => Ok(Nothovarietas),
        "forma specialis" => Ok(SpecialForm),
        "pathovar" => Ok(Pathovar),
        "serovar" => Ok(Serovar),
        "biovar" => Ok(Biovar),
        "species aggregate" => Ok(AggregateSpecies),
        "infraspecific name" => Ok(Infraspecies),
        "other" => Ok(Unranked),

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
        "provisionally accepted" => Ok(Accepted),

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
        "ambiguous synonym" => Ok(Synonym),

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
        "unsourced misapplied" => Ok(Misapplied),
        "alternative name" => Ok(AlternativeName),
        "alternative representation" => Ok(AlternativeName),

        "pro parte misapplied" => Ok(ProParteMisapplied),
        "unsourced pro parte misapplied" => Ok(ProParteMisapplied),
        "pro parte taxonomic synonym" => Ok(ProParteTaxonomicSynonym),

        "doubtful misapplied" => Ok(DoubtfulMisapplied),
        "unsourced doubtful misapplied" => Ok(DoubtfulMisapplied),
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

pub fn str_to_nomenclatural_act(value: &str) -> Result<NomenclaturalActType, ParseError> {
    use NomenclaturalActType::*;

    match value.to_lowercase().as_str() {
        "species_nova" => Ok(SpeciesNova),
        "subspecies_nova" => Ok(SubspeciesNova),
        "genus_species_nova" => Ok(GenusSpeciesNova),
        "combinatio_nova" => Ok(CombinatioNova),
        "revived_status" => Ok(RevivedStatus),
        "name_usage" => Ok(NameUsage),
        "new_species" => Ok(SpeciesNova),
        "genus_transfer" => Ok(CombinatioNova),
        "subgenus_placement" => Ok(SubgenusPlacement),

        val => Err(ParseError::InvalidValue(val.to_string())),
    }
}

pub fn parse_date_time(value: &str) -> Result<DateTime<Utc>, ParseError> {
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%z") {
        return Ok(datetime.into());
    }
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%#z") {
        return Ok(datetime.into());
    }
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.3f%#z") {
        return Ok(datetime.into());
    }
    // format used in afd
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y%m%dT%H:%M:%S%.3f%#z") {
        return Ok(datetime.into());
    }
    // rfc3339 doesn't include millis so we support the deviation here
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.3f%#z") {
        return Ok(datetime.into());
    }

    Ok(DateTime::parse_from_rfc3339(value)?.into())
}

pub fn date_time_from_str_opt<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Deserialize::deserialize(deserializer)?;

    Ok(match s {
        None => None,
        Some(s) => match parse_date_time(&s) {
            Ok(date) => Some(date),
            Err(_) => None,
        },
    })
}

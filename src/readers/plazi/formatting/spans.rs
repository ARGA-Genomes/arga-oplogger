use tracing::warn;

use super::prelude::*;
use super::sections::prelude::*;


#[derive(Debug)]
pub struct SpanProperties {
    pub bold: bool,
    pub italics: bool,
}

#[derive(Debug)]
pub enum Span {
    Root(Vec<Span>),
    SubSection(Vec<Span>),
    Paragraph(Vec<Span>),
    Heading(Vec<Span>),
    Title(Vec<Span>),
    Emphasis(Vec<Span>),
    SmallCaps(Vec<Span>),
    Superscript(Vec<Span>),
    Subscript(Vec<Span>),
    TaxonomicName(Vec<Span>),
    PageBreakToken {
        attributes: PageBreakToken,
        children: Vec<Span>,
    },
    TreatmentCitationGroup(Vec<Span>),
    TreatmentCitation(Vec<Span>),
    MaterialsCitation(Vec<Span>),
    FigureCitation(Vec<Span>),
    TaxonNameAuthority(Vec<Span>),
    Uri(Vec<Span>),
    BibRef(Vec<Span>),
    KeyLead(Vec<Span>),
    KeyStep(Vec<Span>),
    DeterminerName(Vec<Span>),

    // TODO: process the attributes of these and determine
    // if they can have nested formatting or not
    JournalOrPublisher(Vec<Span>),
    Year(Vec<Span>),
    Part(Vec<Span>),
    Pagination(Vec<Span>),
    AccessionNumber(Vec<Span>),
    Number(Vec<Span>),
    Author(Vec<Span>),

    Text(String),
    Uuid(String),
    NormalizedToken(String),
    PageStartToken(String),
    Authority(String),

    BibRefCitation {
        attributes: Citation,
        children: Vec<Span>,
    },

    BibCitation {
        attributes: BibCitation,
        children: Vec<Span>,
    },

    Table(Vec<Span>),
    Tr(Vec<Span>),
    Th(Vec<Span>),
    Td(Vec<Span>),

    Quantity(String),
    Date(String),
    CollectingRegion(String),
    CollectingCountry {
        attributes: CollectingCountry,
        children: Vec<Span>,
    },

    CollectionCode {
        attributes: CollectionCode,
        children: Vec<Span>,
    },

    SpecimenCode(SpecimenCode),

    TypeStatus {
        attributes: TypeStatus,
        children: Vec<Span>,
    },
}

impl Span {
    pub fn paragraph() -> Self {
        Self::Paragraph(Vec::new())
    }

    pub fn subsection() -> Self {
        Self::Paragraph(Vec::new())
    }

    pub fn heading() -> Self {
        Self::Heading(Vec::new())
    }

    pub fn title() -> Self {
        Self::Title(Vec::new())
    }

    pub fn emphasis() -> Self {
        Self::Emphasis(Vec::new())
    }

    pub fn small_caps() -> Self {
        Self::SmallCaps(Vec::new())
    }

    pub fn superscript() -> Self {
        Self::Superscript(Vec::new())
    }

    pub fn subscript() -> Self {
        Self::Subscript(Vec::new())
    }

    pub fn taxonomic_name() -> Self {
        Self::TaxonomicName(Vec::new())
    }

    pub fn page_break_token(attributes: PageBreakToken, children: Vec<Span>) -> Self {
        Self::PageBreakToken { attributes, children }
    }

    pub fn treatment_citation_group() -> Self {
        Self::TreatmentCitationGroup(Vec::new())
    }

    pub fn treatment_citation() -> Self {
        Self::TreatmentCitation(Vec::new())
    }

    pub fn materials_citation() -> Self {
        Self::MaterialsCitation(Vec::new())
    }

    pub fn figure_citation() -> Self {
        Self::FigureCitation(Vec::new())
    }

    pub fn taxon_name_authority() -> Self {
        Self::TaxonNameAuthority(Vec::new())
    }

    pub fn bib_ref(children: Vec<Span>) -> Self {
        Self::BibRef(children)
    }

    pub fn key_lead() -> Self {
        Self::KeyLead(Vec::new())
    }

    pub fn key_step() -> Self {
        Self::KeyStep(Vec::new())
    }

    pub fn determiner_name() -> Self {
        Self::DeterminerName(Vec::new())
    }

    pub fn uri(children: Vec<Span>) -> Self {
        Self::Uri(children)
    }

    pub fn text(text: &str) -> Self {
        Self::Text(text.to_string())
    }

    pub fn uuid(text: &str) -> Self {
        Self::Uuid(text.to_string())
    }

    pub fn citation(attributes: Citation, children: Vec<Span>) -> Self {
        Self::BibRefCitation { attributes, children }
    }

    pub fn bib_citation(attributes: BibCitation, children: Vec<Span>) -> Self {
        Self::BibCitation { attributes, children }
    }

    pub fn collection_code(attributes: CollectionCode, children: Vec<Span>) -> Self {
        Self::CollectionCode { attributes, children }
    }

    pub fn specimen_code(span: SpecimenCode) -> Self {
        Self::SpecimenCode(span)
    }

    pub fn type_status(attributes: TypeStatus, children: Vec<Span>) -> Self {
        Self::TypeStatus { attributes, children }
    }

    pub fn normalized_token(text: &str) -> Self {
        Self::NormalizedToken(text.to_string())
    }

    pub fn page_start_token(text: &str) -> Self {
        Self::PageStartToken(text.to_string())
    }

    pub fn authority(text: &str) -> Self {
        Self::Authority(text.to_string())
    }

    pub fn table() -> Self {
        Self::Table(Vec::new())
    }

    pub fn tr() -> Self {
        Self::Tr(Vec::new())
    }

    pub fn th() -> Self {
        Self::Th(Vec::new())
    }

    pub fn td() -> Self {
        Self::Td(Vec::new())
    }

    pub fn quantity(text: &str) -> Self {
        Self::Quantity(text.to_string())
    }

    pub fn date(text: &str) -> Self {
        Self::Date(text.to_string())
    }

    pub fn collecting_region(text: &str) -> Self {
        Self::CollectingRegion(text.to_string())
    }

    pub fn collecting_country(attributes: CollectingCountry, children: Vec<Span>) -> Self {
        Self::CollectingCountry { attributes, children }
    }

    pub fn journal_or_publisher() -> Self {
        Self::JournalOrPublisher(Vec::new())
    }

    pub fn year() -> Self {
        Self::Year(Vec::new())
    }

    pub fn part() -> Self {
        Self::Part(Vec::new())
    }

    pub fn pagination() -> Self {
        Self::Pagination(Vec::new())
    }

    pub fn accession_number() -> Self {
        Self::AccessionNumber(Vec::new())
    }

    pub fn number() -> Self {
        Self::Number(Vec::new())
    }

    pub fn author() -> Self {
        Self::Author(Vec::new())
    }

    pub fn push_child(&mut self, child: Span) {
        use Span::*;

        match self {
            Root(children) => children.push(child),
            SubSection(children) => children.push(child),
            Paragraph(children) => children.push(child),
            Heading(children) => children.push(child),
            Title(children) => children.push(child),
            Emphasis(children) => children.push(child),
            SmallCaps(children) => children.push(child),
            Superscript(children) => children.push(child),
            Subscript(children) => children.push(child),
            TaxonomicName(children) => children.push(child),
            PageBreakToken { children, .. } => children.push(child),
            TreatmentCitationGroup(children) => children.push(child),
            TreatmentCitation(children) => children.push(child),
            MaterialsCitation(children) => children.push(child),
            FigureCitation(children) => children.push(child),
            TaxonNameAuthority(children) => children.push(child),
            BibRefCitation { children, .. } => children.push(child),
            BibCitation { children, .. } => children.push(child),
            CollectionCode { children, .. } => children.push(child),
            SpecimenCode(span) => span.children.push(child),
            TypeStatus { children, .. } => children.push(child),
            BibRef(children) => children.push(child),
            KeyLead(children) => children.push(child),
            KeyStep(children) => children.push(child),
            DeterminerName(children) => children.push(child),

            JournalOrPublisher(children) => children.push(child),
            Year(children) => children.push(child),
            Part(children) => children.push(child),
            Pagination(children) => children.push(child),
            AccessionNumber(children) => children.push(child),
            Number(children) => children.push(child),
            Author(children) => children.push(child),

            Text(_) => warn!("Ignoring attempt to push a child into a Text span"),
            Uri(_) => warn!("Ignoring attempt to push a child into a Uri span"),
            Uuid(_) => warn!("Ignoring attempt to push a child into a Uuid span"),
            NormalizedToken(_) => warn!("Ignoring attempt to push a child into a NormalizedToken span"),
            PageStartToken(_) => warn!("Ignoring attempt to push a child into a PageStartToken span"),
            Authority(_) => warn!("Ignoring attempt to push a child into a PageStartToken span"),

            Table(children) => children.push(child),
            Tr(children) => children.push(child),
            Th(children) => children.push(child),
            Td(children) => children.push(child),

            Quantity(_) => warn!("Ignoring attempt to push a child into a Quantity span"),
            CollectingRegion(_) => warn!("Ignoring attempt to push a child into a CollectingRegion span"),
            CollectingCountry { children, .. } => children.push(child),
            Date(_) => warn!("Ignoring attempt to push a child into a Date span"),
        }
    }
}


impl ToString for Span {
    fn to_string(&self) -> String {
        use Span::*;

        match self {
            Text(text) => text.clone(),
            Uuid(text) => text.clone(),
            NormalizedToken(text) => text.clone(),
            PageStartToken(text) => text.clone(),
            Authority(text) => text.clone(),
            Quantity(text) => text.clone(),
            Date(text) => text.clone(),
            CollectingRegion(text) => text.clone(),

            Root(children) => flatten_span(children),
            SubSection(children) => flatten_span(children),
            Paragraph(children) => flatten_span(children),
            Heading(children) => flatten_span(children),
            Title(children) => flatten_span(children),
            Emphasis(children) => flatten_span(children),
            SmallCaps(children) => flatten_span(children),
            Superscript(children) => flatten_span(children),
            Subscript(children) => flatten_span(children),
            TaxonomicName(children) => flatten_span(children),
            TaxonNameAuthority(children) => flatten_span(children),
            TreatmentCitationGroup(children) => flatten_span(children),
            TreatmentCitation(children) => flatten_span(children),
            FigureCitation(children) => flatten_span(children),
            Uri(children) => flatten_span(children),
            BibRef(children) => flatten_span(children),
            KeyLead(children) => flatten_span(children),
            KeyStep(children) => flatten_span(children),
            DeterminerName(children) => flatten_span(children),
            JournalOrPublisher(children) => flatten_span(children),
            Year(children) => flatten_span(children),
            Part(children) => flatten_span(children),
            Pagination(children) => flatten_span(children),
            AccessionNumber(children) => flatten_span(children),
            Number(children) => flatten_span(children),
            Author(children) => flatten_span(children),
            Table(children) => flatten_span(children),
            Tr(children) => flatten_span(children),
            Th(children) => flatten_span(children),
            Td(children) => flatten_span(children),
            MaterialsCitation(children) => flatten_span(children),
            SpecimenCode(code) => flatten_span(&code.children),

            PageBreakToken { children, .. } => flatten_span(children),
            BibRefCitation { children, .. } => flatten_span(children),
            BibCitation { children, .. } => flatten_span(children),
            CollectingCountry { children, .. } => flatten_span(children),
            CollectionCode { children, .. } => flatten_span(children),
            TypeStatus { children, .. } => flatten_span(children),
        }
    }
}


fn flatten_span(children: &Vec<Span>) -> String {
    children
        .iter()
        .map(|child| child.to_string().trim().to_string())
        .collect::<Vec<String>>()
        .join(" ")
        .trim()
        .to_string()
}


#[derive(Debug)]
pub struct SpanStack {
    stack: Vec<Span>,
}

impl SpanStack {
    pub fn new() -> SpanStack {
        let root = Span::Root(vec![]);
        SpanStack { stack: vec![root] }
    }

    pub fn extend(&mut self, children: Vec<Span>) {
        self.stack.extend(children);
    }

    pub fn push(&mut self, child: Span) {
        use Span::*;

        let commit = match child {
            Text(_) => true,
            Uri(_) => true,
            Uuid(_) => true,
            NormalizedToken(_) => true,
            PageBreakToken { .. } => true,
            PageStartToken(_) => true,
            Authority(_) => true,
            Quantity(_) => true,
            CollectingRegion(_) => true,
            Date(_) => true,
            _ => false,
        };

        self.stack.push(child);
        if commit {
            self.commit_top();
        }
    }

    pub fn pop(&mut self) -> Option<Span> {
        self.stack.pop()
    }

    pub fn commit_and_pop_all(&mut self) -> Vec<Span> {
        self.commit_all();

        match self.stack.pop() {
            None => vec![],
            Some(span) => match span {
                Span::Root(children) => children,
                _ => vec![],
            },
        }
    }

    /// "Closes" the span at the top of the stack and add it to the
    /// span next on the stack, which effectively becomes the parent.
    pub fn commit_top(&mut self) {
        let item = self.stack.pop().unwrap();

        match self.stack.last_mut() {
            Some(parent) => parent.push_child(item),
            None => self.stack.push(item),
        }
    }

    pub fn commit_all(&mut self) {
        // commit until only root is left
        while let Some(span) = self.stack.last_mut() {
            match span {
                Span::Root(_) => break,
                _ => self.commit_top(),
            }
        }
    }
}

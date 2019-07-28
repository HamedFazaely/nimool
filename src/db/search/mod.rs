use std::time::{Duration, Instant};
use std::fmt::Debug;
use std::ops::Deref;


use tantivy::query::{
    Query,
    AllQuery,
    TermQuery,
    FuzzyTermQuery,
    RangeQuery,
    RegexQuery,
    PhraseQuery,
    QueryParser,
    BooleanQuery};

use tantivy::collector::{
    TopDocs,
    Count,
};

use tantivy::IndexReader;

use tantivy::schema::{
    Term,
    IndexRecordOption,
    Field,
    Document,
};

use tantivy::Result as TResult;
use crate::db::IndexDescriptor;

pub trait QueryHandler {
    fn handle(self, reader: &IndexDescriptor) -> TResult<SearchResult>;
}

impl<T> QueryHandler for T where T: Into<SearchQuery> {
    fn handle(self, idx_desc: &IndexDescriptor) -> TResult<SearchResult> {
        use SearchQuery::*;
        match self.into() {
            AllQ(q) => handle_query(idx_desc.get_reader(), &q),
            TermQ(q) => handle_query(idx_desc.get_reader(), &q),
            FuzzyQ(q) => handle_query(idx_desc.get_reader(), &q),
            RangeQ(q) => handle_query(idx_desc.get_reader(), &q),
            RegexQ(q) => handle_query(idx_desc.get_reader(), &q),
            PhraseQ(q) => handle_query(idx_desc.get_reader(), &q),
            BooleanQ(q) => handle_query(idx_desc.get_reader(), &q),
            FreeQ(exp) => {
                let qp = QueryParser::for_index(idx_desc, idx_desc.get_raw_fields());
                let q = qp.parse_query(&exp)?;
                handle_query(idx_desc.get_reader(), &q)
            }
        }
    }
}


#[derive(Debug, Clone)]
pub enum SearchQuery {
    AllQ(AllQuery),
    TermQ(TermQuery),
    FuzzyQ(FuzzyTermQuery),
    RangeQ(RangeQuery),
    RegexQ(RegexQuery),
    PhraseQ(PhraseQuery),
    BooleanQ(BooleanQuery),
    FreeQ(String),
}


#[derive(Debug, Clone)]
pub struct SearchResult {
    pub took: Duration,
    pub hits: usize,
    pub top_doc_score: f32,
    pub docs: Vec<(f32, Document)>,
}


impl SearchResult {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            took: Duration::new(0, 0),
            hits: 0,
            top_doc_score: 0.0,
            docs: Vec::with_capacity(cap),
        }
    }

    pub fn add_doc(&mut self, d: Document, score: f32) {
        self.docs.push((score, d))
    }
}

impl Default for SearchResult {
    fn default() -> Self {
        Self {
            took: Duration::new(0, 0),
            hits: 0,
            top_doc_score: 0.0,
            docs: Vec::new(),
        }
    }
}


fn handle_query<Q: Query>(reader: &IndexReader, q: &Q) -> TResult<SearchResult> {
    let searcher = reader.searcher();
    let now = Instant::now();
    let (count, score_addr) = searcher.search(q, &(Count, TopDocs::with_limit(10)))?;
    let took = now.elapsed();
    let mut sr = SearchResult::with_capacity(score_addr.len());
    sr.took = took;
    sr.hits = count;
    if count == 0 {
        Ok(sr)
    } else {
        sr.top_doc_score = score_addr.get(0).unwrap().0;
        for (score, id) in score_addr {
            if let Ok(doc) = searcher.doc(id) {
                sr.add_doc(doc, score);
            } else {
                warn!("document not found : {:?}", id);
                continue;
            }
        }
        Ok(sr)
    }
}



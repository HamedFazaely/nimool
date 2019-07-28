use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use tantivy::schema::{STORED, TEXT, TextOptions, IndexRecordOption, TextFieldIndexing, IntOptions};
use core::borrow::Borrow;
use tantivy::schema::FieldType as TFiledType;
use serde::export::fmt::Debug;


#[derive(Debug, Copy, Clone, Serialize, Deserialize, )]
pub enum FieldTermOption {
    DocId,
    Freq,
    FreqPos,
}


impl Into<IndexRecordOption> for FieldTermOption {
    fn into(self) -> IndexRecordOption {
        match self {
            FieldTermOption::DocId => IndexRecordOption::Basic,
            FieldTermOption::Freq => IndexRecordOption::WithFreqs,
            FieldTermOption::FreqPos => IndexRecordOption::WithFreqsAndPositions
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AnalyzeOption {
    Keyword,
    Analyzed(String),
}

impl AnalyzeOption {
    pub fn new_kw() -> Self {
        AnalyzeOption::Keyword
    }

    pub fn new_analyzed(analyzer: &str) -> Self {
        AnalyzeOption::Analyzed(analyzer.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexOpt {
    pub analyzer: AnalyzeOption,
    pub record_options: FieldTermOption,
}

impl IndexOpt {
    pub fn from_ananlyzer_freq(analyzer: &str) -> Self {
        Self {
            analyzer: AnalyzeOption::new_analyzed(analyzer),
            record_options: FieldTermOption::Freq,
        }
    }

    pub fn from_opts(analyzer: Option<&str>, rec_opts: FieldTermOption) -> Self {
        Self {
            analyzer: match analyzer {
                Some(name) => AnalyzeOption::new_analyzed(name),
                None => AnalyzeOption::Keyword
            },
            record_options: rec_opts,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TextIndexConfig {
    pub index_options: Option<IndexOpt>,
    pub store: bool,
}

impl TextIndexConfig {
    pub fn not_analyzed() -> Self {
        let opts = IndexOpt {
            analyzer: AnalyzeOption::new_kw(),
            record_options: FieldTermOption::DocId,
        };

        Self {
            index_options: Some(opts),
            store: true,
        }
    }

    pub fn stored_only() -> Self {
        Self {
            index_options: None,
            store: true,
        }
    }

    pub fn from_options(store: bool, analyzer: Option<&str>, rec_opts: FieldTermOption) -> Self {
        Self {
            index_options: Some(IndexOpt::from_opts(analyzer, rec_opts)),
            store,
        }
    }
}

impl Into<TextOptions> for TextIndexConfig {
    fn into(self) -> TextOptions {
        let mut result = TextOptions::default();
        if self.store {
            result = result.set_stored();
        }

        if let Some(ref idx_opt) = self.index_options {
            let mut x = TextFieldIndexing::default();
            match idx_opt.analyzer {
                AnalyzeOption::Keyword => {
                    x = x.set_tokenizer("raw");
                }
                AnalyzeOption::Analyzed(ref an) => {
                    x = x.set_tokenizer(an);
                }
            }
            x = x.set_index_option(idx_opt.record_options.into());
            result = result.set_indexing_options(x);
        }
        result
    }
}


#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct NumberIndexConfig {
    pub stored: bool,
    pub indexed: bool,
}

impl NumberIndexConfig {
    pub fn new(stored: bool, indexed: bool) -> Self {
        Self {
            stored,
            indexed,
        }
    }
}

impl Into<IntOptions> for NumberIndexConfig {
    fn into(self) -> IntOptions {
        let mut result = IntOptions::default();
        if self.stored {
            result = result.set_stored();
        }
        if self.indexed {
            result = result.set_indexed();
        }
        result
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    Text(TextIndexConfig),
    Int64(NumberIndexConfig),
    UInt64(NumberIndexConfig),
    Date(NumberIndexConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
}

impl Field {
    pub fn new(name: &str, ft: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type: ft,
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct IndexConfig {
    pub fields: Vec<Field>,
    pub index_name: String,
}

impl IndexConfig {
    pub fn new(name: &str) -> Self {
        Self {
            fields: Vec::new(),
            index_name: name.to_string(),
        }
    }

    pub fn add_field(&mut self, f: Field) {
        self.fields.push(f);
    }
}

#[derive(Debug)]
pub struct TantivyFiled<T> where T: Into<TFiledType> + Debug + Send {
    pub name: String,
    pub ft: T,
}

impl<'a, T> TantivyFiled<T> where T: Into<TFiledType> + Debug + Send {
    pub fn new(name: &'a str, ft: T) -> TantivyFiled<T> {
        Self {
            name: name.to_string(),
            ft,
        }
    }
}

#[derive(Debug)]
pub struct IndexCreationConfig<T> where T: Into<TFiledType> + Debug + Send {
    pub index_name: String,
    pub fields: Vec<TantivyFiled<T>>,
}










use tantivy::{IndexReader, IndexWriter, Index, Result, Document, TantivyError, ReloadPolicy, DocAddress};
use tantivy::schema::FieldType as TFieldType;


use std::sync::{Arc, LockResult, MutexGuard, PoisonError};

use std::sync::Mutex;
use std::fmt::Debug;
use tantivy::schema::{DocParsingError, Schema, Field as TField};
use tantivy::collector::{
    TopDocs,
    Count,
};

use std::time::{Instant, Duration};
use crate::config::AppConf;
use crate::db::search::{SearchQuery, SearchResult, QueryHandler};
use std::path::Path;


use super::config::{
    IndexConfig,
    Field,
    FieldType,
    NumberIndexConfig,
    TextIndexConfig,
    TantivyFiled,
};

use super::document::{
    AddDocConfig,
    Doc,
};

use std::sync::atomic::{AtomicU64, Ordering};
use tantivy::query::{RegexQuery, Query};
use std::ops::Deref;

use tokio::sync::oneshot::{Receiver as OneShotReceiver, Sender as OneShotSender, self};
use tokio::prelude::*;
use tokio::timer::{Interval, Error};

use futures::future::Either;

use crate::db::util;

type WriterPoisonErr<'a> = PoisonError<MutexGuard<'a, IndexWriter>>;


#[derive(Clone)]
pub struct IndexDescriptor {
    reader: IndexReader,
    writer: Arc<Mutex<IndexWriter>>,
    schema: Schema,
    raw_fields: Vec<TField>,
    index: Index,
    uncommited_count: Arc<AtomicU64>,

}

pub struct IndexResult {
    pub descriptor: IndexDescriptor,
    pub shut_down_handle: OneShotSender<()>,
}

impl IndexResult {
    fn new(desc: IndexDescriptor, shutdown: OneShotSender<()>) -> Self {
        Self {
            descriptor: desc,
            shut_down_handle: shutdown,
        }
    }
}


impl IndexDescriptor {
    pub fn open(config: &'static AppConf, name: &str) -> Result<IndexResult> {
        debug!("opening index : {}", name);

        let path = Path::new(config.index_path).join(name);

        Index::open_in_dir(path).and_then(|idx| {
            let writer = idx.writer(config.writer_buff_size)?;
            let reader = idx.reader_builder()
                .reload_policy(ReloadPolicy::OnCommit)
                .try_into()?;
            let schema = idx.schema();
            let raw_fields = schema.fields().iter()
                .map(|f| schema.get_field(f.name()).unwrap())
                .collect();
            let (tx, rx) = oneshot::channel::<()>();

            let res = IndexDescriptor {
                reader,
                schema,
                raw_fields,
                index: idx,
                writer: Arc::new(Mutex::new(writer)),
                uncommited_count: Arc::new(AtomicU64::new(0)),
            };

            res.spawn_maintainer_task(config.auto_commit_interval, rx);
            Ok(IndexResult::new(res, tx))
        })
    }

    pub fn create(app_conf: &'static AppConf, fields: Vec<Field>, name: &str) -> Result<IndexResult> {
        info!("creating index : {}", name);
        let path = Path::new(app_conf.index_path).join(name);
        let schema = create_schema(fields);
        Index::create_in_dir(path, schema.clone()).and_then(|idx| {
            let writer = idx.writer(app_conf.writer_buff_size)?;
            let reader = idx.reader_builder()
                .reload_policy(ReloadPolicy::OnCommit)
                .try_into()?;
            let (tx, rx) = oneshot::channel::<()>();
            let res = IndexDescriptor {
                reader,
                schema,
                raw_fields: Vec::new(),
                index: idx,
                writer: Arc::new(Mutex::new(writer)),
                uncommited_count: Arc::new(AtomicU64::new(0)),
            };

            res.spawn_maintainer_task(app_conf.auto_commit_interval, rx);
            Ok(IndexResult::new(res, tx))
        })
    }

    pub fn n_create<T: Into<TFieldType> + Debug + Send>(app_conf: &'static AppConf, fields: Vec<TantivyFiled<T>>, name: &str) -> Result<IndexResult> {
        info!("creating index : {}", name);
        let path = Path::new(app_conf.index_path).join(name);
        let (schema, raw_fields) = n_create_schema(fields);
        Index::create_in_dir(path, schema.clone()).and_then(move |idx| {
            let writer = idx.writer(app_conf.writer_buff_size)?;
            let reader = idx.reader()?;
            let (tx, rx) = oneshot::channel::<()>();
            let res = IndexDescriptor {
                reader,
                schema,
                raw_fields,
                index: idx,
                writer: Arc::new(Mutex::new(writer)),
                uncommited_count: Arc::new(AtomicU64::new(0)),
            };
            res.spawn_maintainer_task(app_conf.auto_commit_interval, rx);
            Ok(IndexResult::new(res, tx))
        })
    }

    pub fn get_index(&self) -> &Index {
        &self.index
    }

    pub fn get_raw_fields(&self) -> Vec<TField> {
        self.raw_fields.clone()
    }

    pub fn get_reader(&self) -> &IndexReader {
        &self.reader
    }

    pub fn add_document(&self, document: Doc) -> Result<u64> {
        self.schema.parse_document(document.doc).map_err(|e| {
            TantivyError::from(e)
        }).and_then(|doc| {
            let mut lock = util::acquire_mutex_lock::<IndexWriter, fn(WriterPoisonErr) -> MutexGuard<IndexWriter>>(&self.writer, None);
            let id = lock.add_document(doc);
            if !document.config.commit {
                self.uncommited_count.fetch_add(1, Ordering::SeqCst);
                Ok(id)
            } else {
                lock.commit().and_then(|last_id| {
                    self.uncommited_count.store(0, Ordering::SeqCst);
                    Ok(last_id)
                })
            }
        })
    }

    pub fn search<Q: Into<SearchQuery>>(&self, q: Q) -> Result<SearchResult> {
        q.handle(self)
    }

    fn spawn_maintainer_task(&self, tick_interval: Duration, exit_chan: OneShotReceiver<()>) {
        info!("spawning maintainer task for index");
        let idx = self.clone();

        let interval = Interval::new(Instant::now(), tick_interval).for_each(move |_| {
            info!("starting maintainance cycle");
            if idx.uncommited_count.load(Ordering::SeqCst) > 0 {
                let mut writer = util::acquire_mutex_lock::<IndexWriter, fn(WriterPoisonErr) -> MutexGuard<IndexWriter>>(&idx.writer, None);
                writer.commit().map_err(|err| {
                    error!("error occured: {:?}", err);
                    Error::shutdown()
                }).and_then(|_x| {
                    idx.uncommited_count.store(0, Ordering::SeqCst);
                    Ok(())
                })
            } else {
                info!("nothing to clean up. getting back to sleep");
                Ok(())
            }
        });
        let chan_interval = interval.select2(exit_chan.map_err(|err| {
            error!("sender channel closed before receiving : {:?}", err);
        }).map(|_| {
            info!("shutdown signal received. exiting");
        })).map(|_| {
            ()
        }).map_err(|e| ());

        tokio::spawn(chan_interval);
    }
}

impl Deref for IndexDescriptor {
    type Target = Index;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

fn create_schema(fields: Vec<Field>) -> Schema {
    let mut schema_builder = Schema::builder();

    for f in fields {
        match f.field_type {
            FieldType::Text(c) => schema_builder.add_text_field::<TextIndexConfig>(&f.name, c.into()),
            FieldType::UInt64(c) => schema_builder.add_u64_field::<NumberIndexConfig>(&f.name, c.into()),
            FieldType::Int64(c) => schema_builder.add_i64_field::<NumberIndexConfig>(&f.name, c.into()),
            FieldType::Date(c) => schema_builder.add_date_field::<NumberIndexConfig>(&f.name, c.into())
        };
    }
    schema_builder.build()
}


fn n_create_schema<T: Into<TFieldType> + Debug + Send>(fields: Vec<TantivyFiled<T>>) -> (Schema, Vec<TField>) {
    let mut schema_builder = Schema::builder();
    let mut raw_fields = Vec::with_capacity(fields.len());
    for f in fields {
        match f.ft.into() {
            TFieldType::Str(opt) => raw_fields.push(schema_builder.add_text_field(&f.name, opt)),
            TFieldType::Date(opt) => raw_fields.push(schema_builder.add_date_field(&f.name, opt)),
            TFieldType::Bytes => raw_fields.push(schema_builder.add_bytes_field(&f.name)),
            TFieldType::HierarchicalFacet => raw_fields.push(schema_builder.add_facet_field(&f.name)),
            TFieldType::I64(opt) => raw_fields.push(schema_builder.add_i64_field(&f.name, opt)),
            TFieldType::U64(opt) => raw_fields.push(schema_builder.add_u64_field(&f.name, opt)),
        };
    }
    (schema_builder.build(), raw_fields)
}


mod test {
    use std::time::{Instant, Duration};

    #[test]
    fn test_tokio_interval() {
        use tokio::timer::Interval;
        use tokio::timer::Delay;
        use tokio::prelude::*;
        let i = Interval::new(Instant::now(), Duration::from_secs(1));
        let f = i.for_each(|x| {
            println!("ticked at : {:?}", x);
            Ok(())
        }).map_err(|e| {
            println!("{:?}", e);
        });

        let tout = Delay::new(Instant::now() + Duration::from_secs(10))
            .map_err(|e| println!("{:?}", e)).select(f).map(|x| ())
            .map_err(|e| println!("error"));

        let x = tokio::spawn(tout);
    }
}



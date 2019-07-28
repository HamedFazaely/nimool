use std::collections::HashMap;
use crate::db::idx::IndexDescriptor;
use std::sync::{RwLock, Arc};
use std::thread;
use tokio::sync::oneshot::{channel, Sender, Receiver};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::config::AppConf;
use tantivy::{Result, TantivyError};
use super::config::{
    IndexConfig,
    Field,
    FieldType,
    TextIndexConfig,
    NumberIndexConfig,
};
use tantivy::schema::Schema;
use tantivy::schema::FieldType as TFieldType;
use crate::db::command::{IndexCommand, IndexCommandHandler, CmdHandler, ReplyOn, OpenIndexCmd, CreateIndexCmd};


use futures::lazy;
use tokio::prelude::*;
use futures::future::{ok, Either};
use std::sync::mpsc::RecvError;

pub type GErr = Box<dyn std::error::Error + Send + Sync>;

pub type DynReceiverResult<T, E: std::error::Error + Send + Sync> = Box<dyn Future<Item=T, Error=E> + Send>;


pub struct IndexPool<T> where T: Into<TFieldType> + Debug + Send {
    pool: Arc<RwLock<HashMap<String, IndexDescriptor>>>,
    req_channel: UnboundedSender<IndexCommand<T>>,
}

impl<T> Clone for IndexPool<T> where T: Into<TFieldType> + Debug + Send {
    fn clone(&self) -> IndexPool<T> {
        info!("index pool cloned");
        IndexPool {
            pool: self.pool.clone(),
            req_channel: self.req_channel.clone(),
        }
    }
}

impl<T> IndexPool<T> where T: 'static + Into<TFieldType> + Debug + Send {
    pub fn new(config: &'static AppConf) -> Self {
        let (tx, rx) = unbounded_channel::<IndexCommand<T>>();
        let m = HashMap::new();
        let rw = RwLock::new(m);
        let arc = Arc::new(rw);
        let p = IndexPool {
            pool: arc,
            req_channel: tx,
        };

        let pc = p.pool.clone();

        info!("spawning worker thread");

        Self::spawn_using_tokio(config, pc, rx);


        p
    }

    pub fn acquire_index(&self, index_name: &str) -> DynReceiverResult<tantivy::Result<IndexDescriptor>, tokio::sync::oneshot::error::RecvError>

    {
        let map_r = self.pool.read().unwrap();
        info!("trying to get index from cache...");
        if map_r.contains_key(index_name) {
            let index_descriptor = map_r.get(index_name).unwrap();
            return Box::new(ok(Ok(index_descriptor.clone())));
        } else {
            let (tx, rx) = channel();
            info!("index not in cache. requesting from responsible thread");

            let cmd = OpenIndexCmd::new(index_name, tx);

            //important. if we don't drop the reader lock here and just wait for the worker thread on the receiving end, the worker thread
            // can never acquire the writer lock, thus there will be a dead lock in other words there will be BLOOD!
            drop(map_r);
            self.req_channel.clone().send(IndexCommand::Open(cmd));

            return Box::new(rx);
        }
    }

    pub fn acquire_idx(&self, iname: &str) -> impl Future<Item=tantivy::Result<IndexDescriptor>, Error=tokio::sync::oneshot::error::RecvError>

    {
        let map_r = self.pool.read().unwrap();
        info!("trying to get index from cache...");
        if map_r.contains_key(iname) {
            let index_descriptor = map_r.get(iname).unwrap();
            return Either::A(ok(Ok(index_descriptor.clone())));
        } else {
            let (tx, rx) = channel();
            info!("index not in cache. requesting from responsible thread");

            let cmd = OpenIndexCmd::new(iname, tx);

            //important. if we don't drop the reader lock here and just wait for the worker thread on the receiving end, the worker thread
            // can never acquire the writer lock, thus there will be a dead lock in other words there will be BLOOD!
            drop(map_r);
            self.req_channel.clone().send(IndexCommand::Open(cmd));

            return Either::B(rx);
        }
    }

    pub fn create_index(&self, index_config: IndexConfig) -> Receiver<Result<IndexDescriptor>> {
        info!("creating index with config : {:?}", index_config);
        let (tx, rx) = channel();
        let cmd = CreateIndexCmd::new(index_config, tx);
        self.req_channel.clone().send(IndexCommand::Create(cmd));
        rx
    }

    /*fn spawn_catalog_handler_thread(app_conf: &'static AppConf, catalog: Arc<RwLock<HashMap<String, IndexDescriptor>>>, rx: Receiver<IndexCommand>) {
        thread::spawn(move || {
            let handler = IndexCommandHandler::new(app_conf);
            while let Ok(cmd) = rx.recv() {
                handler.handle_command(cmd, &catalog);
            }
        });
    }*/

    fn spawn_using_tokio(app_conf: &'static AppConf, catalog: Arc<RwLock<HashMap<String, IndexDescriptor>>>, rx: tokio::sync::mpsc::UnboundedReceiver<IndexCommand<T>>) {
        let handler = IndexCommandHandler::new(app_conf);
        tokio::spawn(lazy(move || {
            info!("spaaaaaaaaaaaaaawned using tokio");
            rx.for_each(move |cmd| {
                handler.handle_command(cmd, &catalog);
                Ok(())
            }).map_err(|e| { error!("error : {:?}", e) })
        }));
    }
}

use std::fmt::{Display, Formatter, Error};
use serde::export::fmt::Debug;

#[derive(Debug)]
pub struct MyErr {
    pub s: String
}

impl MyErr {
    pub fn new(x: &std::error::Error) -> Self {
        Self {
            s: x.description().to_string()
        }
    }
}


impl std::error::Error for MyErr {}

impl Display for MyErr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

unsafe impl Send for MyErr {}

unsafe impl Sync for MyErr {}


use std::sync::mpsc::Sender;
use std::fmt::{self, Formatter};
use super::idx::IndexDescriptor;
use super::config::{
    IndexConfig,
    Field,
    FieldType,
    NumberIndexConfig,
    TextIndexConfig,
    IndexCreationConfig,
};
use tantivy::Result;
use tantivy::schema::Schema;
use std::sync::RwLock;
use std::collections::HashMap;
use crate::config::AppConf;
use serde::export::fmt::Debug;
use tantivy::schema::FieldType as TFieldType;
use std::process::id;


pub type ReplyOn<T> = tokio::sync::oneshot::Sender<Result<T>>;
pub type ShutdownHandle = tokio::sync::oneshot::Sender<()>;


pub trait CmdHandler {
    fn handle(self, app_conf: &'static AppConf, catalog: &RwLock<HashMap<String, IndexDescriptor>>) -> Option<ShutdownHandle>;
}

pub struct OpenIndexCmd {
    pub index_name: String,
    pub reply_on: ReplyOn<IndexDescriptor>,
}

impl OpenIndexCmd {
    pub fn new(name: &str, s: ReplyOn<IndexDescriptor>) -> Self {
        Self {
            index_name: name.to_string(),
            reply_on: s,
        }
    }
}


pub struct CreateIndexCmd {
    pub index_config: IndexConfig,
    pub reply_on: ReplyOn<IndexDescriptor>,
}

impl CreateIndexCmd {
    pub fn new(iconf: IndexConfig, s: ReplyOn<IndexDescriptor>) -> Self {
        Self {
            index_config: iconf,
            reply_on: s,
        }
    }
}

pub struct NCreateIndexCmd<T> where T: Into<TFieldType> + Debug + Send {
    pub reply_on: ReplyOn<IndexDescriptor>,
    pub create_config: IndexCreationConfig<T>,
}


pub enum IndexCommand<T> where T: Into<TFieldType> + Debug + Send {
    Open(OpenIndexCmd),
    Create(CreateIndexCmd),
    NCreate(NCreateIndexCmd<T>),
}

impl<T> CmdHandler for IndexCommand<T> where T: Into<TFieldType> + Debug + Send {
    fn handle(self, app_conf: &'static AppConf, catalog: &RwLock<HashMap<String, IndexDescriptor>>) -> Option<ShutdownHandle> {
        match self {
            IndexCommand::Open(o) => {
                let mut cat = catalog.write().unwrap();
                if cat.contains_key(&o.index_name) {
                    o.reply_on.send(Ok(cat.get(&o.index_name).unwrap().clone()));
                    return None;
                } else {
                    let open_result = IndexDescriptor::open(app_conf, &o.index_name);
                    match open_result {
                        Ok(idx) => {
                            cat.insert(o.index_name, idx.descriptor.clone());
                            o.reply_on.send(Ok(idx.descriptor));
                            return Some(idx.shut_down_handle);
                        }
                        Err(e) => {
                            o.reply_on.send(Err(e));
                            return None;
                        }
                    };
                }
            }

            IndexCommand::Create(c) => {
                let mut cat = catalog.write().unwrap();
                if cat.contains_key(&c.index_config.index_name) {
                    let idx = cat.get(&c.index_config.index_name).unwrap().clone();
                    c.reply_on.send(Ok(idx));
                    return None;
                } else {
                    let create_result = IndexDescriptor::create(app_conf, c.index_config.fields, &c.index_config.index_name);
                    match create_result {
                        Ok(idx) => {
                            cat.insert(c.index_config.index_name, idx.descriptor.clone());
                            c.reply_on.send(Ok(idx.descriptor));
                            return Some(idx.shut_down_handle);
                        }
                        Err(e) => {
                            c.reply_on.send(Err(e));
                            return None;
                        }
                    }
                }
            }
            IndexCommand::NCreate(c) => {
                let mut cat = catalog.write().unwrap();
                if cat.contains_key(&c.create_config.index_name) {
                    let idx = cat.get(&c.create_config.index_name).unwrap().clone();
                    c.reply_on.send(Ok(idx));
                    return None;
                } else {
                    match IndexDescriptor::n_create(app_conf, c.create_config.fields, &c.create_config.index_name) {
                        Ok(idx) => {
                            cat.insert(c.create_config.index_name, idx.descriptor.clone());
                            c.reply_on.send(Ok(idx.descriptor));
                            return Some(idx.shut_down_handle);
                        }
                        Err(e) => {
                            c.reply_on.send(Err(e));
                            return None;
                        }
                    }
                }
            }
        };
    }
}

impl<T> Debug for IndexCommand<T> where T: Into<TFieldType> + Debug + Send {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IndexCommand::Open(ref x) => {
                write!(f, "open command for index: {:?}", x.index_name)
            }
            IndexCommand::Create(ref c) => {
                write!(f, "create command for index: {:?}", c.index_config.index_name)
            }
            IndexCommand::NCreate(ref c) => {
                write!(f, "create command for index: {:?}", c.create_config.index_name)
            }
        }
    }
}

#[derive(Clone)]
pub struct IndexCommandHandler {
    app_conf: &'static AppConf
}

impl IndexCommandHandler {
    pub fn new(conf: &'static AppConf) -> Self {
        Self {
            app_conf: conf
        }
    }


    pub fn handle_command<C>(&self, cmd: C, catalog: &RwLock<HashMap<String, IndexDescriptor>>) -> Option<ShutdownHandle> where C: CmdHandler {
        cmd.handle(self.app_conf, catalog)
    }
}







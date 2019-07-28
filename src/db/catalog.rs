use std::collections::HashMap;
use std::fmt::Debug;
use crate::db::idx::IndexDescriptor;
use crate::db::command::{IndexCommand, IndexCommandHandler, OpenIndexCmd, NCreateIndexCmd};
use crate::config::AppConf;
use crate::db::config::IndexCreationConfig;
use crate::db::error::NimoolError;
use std::sync::{RwLock, Arc};
use tokio::sync::mpsc::{
    self,
    UnboundedSender,
    UnboundedReceiver,
};
use tokio::prelude::*;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot;
use futures::future;
use tantivy::Result as TantivyResul;
use tantivy::schema::FieldType as TFieldType;
use futures::future::Either;


pub struct IndexCatalog<T> where T: Into<TFieldType> + Debug + Send {
    cmd_chan: UnboundedSender<IndexCommand<T>>,
    catalog: Arc<RwLock<HashMap<String, IndexDescriptor>>>,
}

impl<T> Clone for IndexCatalog<T> where T: Into<TFieldType> + Debug + Send {
    fn clone(&self) -> Self {
        Self {
            cmd_chan: self.cmd_chan.clone(),
            catalog: self.catalog.clone(),
        }
    }
}

impl<T> IndexCatalog<T> where T: 'static + Into<TFieldType> + Debug + Send {
    fn spawn_receiver(rx: UnboundedReceiver<IndexCommand<T>>, catalog: Arc<RwLock<HashMap<String, IndexDescriptor>>>, cnfg: &'static AppConf) {
        let handler = IndexCommandHandler::new(cnfg);
        let mut shot_down_handles = Vec::new();
        let f = rx.for_each(move |cmd| {
            info!("new index command received: {:?}", cmd);
            let res = handler.handle_command(cmd, &catalog);
            if let Some(h) = res {
                shot_down_handles.push(h);
            }
            Ok(())
        }).map_err(|err| {
            error!("error in receiving index command {:?}", err);
        });
        tokio::spawn(f);
    }

    pub fn new(cnfg: &'static AppConf) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<IndexCommand<T>>();
        let map = HashMap::new();
        let rwlock = RwLock::new(map);
        let arc = Arc::new(rwlock);

        Self::spawn_receiver(rx, arc.clone(), cnfg);

        Self {
            cmd_chan: tx,
            catalog: arc,
        }
    }

    pub fn get_index_handle(&self, name: &str) -> impl Future<Item=TantivyResul<IndexDescriptor>, Error=NimoolError> {
        let catalog = self.catalog.read().unwrap();
        info!("trying to find index handle in cache: {}", name);
        if catalog.contains_key(name) {
            info!("index in cache : {}", name);
            let handle = catalog.get(name).unwrap();
            return Either::A(future::ok(Ok(handle.clone())));
        }
        info!("index not in cache. requesting from catalog maintainer task. index name : {}", name);
        let (tx, rx) = oneshot::channel();
        let open_cmd = IndexCommand::Open(OpenIndexCmd {
            index_name: name.to_string(),
            reply_on: tx,
        });
        //important. if we don't drop the reader lock here and just wait for the worker thread on the receiving end, the worker thread
        // can never acquire the writer lock, thus there will be a dead lock in other words there will be BLOOD!
        drop(catalog);
        let f = self.cmd_chan.clone().send(open_cmd).map_err(|e| {
            NimoolError::from(e)
        }).and_then(move |_| {
            rx.map_err(|e| {
                NimoolError::from(e)
            })
        });
        Either::B(f)
    }

    pub fn create_index(&self, creation_config: IndexCreationConfig<T>) -> impl Future<Item=TantivyResul<IndexDescriptor>, Error=NimoolError> {
        let (tx, rx) = oneshot::channel();
        let cmd = IndexCommand::NCreate(NCreateIndexCmd {
            reply_on: tx,
            create_config: creation_config,
        });
        self.cmd_chan.clone().send(cmd).map_err(|e| NimoolError::from(e))
            .and_then(move |_| {
                rx.map_err(|e| NimoolError::from(e))
            })
    }
}


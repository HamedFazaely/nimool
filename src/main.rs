#[macro_use]
extern crate log;
extern crate uuid;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate tantivy;

#[macro_use]
extern crate futures;

extern crate tokio;

extern crate hyper;
extern crate regex;
extern crate bytes;

mod logo;
mod config;
mod command;
mod router;

mod db;


use config::{SimpleLogger, AppConf};


//standard imports
use std::net::SocketAddr;
use std::clone::Clone;


//third party imports
use log::LevelFilter;
use hyper::{Body, Request, Response, Server};
use tantivy::schema::FieldType;

use hyper::service::service_fn_ok;
use hyper::service::service_fn;
use futures::{future, Future, Stream};


//our imports
use db::IndexCatalog;
use std::error::Error;
use std::sync::mpsc::RecvError;
use self::db::IndexDescriptor;


use std::time::Duration;

use tokio::prelude::*;
use futures::future::ok;
use crate::router::{
    Route,
    NimoolRouter,
};

use crate::router::handler;

static LOGGER: SimpleLogger = SimpleLogger;


//read this from config file later
static APP_CONFIGURATION: AppConf = AppConf {
    index_path: "./indexes",
    writer_buff_size: 50_000_000,
    listen_address: "127.0.0.1",
    listen_port: 1969,
    auto_commit_interval: Duration::from_secs(5),
};

use tokio::sync::oneshot::error;
use hyper::Version;
use std::sync::Arc;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type ResponseFuture = Box<dyn Future<Item=Response<Body>, Error=GenericError> + Send>;

#[derive(Debug)]
pub struct DummyIntoFieldType;

unsafe impl Send for DummyIntoFieldType {}

impl Into<FieldType> for DummyIntoFieldType {
    fn into(self) -> FieldType {
        FieldType::Bytes
    }
}


fn main() {
    println!("{}", logo::LOGO);
    config::init_logger(LevelFilter::Debug, &LOGGER).unwrap();

    let mut nrouter = NimoolRouter::new();
    let mut route = Route::new_get(r"^/nimool/index/(\w*)$", handler::open_handler);
    nrouter.add_route(route);
    route = Route::new_post(r"^/nimool/test$", handler::handle_post);
    nrouter.add_route(route);


    let addr: SocketAddr = ([127, 0, 0, 1], 1969).into();


    hyper::rt::run(future::lazy(move || {
        // Share a pool with all `Service`s
        let catalog: IndexCatalog<DummyIntoFieldType> = IndexCatalog::new(&APP_CONFIGURATION);
        let arc_router = Arc::new(nrouter);


        let new_service = move || {
            // Move a clone of `catalog` into the `service_fn`.
            let p = catalog.clone();
            let r = arc_router.clone();
            info!("pool cloned");
            service_fn(move |req| {
                r.handle_request(req, &p)
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        println!("Listening on http://{}", addr);

        server
    }));
}


//ToDo : config does not need to be a HashMap
//ToDo : load configuration from file

use super::{
    ResponseFuture,
    GenericError,
};

use tokio::prelude::*;
use hyper::{
    Request,
    Body,
    Response,
    StatusCode,
};
use crate::db::IndexCatalog;
use crate::DummyIntoFieldType;

use serde::{
    Serialize,
    Deserialize,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct MyReqData {
    x: usize,
    y: f32,
}

use bytes::Buf;

pub fn open_handler(req: Request<Body>, catalog: &IndexCatalog<DummyIntoFieldType>, params: Option<Vec<&str>>) -> ResponseFuture {
    let mut resp = Response::new(Body::empty());
    let index_name = params.unwrap()[0];
    let x = catalog.get_index_handle(index_name)
        .map_err(|e| {
            error!("{:?}", e);
            Box::new(e) as GenericError
        }).map(move |res| {
        if let Ok(idx) = res {
            *resp.status_mut() = StatusCode::OK;
            *resp.body_mut() = Body::from("index opened");
            resp
        } else {
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            *resp.body_mut() = Body::from("internal server error");
            resp
        }
    });
    Box::new(x)
}

pub fn handle_post(req: Request<Body>, catalog: &IndexCatalog<DummyIntoFieldType>, params: Option<Vec<&str>>) -> ResponseFuture {
    let mut f = Response::new(Body::empty());
    let resp = req.into_body()
        .concat2().map_err(|e| {
        error!("{:?}", e);
        Box::new(e) as GenericError
    })
        .map(move |x| {
            let bytes = x.bytes();
            let r = serde_json::from_slice::<MyReqData>(bytes);

            if let Ok(data) = r {
                info!("POST DATA RECEIVED : {:?}", data);
                let des = serde_json::to_string(&data).unwrap();
                *f.status_mut() = StatusCode::OK;
                *f.body_mut() = Body::from(des);
            } else {
                let e = format!("{:?}", r.err().unwrap());
                *f.status_mut() = StatusCode::BAD_REQUEST;
                *f.body_mut() = Body::from(e);
            }
            f
        });
    Box::new(resp)
}

mod test {
    use serde::{
        Serialize, Deserialize,
    };

    #[derive(Serialize, Deserialize)]
    struct MyData {
        d: usize,
        g: f64,
    }

    #[test]
    fn test_serde() {
        let x = MyData {
            d: 100,
            g: 2.0,
        };

        let res = serde_json::to_string(&x).unwrap();
        println!("{}", res);
    }
}
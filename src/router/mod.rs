use tokio::prelude::*;
use hyper::{Body, Request, Response, Method, Error};
use hyper::http::StatusCode;
use crate::db::IndexCatalog;
use crate::DummyIntoFieldType;
use futures::future::ok;
use regex::Regex;
use regex::Captures;
use std::hash::{Hash, Hasher};

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<dyn Future<Item=Response<Body>, Error=GenericError> + Send>;
pub type HandlerFunc = fn(Request<Body>, &IndexCatalog<DummyIntoFieldType>, Option<Vec<&str>>) -> ResponseFuture;

pub mod handler;


pub struct Route {
    method: Method,
    pattern: Regex,
    handler: HandlerFunc,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.method == other.method && self.pattern.as_str() == other.pattern.as_str()
    }
}

impl Eq for Route {}

impl Hash for Route {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.method.hash(state);
        self.pattern.as_str().hash(state);
    }
}

impl Route where {
    pub fn new(meth: Method, pat: &str, handler: HandlerFunc) -> Self {
        Self {
            method: meth,
            pattern: Regex::new(pat).unwrap(),
            handler,

        }
    }

    pub fn new_get(pat: &str, h: HandlerFunc) -> Self {
        Self::new(Method::GET, pat, h)
    }

    pub fn new_post(pat: &str, h: HandlerFunc) -> Self {
        Self::new(Method::POST, pat, h)
    }

    pub fn new_put(pat: &str, h: HandlerFunc) -> Self {
        Self::new(Method::PUT, pat, h)
    }

    pub fn new_delete(pat: &str, h: HandlerFunc) -> Self {
        Self::new(Method::DELETE, pat, h)
    }

    pub fn is_match(&self, path: &str, meth: &Method) -> bool {
        self.pattern.is_match(path) && self.method == *meth
    }

    fn handle(&self, req: Request<Body>, catalog: &IndexCatalog<DummyIntoFieldType>) -> ResponseFuture {
        let path = req.uri().path().to_string();
        let caps: Option<Vec<&str>> = self.pattern.captures(&path)
            .map(|c| c.iter().skip(1)
                .filter(|x| x.is_some())
                .map(|x| x.unwrap().as_str()).collect());
        (self.handler)(req, catalog, caps)
    }
}


pub struct NimoolRouter {
    routes: Vec<Route>
}

impl NimoolRouter {
    pub fn new() -> Self {
        Self {
            routes: Vec::new()
        }
    }

    pub fn add_route(&mut self, route: Route) {
        self.routes.push(route);
    }

    pub fn handle_request(&self, req: Request<Body>, catalog: &IndexCatalog<DummyIntoFieldType>) -> ResponseFuture {
        let path = req.uri().path();
        for route in &self.routes {
            if route.is_match(path, req.method()) {
                return route.handle(req, catalog);
            }
        }
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = StatusCode::NOT_FOUND;
        Box::new(ok(resp))
    }
}

mod test {
    use regex::Regex;

    #[test]
    fn regex_test() {
        let text = "/nimool/index/somename";
        let re = Regex::new(r"^/nimool/index/(\w*)$").unwrap();
        assert_eq!(re.is_match(text), true);
        let caps = re.captures(text).unwrap();
        assert_eq!(caps.len(), 2);
        assert_eq!(caps.get(1).unwrap().as_str(), "somename");
        assert_eq!(re.is_match("sdfsfsf"), false);
    }
}



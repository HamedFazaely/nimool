use tantivy::TantivyError;
use tantivy::schema::DocParsingError;
use tokio::sync::oneshot::error::RecvError as OneChotRecvError;
use tokio::sync::mpsc::error::{SendError, RecvError};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub fn doc_parsing_err_to_string(e: DocParsingError) -> String {
    match e {
        DocParsingError::NoSuchFieldInSchema(s) => s,
        DocParsingError::NotJSON(s) => s,
        DocParsingError::ValueError(s, v) => format!("{} {:?}", s, v)
    }
}

#[derive(Debug, Clone)]
pub enum NimoolError {
    ChannelSendErr(String),
    ChannelReceiveError(String),
    GeneralError(String),
}

impl NimoolError {
    pub fn unwrap(&self) -> &str {
        use NimoolError::*;
        match self {
            GeneralError(s) => s,
            ChannelReceiveError(s) => s,
            ChannelSendErr(s) => s,
        }
    }

    pub fn from<E: Error>(e: E) -> NimoolError {
        NimoolError::GeneralError(e.description().to_string())
    }
}

impl Display for NimoolError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.unwrap())
    }
}

/*impl<E> From<E> for NimoolError where E: Error {

}*/

impl Error for NimoolError {}


unsafe impl Send for NimoolError {}

unsafe impl Sync for NimoolError {}



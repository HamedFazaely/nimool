use std::collections::HashMap;

//#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddDocConfig {
    pub commit: bool,
}


pub struct Doc<'a> {
    pub doc: &'a str,
    pub config: AddDocConfig,
}
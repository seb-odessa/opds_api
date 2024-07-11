use crate::value::Value;

#[derive(Debug, PartialEq, Eq)]
pub struct Author {
    pub first_name: Value,
    pub middle_name: Value,
    pub last_name: Value,
}
impl Author {
    pub fn new(first_name: Value, middle_name: Value, last_name: Value) -> Self {
        Self { first_name, middle_name, last_name }
    }
}
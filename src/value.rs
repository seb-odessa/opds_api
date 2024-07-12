#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    pub id: u32,
    pub value: String,
}
impl Value {
    pub fn new<T: Into<String>>(id: u32, value: T) -> Self {
        Self { id, value: value.into() }
    }
}
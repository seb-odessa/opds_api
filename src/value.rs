use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    pub id: u32,
    pub value: String,
}
impl Value {
    pub fn new<T: Into<String>>(id: u32, value: T) -> Self {
        Self {
            id,
            value: value.into(),
        }
    }
}
impl From<(u32, String)> for Value {
    fn from((id, value): (u32, String)) -> Self {
        Self::new(id, value)
    }
}
impl From<(u32, &str)> for Value {
    fn from((id, value): (u32, &str)) -> Self {
        Self::new(id, value)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt() {
        assert_eq!("A", format!("{}", &Value::new(1, "A")));
    }
}

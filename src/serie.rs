use std::fmt;

use crate::author::Author;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Serie {
    pub id: u32,
    pub name: String,
    pub count: u32,
    pub author: Author,
}
impl Serie {
    pub fn new<T: Into<String>>(id: u32, name: T, count: u32, author: Author) -> Self {
        Self {
            id,
            name: name.into(),
            count,
            author,
        }
    }
}
impl fmt::Display for Serie {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} [{}] ({})",
            self.name, self.author, self.count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn fmt() {
        assert_eq!(
            "A [A B C] (42)",
            format!(
                "{}",
                &Serie::new(
                    1,
                    "A",
                    42,
                    Author {
                        first_name: Value::new(1, "A"),
                        middle_name: Value::new(2, "B"),
                        last_name: Value::new(3, "C"),
                    }
                )
            )
        );
    }
}

use crate::value::Value;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Author {
    pub first_name: Value,
    pub middle_name: Value,
    pub last_name: Value,
}
impl Author {
    pub fn new(first_name: Value, middle_name: Value, last_name: Value) -> Self {
        Self {
            first_name,
            middle_name,
            last_name,
        }
    }
}
impl fmt::Display for Author {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = std::iter::once(self.first_name.value.trim())
            .chain(std::iter::once(self.middle_name.value.trim()))
            .chain(std::iter::once(self.last_name.value.trim()))
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>()
            .join(" ");
        formatter.write_str(&out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt() {
        assert_eq!(
            "A B C",
            format!(
                "{}",
                &Author {
                    first_name: Value::new(1, "A"),
                    middle_name: Value::new(2, "B"),
                    last_name: Value::new(3, "C"),
                }
            )
        );
        assert_eq!(
            "A C",
            format!(
                "{}",
                &Author {
                    first_name: Value::new(1, "A"),
                    middle_name: Value::new(2, ""),
                    last_name: Value::new(3, "C"),
                }
            )
        );
        assert_eq!(
            "C",
            format!(
                "{}",
                &Author {
                    first_name: Value::new(1, ""),
                    middle_name: Value::new(2, ""),
                    last_name: Value::new(3, "C"),
                }
            )
        );
        assert_eq!(
            "",
            format!(
                "{}",
                &Author {
                    first_name: Value::new(1, ""),
                    middle_name: Value::new(2, ""),
                    last_name: Value::new(3, ""),
                }
            )
        );
    }
}

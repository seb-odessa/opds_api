use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Serie {
    pub id: u32,
    pub name: String,
    pub count: u32,
}
impl Serie {
    pub fn new<T: Into<String>>(id: u32, name: T, count: u32) -> Self {
        Self {
            id,
            name: name.into(),
            count,
        }
    }
}
impl fmt::Display for Serie {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.name, self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt() {
        assert_eq!("A (42)", format!("{}", &Serie::new(1, "A", 42)));
    }
}

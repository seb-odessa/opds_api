use lazy_static;
use std::collections::HashMap;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Query {
    AuthorNextCharByPrefix,
    SerieNextCharByPrefix,
}
impl Query {
    pub const VALUES: [Self; 2] = [
        Self::AuthorNextCharByPrefix,
        Self::SerieNextCharByPrefix,
    ];
}

lazy_static::lazy_static! {
    pub static ref MAP: HashMap<Query, &'static str> = {
        let mut m = HashMap::new();
        m.insert(
            Query::AuthorNextCharByPrefix,
            "SELECT DISTINCT substr(value, 1, $1) AS name FROM last_names WHERE value LIKE $2 || '%'");
        m.insert(
            Query::SerieNextCharByPrefix,
            "SELECT DISTINCT substr(value, 1, $1) AS name FROM series WHERE value LIKE $2 || '%'");
        m
    };
}

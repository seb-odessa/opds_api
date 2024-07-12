use author::Author;
use log::{debug, error};
use queries::{Mapper, Query};
use rusqlite::{params, CachedStatement, Connection};
use std::convert::TryFrom;
use value::Value;

pub mod author;
pub mod collation;
pub mod queries;
pub mod value;

#[derive(Debug)]
pub struct OpdsApi {
    conn: Connection,
}

impl OpdsApi {
    fn prepare(&self, query: &Query) -> anyhow::Result<CachedStatement> {
        let sql = Query::get(query)?;
        let statement = self.conn.prepare_cached(sql)?;
        Ok(statement)
    }

    /// Create OpdsApi instance
    pub fn new(conn: Connection) -> Self {
        OpdsApi { conn }
    }

    /// Returns true if database opened in ReadOnly
    pub fn is_readonly(&self) -> anyhow::Result<bool> {
        Ok(self.conn.is_readonly(rusqlite::DatabaseName::Main)?)
    }

    /// Returns next possible variants of the author name by given prefix
    pub fn authors_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let len = (prefix.chars().count() + 1) as u32;
        let query = Query::AuthorNextCharByPrefix;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query(params![len, prefix])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns next possible variants of the serie name by given prefix
    pub fn series_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let len = (prefix.chars().count() + 1) as u32;
        let query = Query::SerieNextCharByPrefix;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query(params![len, prefix])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Authors by exact last name
    pub fn authors_by_last_name(&self, name: &String) -> anyhow::Result<Vec<Author>> {
        let query = Query::AuthorsByLastName;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([name])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by exact serie name
    pub fn series_by_serie_name(&self, name: &String) -> anyhow::Result<Vec<Value>> {
        let query = Query::SeriesBySerieName;
        if let Mapper::Value(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([name])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Author by ids
    pub fn author_by_ids(&self, fid: u32, mid: u32, lid: u32) -> anyhow::Result<Option<Author>> {
        let query = Query::AuthorByIds;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res.first().cloned())
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }
}

impl TryFrom<&str> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &str) -> anyhow::Result<Self> {
        debug!("database: {database}");
        let conn = Connection::open(&database).inspect_err(|e| error!("{e}"))?;
        conn.create_collation("opds", collation::collation)?;
        Ok(Self::new(conn))
    }
}
impl TryFrom<&String> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &String) -> anyhow::Result<Self> {
        debug!("database: {database}");
        OpdsApi::try_from(database.as_str())
    }
}

fn transfrom<T, E, I>(collection: I) -> anyhow::Result<Vec<T>, E>
where
    I: IntoIterator<Item = rusqlite::Result<T, E>>,
{
    collection.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATABASE: &'static str = "file:/lib.rus.ec/books.db?mode=ro";

    #[test]
    fn is_readonly() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        assert!(api.is_readonly()?);
        Ok(())
    }

    #[test]
    fn authors_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.authors_next_char_by_prefix(&String::from("Диво"))?;
        assert_eq!(result, vec!["Дивов", "Дивон"]);
        Ok(())
    }

    #[test]
    fn series_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.series_next_char_by_prefix(&String::from("Warhammer"))?;
        assert_eq!(result, vec!["Warhammer ", "warhammer "]);
        Ok(())
    }

    #[test]
    fn authors_by_last_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.authors_by_last_name(&String::from("Стругацкий"))?;
        assert_eq!(
            result,
            vec![
                Author {
                    first_name: Value::new(24, "Аркадий"),
                    middle_name: Value::new(29, "Натанович"),
                    last_name: Value::new(9649, "Стругацкий"),
                },
                Author {
                    first_name: Value::new(126, "Борис"),
                    middle_name: Value::new(29, "Натанович"),
                    last_name: Value::new(9649, "Стругацкий"),
                },
                Author {
                    first_name: Value::new(19, "Владимир"),
                    middle_name: Value::new(97, "Ильич"),
                    last_name: Value::new(9649, "Стругацкий"),
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn series_by_serie_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.series_by_serie_name(&String::from("Warhammer Horror"))?;
        assert_eq!(result, vec![Value::new(33771, "Warhammer Horror")]);
        Ok(())
    }

    #[test]
    fn author_by_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.author_by_ids(126, 29, 9649)?;
        assert_eq!(
            result,
            Some(Author {
                first_name: Value::new(126, "Борис"),
                middle_name: Value::new(29, "Натанович"),
                last_name: Value::new(9649, "Стругацкий"),
            })
        );
        Ok(())
    }
}

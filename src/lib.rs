use anyhow::anyhow;
use author::Author;
use log::{debug, error};
use queries::Query;
use rusqlite::Connection;
use std::convert::TryFrom;
use value::Value;

pub mod author;
pub mod queries;
pub mod collation;
pub mod value;

#[derive(Debug)]
pub struct OpdsApi {
    conn: Connection,
}

impl OpdsApi {
    /// Create OpdsApi instance
    pub fn new(conn: Connection) -> Self {
        OpdsApi { conn }
    }

    /// Returns true if database opened in ReadOnly
    pub fn is_readonly(&self) -> anyhow::Result<bool> {
        Ok(self.conn.is_readonly(rusqlite::DatabaseName::Main)?)
    }

    fn next_char_by_prefix(&self, sql: &str, prefix: &String) -> anyhow::Result<Vec<String>> {
        let len = (prefix.chars().count() + 1) as u32;
        self.conn
            .prepare_cached(sql)
            .inspect(|s| {
                if let Some(sql) = s.expanded_sql() {
                    debug!("{sql}")
                }
            })
            .inspect_err(|e| error!("{e}"))?
            .query((len, prefix))
            .inspect_err(|e| error!("{e}"))?
            .mapped(|row| row.get(0))
            .try_fold(Vec::new(), |mut acc, item| {
                acc.push(item.inspect_err(|e| error!("{e}"))?);
                Ok(acc)
            })
    }

    /// Returns next possible variants of the author name by given prefix
    pub fn authors_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let query = Query::AuthorNextCharByPrefix;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.next_char_by_prefix(sql, prefix),
            None => Err(anyhow!("{:?} Not found", query)),
        }
    }

    /// Returns next possible variants of the serie name by given prefix
    pub fn series_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let query = Query::SerieNextCharByPrefix;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.next_char_by_prefix(sql, prefix),
            None => Err(anyhow!("{:?} Not found", query)),
        }
    }

    fn authors_by_name(&self, sql: &str, name: &String) -> anyhow::Result<Vec<Author>> {
        self.conn
            .prepare_cached(sql)
            .inspect(|s| {
                if let Some(sql) = s.expanded_sql() {
                    debug!("{sql}")
                }
            })
            .inspect_err(|e| error!("{e}"))?
            .query([name])
            .inspect_err(|e| error!("{e}"))?
            .mapped(|row| {
                let fname = Value::new(row.get(0)?, row.get::<usize, String>(1)?);
                let mname = Value::new(row.get(2)?, row.get::<usize, String>(3)?);
                let lname = Value::new(row.get(4)?, row.get::<usize, String>(5)?);
                Ok(Author::new(fname, mname, lname))
            })
            .try_fold(Vec::new(), |mut acc, item| {
                acc.push(item.inspect_err(|e| error!("{e}"))?);
                Ok(acc)
            })
    }

    pub fn authors_by_last_name(&self, name: &String) -> anyhow::Result<Vec<Author>> {
        let query = Query::AuthorsByLastName;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.authors_by_name(sql, name),
            None => Err(anyhow!("{:?} Not found", query)),
        }
    }

    fn series_by_name(&self, sql: &str, name: &String) -> anyhow::Result<Vec<Value>> {
        self.conn
            .prepare_cached(sql)
            .inspect(|s| {
                if let Some(sql) = s.expanded_sql() {
                    debug!("{sql}")
                }
            })
            .inspect_err(|e| error!("{e}"))?
            .query([name])
            .inspect_err(|e| error!("{e}"))?
            .mapped(|row| {
                let serie = Value::new(row.get(0)?, row.get::<usize, String>(1)?);
                Ok(serie)
            })
            .try_fold(Vec::new(), |mut acc, item| {
                acc.push(item.inspect_err(|e| error!("{e}"))?);
                Ok(acc)
            })
    }

    pub fn series_by_serie_name(&self, name: &String) -> anyhow::Result<Vec<Value>> {
        let query = Query::SeriesBySerieName;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.series_by_name(sql, name),
            None => Err(anyhow!("{:?} Not found", query)),
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
        let names = api.authors_next_char_by_prefix(&String::from("Диво"))?;
        assert_eq!(names, vec!["Дивов", "Дивон"]);
        Ok(())
    }

    #[test]
    fn series_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let names = api.series_next_char_by_prefix(&String::from("Warhammer"))?;
        assert_eq!(names, vec!["Warhammer ", "warhammer "]);
        Ok(())
    }

    #[test]
    fn authors_by_last_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let names = api.authors_by_last_name(&String::from("Стругацкий"))?;
        assert_eq!(
            names,
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
        let names = api.series_by_serie_name(&String::from("Warhammer Horror"))?;
        assert_eq!(names, vec![Value::new(33771, "Warhammer Horror")]);
        Ok(())
    }

}

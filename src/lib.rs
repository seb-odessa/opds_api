use anyhow::anyhow;
use log::{debug, error};
use queries::Query;
use rusqlite::Connection;
use std::convert::TryFrom;

pub mod queries;

#[derive(Debug)]
pub struct OpdsApi {
    conn: Connection,
}

impl OpdsApi {
    pub fn new(conn: Connection) -> Self {
        OpdsApi { conn }
    }

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

    pub fn authors_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let query = Query::AuthorNextCharByPrefix;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.next_char_by_prefix(sql, prefix),
            None => Err(anyhow!("{:?} Not found", query)),
        }
    }

    pub fn series_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        let query = Query::SerieNextCharByPrefix;
        match queries::MAP.get(&query).cloned() {
            Some(sql) => self.next_char_by_prefix(sql, prefix),
            None => Err(anyhow!("{:?} Not found", query)),
        }
    }
}

impl TryFrom<&str> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &str) -> anyhow::Result<Self> {
        debug!("database: {database}");
        let conn = Connection::open(&database).inspect_err(|e| error!("{e}"))?;
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

    #[test]
    fn is_readonly() -> anyhow::Result<()> {
        let api = OpdsApi::try_from("file:/lib.rus.ec/books.db?mode=ro")?;
        assert!(api.is_readonly()?);
        Ok(())
    }

    #[test]
    fn authors_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from("file:/lib.rus.ec/books.db?mode=ro")?;
        let names = api.authors_next_char_by_prefix(&String::from("Диво"))?;
        assert_eq!(names, vec!["Дивов", "Дивон"]);
        Ok(())
    }

    #[test]
    fn series_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from("file:/lib.rus.ec/books.db?mode=ro")?;
        let names = api.series_next_char_by_prefix(&String::from("Warhammer"))?;
        assert_eq!(names, vec!["Warhammer ", "warhammer "]);
        Ok(())
    }


}

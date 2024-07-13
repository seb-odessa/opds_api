use log::{debug, error};
use queries::{Mapper, Query};
use rusqlite::{params, CachedStatement, Connection};

use std::convert::TryFrom;

pub use author::Author;
pub use book::Book;
pub use serie::Serie;
pub use value::Value;

pub mod author;
pub mod book;
pub mod collation;
pub mod queries;
pub mod serie;
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
    pub fn series_by_serie_name(&self, name: &String) -> anyhow::Result<Vec<Serie>> {
        let query = Query::SeriesBySerieName;
        if let Mapper::Serie(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([name])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by authors ids
    pub fn series_by_author_ids(&self, fid: u32, mid: u32, lid: u32) -> anyhow::Result<Vec<Serie>> {
        let query = Query::SeriesByAuthorIds;
        if let Mapper::Serie(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid])?.mapped(mapper);
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

    /// Returns book by Author by ids and Serie id
    pub fn books_by_author_ids_and_serie_id(
        &self,
        fid: u32,
        mid: u32,
        lid: u32,
        sid: u32,
    ) -> anyhow::Result<Vec<Book>> {
        let query = Query::BooksByAuthorIdsAndSerieId;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid, sid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }
}

impl TryFrom<&str> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &str) -> anyhow::Result<Self> {
        debug!("database: {database}");
        let conn = Connection::open(database).inspect_err(|e| error!("{e}"))?;
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

    const DATABASE: &'static str = "file:data/fb2-768381-769440.db?mode=ro";

    #[test]
    fn is_readonly() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        assert!(api.is_readonly()?);
        Ok(())
    }

    #[test]
    fn authors_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.authors_next_char_by_prefix(&String::from("Сто"))?;

        assert_eq!(result, vec!["Стое", "Стоу"]);
        Ok(())
    }

    #[test]
    fn series_next_char_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.series_next_char_by_prefix(&String::from("Го"))?;

        assert_eq!(result, vec!["Гон", "Гор", "Гос"]);
        Ok(())
    }

    #[test]
    fn authors_by_last_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api
            .authors_by_last_name(&String::from("Кейн"))?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![String::from("Адель Кейн"), String::from("Рэйчел Кейн")]
        );
        Ok(())
    }

    #[test]
    fn series_by_serie_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api
            .series_by_serie_name(&String::from("Кровь на воздух"))?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![String::from("Кровь на воздух [Павел Сергеевич Иевлев] (2)")]
        );
        Ok(())
    }

    #[test]
    fn series_by_author_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api
            .series_by_author_ids(50, 42, 281)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![String::from("Кровь на воздух [Павел Сергеевич Иевлев] (2)")]
        );
        Ok(())
    }

    #[test]
    fn author_by_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api
            .author_by_ids(50, 42, 281)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        assert_eq!(result, vec![String::from("Павел Сергеевич Иевлев")]);
        Ok(())
    }

    #[test]
    fn books_by_author_ids_and_serie_id() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let result = api
            .books_by_author_ids_and_serie_id(50, 42, 281, 56)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                String::from(
                    "1 «Кровь на воздух», часть первая «Капитан-соло» (2024-06-08) [3.50 MB]"
                ),
                String::from("2 Пустота внутри кота (2024-06-23) [4.29 MB]")
            ]
        );

        Ok(())
    }
}

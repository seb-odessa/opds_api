use log::{debug, error};
use queries::{Mapper, Query};
use rusqlite::{functions::FunctionFlags, params, CachedStatement, Connection};

use std::{convert::TryFrom, rc::Rc};

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

#[cfg(test)]
mod api;

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

    fn search_by_mask<F, S>(mask: S, fetcher: F) -> anyhow::Result<(Vec<String>, Vec<String>)>
    where
        F: Fn(&String) -> anyhow::Result<Vec<String>>,
        S: Into<String>,
    {
        let mut mask = mask.into();
        let mut complete = Vec::new();
        let mut incomplete = Vec::new();

        debug!("search_by_mask <- {mask}");

        loop {
            let patterns = fetcher(&mask)?;
            let (mut exact, mut tail) = patterns.into_iter().partition(|curr| mask.eq(curr));
            complete.append(&mut exact);

            if tail.is_empty() {
                break;
            } else if 1 == tail.len() {
                std::mem::swap(&mut mask, &mut tail[0]);
            } else if 2 == tail.len() {
                let are_equal = tail[0].to_lowercase() == tail[1].to_lowercase();
                if are_equal {
                    std::mem::swap(&mut mask, &mut tail[0]);
                } else {
                    incomplete.append(&mut tail);
                    break;
                }
            } else {
                incomplete.append(&mut tail);
                break;
            }
        }

        Ok((complete, incomplete))
    }

    /// Create OpdsApi instance
    pub fn new(conn: Connection) -> Self {
        OpdsApi { conn }
    }

    /// Returns true if database opened in ReadOnly
    pub fn is_readonly(&self) -> anyhow::Result<bool> {
        Ok(self.conn.is_readonly(rusqlite::DatabaseName::Main)?)
    }

    /// Returns Authors and NVC of the author name by given prefix
    pub fn search_authors_by_prefix(
        &self,
        prefix: &String,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
        debug!("search_authors_by_prefix <- {prefix}");

        let fetcher = |s: &String| self.authors_next_char_by_prefix(s);
        Self::search_by_mask(prefix, fetcher)
    }

    /// Returns next possible variants of the author name by given prefix
    pub fn authors_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        debug!("authors_next_char_by_prefix <- {prefix}");

        let len = (prefix.chars().count() + 1) as u32;
        let query = Query::AuthorNextCharByPrefix;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let matcher = format!(
                "{}*",
                prefix
                    .replace("[", "?")
                    .replace("]", "?")
                    .replace("*", "?")
                    .to_lowercase()
            );
            let rows = statement.query(params![len, matcher])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns next possible variants of the serie name by given prefix
    pub fn series_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        debug!("series_next_char_by_prefix <- {prefix}");

        let len = (prefix.chars().count() + 1) as u32;
        let query = Query::SerieNextCharByPrefix;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let matcher = format!(
                "{}*",
                prefix
                    .replace("[", "?")
                    .replace("]", "?")
                    .replace("*", "?")
                    .to_lowercase()
            );
            let rows = statement.query(params![len, matcher])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns next possible variants of the serie name by given prefix
    pub fn books_next_char_by_prefix(&self, prefix: &String) -> anyhow::Result<Vec<String>> {
        debug!("books_next_char_by_prefix <- {prefix}");

        let len = (prefix.chars().count() + 1) as u32;
        let query = Query::BookNextCharByPrefix;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let matcher = format!(
                "{}*",
                prefix
                    .replace("[", "?")
                    .replace("]", "?")
                    .replace("*", "?")
                    .to_lowercase()
            );
            let rows = statement.query(params![len, matcher])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns NVC of the serie name by given prefix
    pub fn search_series_by_prefix(
        &self,
        prefix: &String,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
        debug!("search_series_by_prefix <- {prefix}");

        let fetcher = |s: &String| self.series_next_char_by_prefix(s);
        Self::search_by_mask(prefix, fetcher)
    }

    /// Returns NVC of the book title by given prefix
    pub fn search_books_by_prefix(
        &self,
        prefix: &String,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
        debug!("search_books_by_prefix <- {prefix}");

        let fetcher = |s: &String| self.books_next_char_by_prefix(s);
        Self::search_by_mask(prefix, fetcher)
    }

    /// Returns Authors by exact last name
    pub fn authors_by_last_name(&self, name: &String) -> anyhow::Result<Vec<Author>> {
        debug!("authors_by_last_name <- {name}");

        let query = Query::AuthorsByLastName;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([name.to_lowercase()])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Authors by Genre name
    pub fn authors_by_genre_id(&self, gid: u32) -> anyhow::Result<Vec<Author>> {
        debug!("authors_by_genre_id <- {gid}");

        let query = Query::AuthorsByGenreId;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([gid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Authors by Genre name
    pub fn authors_by_books_ids(&self, ids: Vec<u32>) -> anyhow::Result<Vec<Author>> {
        debug!("authors_by_books_ids <- {:?}", ids);

        let query = Query::AuthorsByBooksIds;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            use rusqlite::types::Value;
            let params = Rc::new(ids.into_iter().map(Value::from).collect::<Vec<Value>>());
            let rows = statement.query(params![params])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by series ids
    pub fn series_by_ids(&self, ids: Vec<u32>) -> anyhow::Result<Vec<Serie>> {
        debug!("series_by_serie_name <- {:?}", ids);

        let query = Query::SeriesByIds;
        if let Mapper::Serie(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            use rusqlite::types::Value;
            let params = Rc::new(ids.into_iter().map(Value::from).collect::<Vec<Value>>());
            let rows = statement.query(params![params])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by exact serie name
    pub fn series_by_serie_name(&self, name: &String) -> anyhow::Result<Vec<Serie>> {
        debug!("series_by_serie_name <- {name}");

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

    /// Returns Series by Genre name
    pub fn series_by_genre_id(&self, gid: u32) -> anyhow::Result<Vec<Serie>> {
        debug!("series_by_genre_id <- {gid}");

        let query = Query::SeriesByGenreId;
        if let Mapper::Serie(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([gid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by authors ids
    pub fn series_by_author_ids(&self, fid: u32, mid: u32, lid: u32) -> anyhow::Result<Vec<Serie>> {
        debug!("series_by_author_ids <- {fid}, {mid}, {lid}");

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
        debug!("author_by_ids <- {fid}, {mid}, {lid}");

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

    /// Returns Book by id
    pub fn book_by_id(&self, bid: u32) -> anyhow::Result<Option<Book>> {
        debug!("book_by_id <- {bid}");

        let query = Query::BookById;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([bid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res.first().cloned())
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns book by Author by ids
    pub fn books_by_author_ids(&self, fid: u32, mid: u32, lid: u32) -> anyhow::Result<Vec<Book>> {
        debug!("books_by_author_ids <- {fid}, {mid}, {lid}");

        let query = Query::BooksByAuthorIds;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
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
        debug!("books_by_author_ids_and_serie_id <- {fid}, {mid}, {lid}, {sid}");

        let query = Query::BooksByAuthorIds;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid])?.mapped(mapper);
            let res = transfrom(rows)?
                .into_iter()
                .filter(|book| {
                    if let Some(serie_id) = book.sid {
                        serie_id == sid
                    } else {
                        false
                    }
                })
                .collect();
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns book by Author by ids without Serie
    pub fn books_by_author_ids_without_serie(
        &self,
        fid: u32,
        mid: u32,
        lid: u32,
    ) -> anyhow::Result<Vec<Book>> {
        debug!("books_by_author_ids_without_serie <- {fid}, {mid}, {lid}");

        let query = Query::BooksByAuthorIds;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([fid, mid, lid])?.mapped(mapper);
            let res = transfrom(rows)?
                .into_iter()
                .filter(|book| book.sid.is_none())
                .collect();
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns book by Serie id
    pub fn books_by_serie_id(&self, sid: u32) -> anyhow::Result<Vec<Book>> {
        debug!("books_by_serie_id <- {sid}");

        let query = Query::BooksBySerieId;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([sid])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns book by Genre id and date filter
    pub fn books_by_genre_id_and_date(&self, gid: u32, date: String) -> anyhow::Result<Vec<Book>> {
        debug!("books_by_genre_id_and_date <- {gid}, {date}");

        let query = Query::BooksByGenreIdAndDate;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query(params![gid, date])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Series by exact serie name
    pub fn books_by_book_title(&self, name: &String) -> anyhow::Result<Vec<Book>> {
        debug!("series_by_serie_name <- {name}");

        let query = Query::BooksByBookTitle;
        if let Mapper::Book(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([name])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns Metas of Genres
    pub fn meta_genres(&self) -> anyhow::Result<Vec<String>> {
        debug!("meta_genres <- ");

        let query = Query::MetaGenres;
        if let Mapper::String(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([])?.mapped(mapper);
            let res = transfrom(rows)?;
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Unexpected mapper"))
        }
    }

    /// Returns genres Meta
    pub fn genres_by_meta(&self, meta: &String) -> anyhow::Result<Vec<Value>> {
        debug!("genres_by_meta <- {meta}");

        let query = Query::GenresByMeta;
        if let Mapper::Value(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
            let rows = statement.query([meta])?.mapped(mapper);
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

        let flags = FunctionFlags::SQLITE_DETERMINISTIC;
        conn.create_scalar_function("LOWER", 1, flags, |ctx| {
            ctx.get::<String>(0).and_then(|s| Ok(s.to_lowercase()))
        })?;
        rusqlite::vtab::array::load_module(&conn)?;
        Ok(Self::new(conn))
    }
}
impl TryFrom<&String> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &String) -> anyhow::Result<Self> {
        OpdsApi::try_from(database.as_str())
    }
}

fn transfrom<T, E, I>(collection: I) -> anyhow::Result<Vec<T>, E>
where
    I: IntoIterator<Item = rusqlite::Result<T, E>>,
{
    collection.into_iter().collect()
}

// #[cfg(test)]
// mod tests {
// }

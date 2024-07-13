use lazy_static;
use rusqlite::Row;
use std::collections::HashMap;

use crate::{Author, Book, Serie, Value};

#[derive(Debug)]
pub enum Mapper {
    String(fn(&Row) -> rusqlite::Result<String>),
    Value(fn(&Row) -> rusqlite::Result<Value>),
    Author(fn(&Row) -> rusqlite::Result<Author>),
    Serie(fn(&Row) -> rusqlite::Result<Serie>),
    Book(fn(&Row) -> rusqlite::Result<Book>),
    None,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Query {
    AuthorNextCharByPrefix,
    SerieNextCharByPrefix,
    AuthorsByLastName,
    SeriesBySerieName,
    SeriesByAuthorIds,
    AuthorByIds,
    BooksByAuthorIdsAndSerieId,
}
impl Query {
    pub const VALUES: [Self; 7] = [
        Self::AuthorNextCharByPrefix,
        Self::SerieNextCharByPrefix,
        Self::AuthorsByLastName,
        Self::SeriesBySerieName,
        Self::SeriesByAuthorIds,
        Self::AuthorByIds,
        Self::BooksByAuthorIdsAndSerieId,
    ];

    pub fn get(&self) -> anyhow::Result<&'static str> {
        match MAP.get(self).cloned() {
            Some(sql) => Ok(sql),
            None => Err(anyhow::anyhow!("SQL for {:?} is not defined", self)),
        }
    }

    pub fn mapper(&self) -> Mapper {
        match self {
            Self::AuthorNextCharByPrefix => Mapper::String(map_to_string),
            Self::SerieNextCharByPrefix => Mapper::String(map_to_string),
            Self::AuthorsByLastName => Mapper::Author(map_to_author),
            Self::SeriesBySerieName => Mapper::Serie(map_to_serie),
            Self::SeriesByAuthorIds => Mapper::Serie(map_to_serie),
            Self::AuthorByIds => Mapper::Author(map_to_author),
            Self::BooksByAuthorIdsAndSerieId => Mapper::Book(map_to_book),
        }
    }
}

lazy_static::lazy_static! {
    static ref MAP: HashMap<Query, &'static str> = {
        let mut m = HashMap::new();
        m.insert(
            Query::AuthorNextCharByPrefix,
            r#"
            SELECT DISTINCT substr(value, 1, $1) AS value
            FROM last_names WHERE value LIKE $2 || '%'
            ORDER BY 1
            COLLATE opds;
            "#
        );
        m.insert(
            Query::SerieNextCharByPrefix,
            r#"
            SELECT DISTINCT substr(value, 1, $1) AS value
            FROM series WHERE value LIKE $2 || '%'
            ORDER BY 1
            COLLATE opds;
            "#
        );
        m.insert(
            Query::AuthorsByLastName,
            r#"
            WITH last_name(fid, mid, lid, value) AS (
                SELECT DISTINCT first_name_id, middle_name_id, id, value
			    FROM last_names LEFT JOIN authors_map ON authors_map.last_name_id = id
                WHERE value = $1
            )
            SELECT
  	            fid, first_names.value AS fname,
                mid, middle_names.value AS mname,
			    lid, last_name.value AS lname
			FROM last_name
            JOIN middle_names ON middle_names.id = mid
            JOIN first_names ON first_names.id = fid
            ORDER BY 6, 2, 4
            COLLATE opds;
            "#
        );
        m.insert(
            Query::SeriesBySerieName,
            r#"
            SELECT
                series.id AS id,
                series.value AS name,
                count(books.book_id) as count,
			    first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
            FROM series
		    JOIN series_map ON series_map.serie_id = series.id
		    JOIN authors_map ON authors_map.book_id = series_map.book_id
		    JOIN books ON books.book_id = series_map.book_id
		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id
            WHERE series.value = $1 AND name IS NOT NULL
            GROUP BY 1, 4, 6, 8
		    ORDER BY 6, 4, 5 COLLATE opds;
            "#
        );
        m.insert(
            Query::SeriesByAuthorIds,
            r#"
           	SELECT
                series.id AS id,
                series.value AS name,
			    count(books.book_id) as count,
			    first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
            FROM authors_map
            LEFT JOIN books ON authors_map.book_id = books.book_id
            LEFT JOIN series_map ON  books.book_id = series_map.book_id
            LEFT JOIN series ON series_map.serie_id = series.id
		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id
            WHERE first_name_id = $1 AND middle_name_id = $2 AND last_name_id = $3 AND name IS NOT NULL
            GROUP BY 1
		    ORDER BY 6, 4, 5 COLLATE opds;
            "#
        );
        m.insert(
            Query::AuthorByIds,
            r#"
            SELECT
  	            first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
            FROM first_names, middle_names, last_names
            WHERE first_names.id = $1 AND middle_names.id = $2 AND last_names.id = $3;
            "#
        );
        m.insert(
            Query::BooksByAuthorIdsAndSerieId,
            r#"
            SELECT
                books.book_id AS id,
                series_map.serie_num AS idx,
                titles.value AS name,
                books.book_size AS size,
                dates.value AS added
            FROM authors_map
            LEFT JOIN books ON authors_map.book_id = books.book_id
            LEFT JOIN titles ON books.title_id = titles.id
            LEFT JOIN series_map ON  books.book_id = series_map.book_id
            LEFT JOIN series ON series_map.serie_id = series.id
            LEFT JOIN dates ON  books.date_id = dates.id
            WHERE first_name_id = $1 AND middle_name_id = $2 AND last_name_id = $3 AND series.id = $4
            ORDER BY 2, 3, 5;
            "#
        );


        assert_eq!(Query::VALUES.len(), m.len());
        return m;
    };
}

fn map_to_string(row: &Row) -> rusqlite::Result<String> {
    let statement = row.as_ref();
    row.get(statement.column_index("value")?)
}

// fn map_to_value(row: &Row) -> rusqlite::Result<Value> {
//     let statement = row.as_ref();
//     let id: u32 = row.get(statement.column_index("id")?)?;
//     let value: String = row.get(statement.column_index("value")?)?;
//     Ok(Value::new(id, value))
// }

fn map_to_author(row: &Row) -> rusqlite::Result<Author> {
    let statement = row.as_ref();

    let fid: u32 = row.get(statement.column_index("fid")?)?;
    let fname: String = row.get(statement.column_index("fname")?)?;

    let mid: u32 = row.get(statement.column_index("mid")?)?;
    let mname: String = row.get::<usize, String>(statement.column_index("mname")?)?;

    let lid: u32 = row.get(statement.column_index("lid")?)?;
    let lname: String = row.get::<usize, String>(statement.column_index("lname")?)?;

    Ok(Author::new(
        Value::new(fid, fname),
        Value::new(mid, mname),
        Value::new(lid, lname),
    ))
}

fn map_to_serie(row: &Row) -> rusqlite::Result<Serie> {
    let statement = row.as_ref();

    let id: u32 = row.get(statement.column_index("id")?)?;
    let name: String = row.get(statement.column_index("name")?)?;
    let count: u32 = row.get(statement.column_index("count")?)?;
    let author = map_to_author(row)?;

    Ok(Serie::new(id, name, count, author))
}

fn map_to_book(row: &Row) -> rusqlite::Result<Book> {
    let statement = row.as_ref();

    let id: u32 = row.get(statement.column_index("id")?)?;
    let idx: u32 = row.get(statement.column_index("idx")?)?;
    let name: String = row.get(statement.column_index("name")?)?;
    let size: u32 = row.get(statement.column_index("size")?)?;
    let added: String = row.get(statement.column_index("added")?)?;

    Ok(Book::new(id, idx, name, size, added))
}

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
    AuthorByIds,
    AuthorNextCharByPrefix,
    AuthorsByBooksIds,
    AuthorsByGenreId,
    AuthorsByLastName,
    BookById,
    BookNextCharByPrefix,
    BooksByAuthorIds,
    BooksByGenreIdAndDate,
    BooksBySerieId,
    GenresByMeta,
    MetaGenres,
    SerieNextCharByPrefix,
    SeriesByAuthorIds,
    SeriesByGenreId,
    SeriesByIds,
    SeriesBySerieName,
}
impl Query {
    pub const VALUES: [Self; 17] = [
        Self::AuthorNextCharByPrefix,
        Self::SerieNextCharByPrefix,
        Self::BookNextCharByPrefix,
        Self::AuthorsByLastName,
        Self::SeriesBySerieName,
        Self::SeriesByAuthorIds,
        Self::AuthorByIds,
        Self::BookById,
        Self::BooksByAuthorIds,
        Self::BooksBySerieId,
        Self::MetaGenres,
        Self::GenresByMeta,
        Self::SeriesByGenreId,
        Self::AuthorsByGenreId,
        Self::BooksByGenreIdAndDate,
        Self::AuthorsByBooksIds,
        Self::SeriesByIds,
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
            Self::BookNextCharByPrefix => Mapper::String(map_to_string),

            Self::AuthorByIds => Mapper::Author(map_to_author),
            Self::AuthorsByGenreId => Mapper::Author(map_to_author),
            Self::AuthorsByLastName => Mapper::Author(map_to_author),
            Self::AuthorsByBooksIds => Mapper::Author(map_to_author),

            Self::SeriesByIds => Mapper::Serie(map_to_serie),
            Self::SeriesByGenreId => Mapper::Serie(map_to_serie),
            Self::SeriesBySerieName => Mapper::Serie(map_to_serie),
            Self::SeriesByAuthorIds => Mapper::Serie(map_to_serie),

            Self::BookById => Mapper::Book(map_to_book),
            Self::BooksBySerieId => Mapper::Book(map_to_book),
            Self::BooksByAuthorIds => Mapper::Book(map_to_book),
            Self::BooksByGenreIdAndDate => Mapper::Book(map_to_book),

            Self::MetaGenres => Mapper::String(map_to_string),
            Self::GenresByMeta => Mapper::Value(map_to_value),
        }
    }
}

lazy_static::lazy_static! {
    static ref MAP: HashMap<Query, &'static str> = {
        let mut m = HashMap::new();
        m.insert(
            Query::AuthorNextCharByPrefix, r#"
            SELECT DISTINCT substr(value, 1, $1) AS value
            FROM last_names WHERE LOWER(value) LIKE LOWER($2) || '%'
            ORDER BY value COLLATE opds;
            "#
        );
        m.insert(
            Query::SerieNextCharByPrefix, r#"
            SELECT DISTINCT substr(value, 1, $1) AS value
            FROM series WHERE LOWER(value) LIKE LOWER($2) || '%'
            ORDER BY value COLLATE opds;
            "#
        );
        m.insert(
            Query::BookNextCharByPrefix, r#"
            SELECT DISTINCT substr(value, 1, $1) AS value
            FROM titles WHERE LOWER(value) LIKE LOWER($2) || '%'
            ORDER BY value COLLATE opds;
            "#
        );
        m.insert(
            Query::AuthorsByLastName, r#"
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
            ORDER BY lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(
            Query::AuthorsByBooksIds, r#"
            SELECT DISTINCT
  	            first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
			FROM authors_map
			JOIN first_names ON first_names.id = authors_map.first_name_id
			JOIN middle_names ON middle_names.id = authors_map.middle_name_id
			JOIN last_names ON last_names.id = authors_map.last_name_id
			WHERE authors_map.book_id IN rarray($1)
            ORDER BY lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(
            Query::SeriesByIds, r#"
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
            WHERE series.id IN rarray($1)
            GROUP BY 1, 4, 6, 8
		    ORDER BY name, lname, fname, mname COLLATE opds;
        "#);
        m.insert(
            Query::SeriesBySerieName, r#"
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
		    ORDER BY name, lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(
            Query::SeriesByAuthorIds, r#"
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
		    ORDER BY name, lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(
            Query::SeriesByGenreId, r#"
           	WITH accepted(id) AS (
                SELECT book_id FROM genres_map WHERE genre_id = $1
            )
            SELECT
			    series.id AS id,
			    series.value AS name,
			    count(series.value) AS count,
			    first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
            FROM accepted
            JOIN series_map ON series_map.book_id = accepted.id
            JOIN series ON series.id = series_map.serie_id
		    JOIN authors_map ON authors_map.book_id = accepted.id
		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id

            WHERE series.value IS NOT NULL
            GROUP BY 1, 4, 6, 8
		    ORDER BY name, lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(Query::AuthorsByGenreId, r#"
            WITH accepted(id) AS (
                SELECT book_id FROM genres_map WHERE genre_id = $1
            )
            SELECT DISTINCT
  	            first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname
			FROM accepted
			JOIN authors_map ON authors_map.book_id = accepted.id
			JOIN first_names ON first_names.id = authors_map.first_name_id
			JOIN middle_names ON middle_names.id = authors_map.middle_name_id
			JOIN last_names ON last_names.id = authors_map.last_name_id
            ORDER BY lname, fname, mname COLLATE opds;
            "#
        );
        m.insert(
            Query::BooksByGenreIdAndDate, r#"
           	WITH accepted(id) AS (
                SELECT book_id FROM genres_map WHERE genre_id = $1
            )
            SELECT
                books.book_id AS id,
				titles.value AS name,
                series.id AS sid,
                series_map.serie_num AS idx,
                first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname,
                books.book_size AS size,
                dates.value AS added
            FROM accepted
			JOIN books ON books.book_id = accepted.id
            JOIN titles ON titles.id = books.title_id
            LEFT JOIN series_map ON series_map.book_id = accepted.id
		    LEFT JOIN series ON series.id = series_map.serie_id
			JOIN authors_map ON authors_map.book_id = accepted.id
   		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id
			JOIN dates ON  dates.id = books.date_id
			WHERE dates.value LIKE $2
            ORDER BY sid, idx, name, added COLLATE opds;
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
            Query::BookById,
            r#"
			WITH book(book_id, title_id, date_id, book_size) AS (
                SELECT book_id, title_id, date_id, book_size FROM books WHERE book_id = $1
            )
            SELECT
                book.book_id AS id,
				titles.value AS name,
                series.id AS sid,
                series_map.serie_num AS idx,
                first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname,
                book.book_size AS size,
                dates.value AS added
            FROM book
			JOIN authors_map ON authors_map.book_id = book.book_id
            JOIN titles ON titles.id = book.title_id
            JOIN dates ON dates.id = book.date_id
            LEFT JOIN series_map ON series_map.book_id = book.book_id
		    LEFT JOIN series ON series.id = series_map.serie_id
   		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id;
            "#
        );


        m.insert(
            Query::BooksByAuthorIds, r#"
            SELECT
                books.book_id AS id,
				titles.value AS name,
                series.id AS sid,
                series_map.serie_num AS idx,
                first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname,
                books.book_size AS size,
                dates.value AS added
            FROM authors_map
            JOIN books ON books.book_id = authors_map.book_id
            JOIN titles ON titles.id = books.title_id
            JOIN dates ON  books.date_id = dates.id
            LEFT JOIN series_map ON series_map.book_id = books.book_id
		    LEFT JOIN series ON series.id = series_map.serie_id
   		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id
            WHERE first_name_id = $1 AND middle_name_id = $2 AND last_name_id = $3
            ORDER BY sid, idx, name, added COLLATE opds;
            "#
        );
        m.insert(
            Query::BooksBySerieId, r#"
            SELECT
                books.book_id AS id,
				titles.value AS name,
                series.id AS sid,
                series_map.serie_num AS idx,
                first_names.id AS fid, first_names.value AS fname,
                middle_names.id AS mid, middle_names.value AS mname,
			    last_names.id AS lid, last_names.value AS lname,
                books.book_size AS size,
                dates.value AS added
            FROM authors_map
            JOIN books ON books.book_id = authors_map.book_id
            JOIN titles ON titles.id = books.title_id
            JOIN dates ON  books.date_id = dates.id
            LEFT JOIN series_map ON series_map.book_id = books.book_id
		    LEFT JOIN series ON series.id = series_map.serie_id
   		    JOIN first_names ON first_names.id = first_name_id
		    JOIN middle_names ON middle_names.id = middle_name_id
		    JOIN last_names ON last_names.id = last_name_id
            WHERE series.id = $1
            ORDER BY idx, name, added COLLATE opds;
            "#
        );
        m.insert(Query::MetaGenres,
            "SELECT DISTINCT meta AS value FROM genres_def ORDER BY value COLLATE opds"
        );
        m.insert(Query::GenresByMeta,r#"
            SELECT genres.id AS id, genre AS value
            FROM genres_def JOIN genres ON genres.value = genres_def.code
            WHERE meta = $1 ORDER BY value COLLATE opds;
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

fn map_to_value(row: &Row) -> rusqlite::Result<Value> {
    let statement = row.as_ref();
    let id: u32 = row.get(statement.column_index("id")?)?;
    let value: String = row.get(statement.column_index("value")?)?;
    Ok(Value::new(id, value))
}

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
    let name: String = row.get(statement.column_index("name")?)?;
    let sid: Option<u32> = row.get(statement.column_index("sid")?)?;
    let idx: Option<u32> = row.get(statement.column_index("idx")?)?;
    let size: u32 = row.get(statement.column_index("size")?)?;
    let added: String = row.get(statement.column_index("added")?)?;
    let author = map_to_author(row)?;

    Ok(Book::new(id, name, sid, idx, author, size, added))
}

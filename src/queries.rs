use lazy_static;
use rusqlite::Row;
use std::collections::HashMap;

use crate::{author::Author, value::Value};

#[derive(Debug)]
pub enum Mapper {
    String(fn(&Row) -> rusqlite::Result<String>),
    Value(fn(&Row) -> rusqlite::Result<Value>),
    Author(fn(&Row) -> rusqlite::Result<Author>),
    None,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Query {
    AuthorNextCharByPrefix,
    SerieNextCharByPrefix,
    AuthorsByLastName,
    SeriesBySerieName,
    AuthorByIds,
}
impl Query {
    pub const VALUES: [Self; 5] = [
        Self::AuthorNextCharByPrefix,
        Self::SerieNextCharByPrefix,
        Self::AuthorsByLastName,
        Self::SeriesBySerieName,
        Self::AuthorByIds,
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
            Self::AuthorsByLastName => Mapper::Author(map_to_authors),
            Self::SeriesBySerieName => Mapper::Value(map_to_value),
            Self::AuthorByIds => Mapper::Author(map_to_authors),
            // _ => Mapper::None,
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
            "SELECT DISTINCT id, value FROM series WHERE value = $1 ORDER BY 1 COLLATE opds;"
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

fn map_to_authors(row: &Row) -> rusqlite::Result<Author> {
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

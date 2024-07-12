use lazy_static;
use std::collections::HashMap;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Query {
    AuthorNextCharByPrefix,
    SerieNextCharByPrefix,
    AuthorsByLastName,
    SeriesBySerieName,
}
impl Query {
    pub const VALUES: [Self; 4] = [
        Self::AuthorNextCharByPrefix,
        Self::SerieNextCharByPrefix,
        Self::AuthorsByLastName,
        Self::SeriesBySerieName,
    ];
}

lazy_static::lazy_static! {
    pub static ref MAP: HashMap<Query, &'static str> = {
        let mut m = HashMap::new();
        m.insert(
            Query::AuthorNextCharByPrefix,
            r#"
            SELECT DISTINCT substr(value, 1, $1) AS name
            FROM last_names WHERE value LIKE $2 || '%'
            ORDER BY 1
            COLLATE opds;
            "#);
        m.insert(
            Query::SerieNextCharByPrefix,
            r#"
            SELECT DISTINCT substr(value, 1, $1) AS name
            FROM series WHERE value LIKE $2 || '%'
            ORDER BY 1
            COLLATE opds;
            "#);
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
            "#);
        m.insert(
            Query::SeriesBySerieName,
            "SELECT DISTINCT id, value FROM series WHERE value = $1 ORDER BY 1 COLLATE opds;"
        );

        return m;
    };
}

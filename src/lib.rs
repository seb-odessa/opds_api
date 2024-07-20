use log::{debug, error};
use queries::{Mapper, Query};
use rusqlite::{params, CachedStatement, Connection};

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
        let fetcher = |s: &String| self.authors_next_char_by_prefix(s);
        Self::search_by_mask(prefix, fetcher)
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

    /// Returns Series and NVC of the author name by given prefix
    pub fn search_series_by_prefix(
        &self,
        prefix: &String,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
        let fetcher = |s: &String| self.series_next_char_by_prefix(s);
        Self::search_by_mask(prefix, fetcher)
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

    /// Returns Authors by Genre name
    pub fn authors_by_genre_id(&self, gid: u32) -> anyhow::Result<Vec<Author>> {
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
        use rusqlite::types::Value;
        let query = Query::AuthorsByBooksIds;
        if let Mapper::Author(mapper) = Query::mapper(&query) {
            let mut statement = self.prepare(&query)?;
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

    /// Returns book by Author by ids
    pub fn books_by_author_ids(&self, fid: u32, mid: u32, lid: u32) -> anyhow::Result<Vec<Book>> {
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

    /// Returns Metas of Genres
    pub fn meta_genres(&self) -> anyhow::Result<Vec<String>> {
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
        rusqlite::vtab::array::load_module(&conn)?;
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
    fn search_authors_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.search_authors_by_prefix(&String::from("Александр"))?;

        assert_eq!(
            result,
            (vec![String::from("Александров"), String::from("Александрова")], vec![])
        );
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
    fn search_series_by_prefix() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let result = api.search_series_by_prefix(&String::from("Авро"))?;

        assert_eq!(
            result,
            (vec![String::from("Аврора [Кауфман]")], vec![])
        );
        Ok(())
    }


    #[test]
    fn authors_by_last_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let strings = api
            .authors_by_last_name(&String::from("Кейн"))?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![String::from("Адель Кейн"), String::from("Рэйчел Кейн")]
        );
        Ok(())
    }

    #[test]
    fn series_by_serie_name() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let strings = api
            .series_by_serie_name(&String::from("Кровь на воздух"))?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(result, vec!["Кровь на воздух [Павел Сергеевич Иевлев] (2)"]);
        Ok(())
    }

    #[test]
    fn series_by_genre_id() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let strings = api
            .series_by_genre_id(24)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "Варяг [Мазин] [Александр Владимирович Мазин] (1)",
                "Восток (РИПОЛ) [Владимир Вячеславович Малявин] (1)"
            ]
        );
        Ok(())
    }

    #[test]
    fn authors_by_genre_id() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .authors_by_genre_id(24)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "Дмитрий Михайлович Балашов",
                "Анатолий Сергеевич Бернацкий",
                "Александр Владимирович Волков",
                "Сергей Михайлович Голицын",
                "Сара Гриствуд",
                "Александр Владимирович Мазин",
                "Владимир Вячеславович Малявин",
                "Александр Викторович Марков",
                "Лев Карлосович Масиель Санчес",
                "Говард Пайл",
                "Джеймс Перкинс",
                "Джордж Сартон",
                "Евгений Викторович Старшов",
                "Дон Холлуэй",
                "Петер Шрайнер"
            ]
        );

        Ok(())
    }

    #[test]
    fn books_by_genre_id_and_date() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .books_by_genre_id_and_date(24, String::from("2024-06-0%"))?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "Игра королев. Женщины, которые изменили историю Европы - Сара Гриствуд (2024-06-07) [2.67 MB]",
                "Рыцари, закованные в сталь - Говард Пайл (2024-06-01) [2.46 MB]"
            ]);

        Ok(())
    }

    #[test]
    fn series_by_author_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let strings = api
            .series_by_author_ids(50, 42, 281)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(result, vec!["Кровь на воздух [Павел Сергеевич Иевлев] (2)"]);
        Ok(())
    }

    #[test]
    fn author_by_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;
        let strings = api
            .author_by_ids(50, 42, 281)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(result, vec!["Павел Сергеевич Иевлев"]);
        Ok(())
    }

    #[test]
    fn books_by_author_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .books_by_author_ids(43, 2, 184)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "День писателя - Анна Велес (2024-06-18) [976.19 KB]",
                "2 Хозяин мрачного замка - Анна Велес (2024-06-05) [1.91 MB]"
            ]
        );
        Ok(())
    }

    #[test]
    fn books_by_author_ids_and_serie_id() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .books_by_author_ids_and_serie_id(43, 2, 184, 29)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec!["2 Хозяин мрачного замка - Анна Велес (2024-06-05) [1.91 MB]"]
        );

        Ok(())
    }

    #[test]
    fn books_by_author_ids_without_serie() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .books_by_author_ids_without_serie(43, 2, 184)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec!["День писателя - Анна Велес (2024-06-18) [976.19 KB]"]
        );

        Ok(())
    }

    #[test]
    fn books_by_serie_id() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .books_by_serie_id(29)?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec!["2 Хозяин мрачного замка - Анна Велес (2024-06-05) [1.91 MB]"]
        );

        Ok(())
    }

    #[test]
    fn authors_by_books_ids() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api
            .authors_by_books_ids(vec![768409, 768571, 768746, 768750])?
            .into_iter()
            .map(|a| format!("{a}"))
            .collect::<Vec<_>>();

        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "Анатолий Сергеевич Бернацкий",
                "Сара Гриствуд",
                "Александр Викторович Марков",
                "Говард Пайл"
            ]
        );

        Ok(())
    }

    #[test]
    fn meta_genres() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let strings = api.meta_genres()?;
        let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

        assert_eq!(
            result,
            vec![
                "Деловая литература",
                "Детективы и Триллеры",
                "Документальная литература",
                "Дом и семья",
                "Драматургия",
                "Искусство, Искусствоведение, Дизайн",
                "Компьютеры и Интернет",
                "Литература для детей",
                "Любовные романы",
                "Наука, Образование",
                "Поэзия",
                "Приключения",
                "Проза",
                "Прочее",
                "Религия, духовность, Эзотерика",
                "Справочная литература",
                "Старинное",
                "Техника",
                "Учебники и пособия",
                "Фантастика",
                "Фольклор",
                "Эзотерика",
                "Юмор"
            ]
        );

        Ok(())
    }

    #[test]
    fn genres_by_meta() -> anyhow::Result<()> {
        let api = OpdsApi::try_from(DATABASE)?;

        let result = api.genres_by_meta(&String::from("Деловая литература"))?;

        assert_eq!(
            result,
            vec![
                (47, "Карьера, кадры"),
                (44, "Маркетинг, PR"),
                (48, "Финансы"),
                (120, "Экономика")
            ]
            .into_iter()
            .map(|(id, value)| Value::new(id, value))
            .collect::<Vec<Value>>()
        );

        Ok(())
    }

    fn fetcher(mask: &String) -> anyhow::Result<Vec<String>> {
        let out = match mask.as_str() {
            "A" => vec!["A", "Ab", "Ac"],
            "B" => vec!["B", "BB"],
            "BB" => vec!["BBB"],
            "BBB" => vec!["BBBB"],
            "BBBB" => vec!["BBBB"],
            "C" => vec!["CC", "cc"],
            "CC" => vec!["CCC", "ccc"],
            "CCC" => vec!["CCC", "ccc"],
            "ccc" => vec!["ccc"],
            _ => vec![],
        };
        if out.is_empty() {
            Err(anyhow::anyhow!("Unexpected mask '{mask}'"))
        } else {
            Ok(out.into_iter().map(|s| String::from(s)).collect())
        }
    }

    #[test]
    fn search_by_mask_a() -> anyhow::Result<()> {
        let (exact, tail) = OpdsApi::search_by_mask("A", fetcher)?;
        assert_eq!(
            vec!["A"],
            exact.iter().map(|a| a.as_str()).collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["Ab", "Ac"],
            tail.iter().map(|a| a.as_str()).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn search_by_mask_b() -> anyhow::Result<()> {
        let empty: Vec<&str> = Vec::new();
        let (exact, tail) = OpdsApi::search_by_mask("B", fetcher)?;
        assert_eq!(
            vec!["B", "BBBB"],
            exact.iter().map(|a| a.as_str()).collect::<Vec<_>>()
        );
        assert_eq!(empty, tail.iter().map(|a| a.as_str()).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn search_by_mask_c() -> anyhow::Result<()> {
        let empty: Vec<&str> = Vec::new();
        let (exact, tail) = OpdsApi::search_by_mask("C", fetcher)?;
        assert_eq!(
            vec!["CCC", "ccc"],
            exact.iter().map(|a| a.as_str()).collect::<Vec<_>>()
        );
        assert_eq!(empty, tail.iter().map(|a| a.as_str()).collect::<Vec<_>>());
        Ok(())
    }
}

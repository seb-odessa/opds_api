use super::*;

#[test]
fn books_next_char_by_prefix() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;
    let result = api.books_next_char_by_prefix(&String::from("сТ"))?;

    assert_eq!(result, vec!["Ста", "Сто", "Стр"]);
    Ok(())
}

#[test]
fn search_books_by_prefix() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;
    let result = api.search_books_by_prefix(&String::from("Ав"))?;

    assert_eq!(result, (vec![String::from("Авиатрисы")], vec![]));
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
        vec!["Рыцари, закованные в сталь - Говард Пайл (2024-06-01) [2.46 MB]"]
    );

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
        .books_by_author_ids_and_serie_id(43, 2, 184, 30)?
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
        vec!["3 Трон змей - Фрост Кей (2024-06-05) [1.71 MB]"]
    );

    Ok(())
}

#[test]
fn book_by_id() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;

    let strings = api
        .book_by_id(768409)?
        .into_iter()
        .map(|a| format!("{a}"))
        .collect::<Vec<_>>();

    let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

    assert_eq!(
        result,
        vec!["Рыцари, закованные в сталь - Говард Пайл (2024-06-01) [2.46 MB]"]
    );

    Ok(())
}

#[test]
fn books_by_book_title() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;

    let strings = api
        .books_by_book_title(&"Авиатрисы".to_owned())?
        .into_iter()
        .map(|a| format!("{a}"))
        .collect::<Vec<_>>();

    let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

    assert_eq!(
        result,
        vec!["1 Авиатрисы - Ами Д. Плат (2024-06-30) [2.70 MB]"]
    );

    Ok(())
}

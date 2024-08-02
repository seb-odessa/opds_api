use super::*;

#[test]
fn authors_next_char_by_prefix() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;
    let result = api.authors_next_char_by_prefix(&String::from("сТо"))?;

    assert_eq!(result, vec!["Стое", "Стоу"]);
    Ok(())
}

#[test]
fn search_authors_by_prefix() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;
    let result = api.search_authors_by_prefix(&String::from("Александр"))?;

    assert_eq!(
        result,
        (
            vec![String::from("Александров"), String::from("Александрова")],
            vec![]
        )
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
            "Анатолий Сергеевич Бернацкий",
            "Александр Владимирович Волков",
            "Сергей Михайлович Голицын",
            "Александр Владимирович Мазин",
            "Александр Викторович Марков",
            "Говард Пайл",
            "Джеймс Перкинс",
            "Джордж Сартон",
            "Дон Холлуэй"
        ]
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

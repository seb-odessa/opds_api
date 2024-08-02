use super::*;

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
    let result = api.search_series_by_prefix(&String::from("Авр"))?;

    assert_eq!(result, (vec![String::from("Аврора [Кауфман]")], vec![]));
    Ok(())
}

#[test]
fn series_by_ids() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;

    let strings = api
        .series_by_ids(vec![42, 44, 2])?
        .into_iter()
        .map(|a| format!("{a}"))
        .collect::<Vec<_>>();

    let result = strings.iter().map(|a| a.as_str()).collect::<Vec<_>>();

    assert_eq!(
        result,
        vec![
            "Когда женщины убивают [Диша Боуз] (1)",
            "Когда женщины убивают [Парини Шрофф] (1)",
            "Разрушенное королевство [Л. Дж. Эндрюс] (1)",
            "Родион Ванзаров [Антон Чиж] (1)"
        ]
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
        vec!["Варяг [Мазин] [Александр Владимирович Мазин] (1)"]
    );
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

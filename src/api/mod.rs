use super::*;

mod author;
mod book;
mod serie;

const DATABASE: &'static str = "file:data/fb2-768381-769440.db?mode=ro";

#[test]
fn is_readonly() -> anyhow::Result<()> {
    let api = OpdsApi::try_from(DATABASE)?;
    assert!(api.is_readonly()?);
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



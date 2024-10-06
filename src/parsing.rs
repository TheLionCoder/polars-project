use polars::prelude::*;

pub(crate) fn load_data(path: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let df: DataFrame = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(false))
        .try_into_reader_with_file_path(Some(path.into()))?
        .finish()?;
    Ok(df)
}

pub(crate) fn make_dataset() -> [String; 4] {
    let data: [String; 4] = [
        String::from("2021-03-27T00:00:00+0100"),
        String::from("2021-03-28T00:00:00+0100"),
        String::from("2021-03-29T00:00:00+0200"),
        String::from("2021-03-30T00:00:00+0200"),
    ];
    data
}

pub(crate) fn cast_string_to_date(dataset: &DataFrame) -> Result<DataFrame, PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([col("Date").str().to_date(StrptimeOptions::default())])
        .collect()?;
    Ok(df)
}

pub(crate) fn extract_date_features(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([col("Date").dt().year().alias("year")])
        .collect()?;

    println!("{}", df);
    Ok(())
}

pub(crate) fn convert_timezone(series: &[String; 4]) -> Result<(), PolarsError> {
    let query: Expr = col("date")
        .str()
        .to_datetime(
            Some(TimeUnit::Microseconds),
            None,
            StrptimeOptions {
                format: Some("%Y-%m-%dT%H:%M:%S%z".into()),
                ..Default::default()
            },
            lit("raise"),
        )
        .dt()
        .convert_time_zone("Europe/Brussels".into());

    let mixed_parse: DataFrame = df!(
        "date" => &series,
    )?
    .lazy()
    .select(&[query])
    .collect()?;
    println!("{}", mixed_parse);
    Ok(())
}

use chrono::{NaiveDate};
use polars::prelude::*;

pub(crate) fn load_data(path: &str) -> PolarsResult<DataFrame> {
    let dataset: DataFrame = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(path.into()))?
        .finish()?
        .sort(
            ["Date"],
            SortMultipleOptions::default().with_maintain_order(true)
        )?;
    Ok(dataset)
}

pub(crate) fn make_dataset() -> PolarsResult<DataFrame> {
    let time: Series = polars::time::date_range(
        "time".into(),
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()
            .and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2021, 12, 31).unwrap()
            .and_hms_opt(0, 0, 0).unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None
    )?
        .cast(&DataType::Date)?;

    let dataset: DataFrame = df!(
        "time" => time
    )?;
    Ok(dataset)
}

pub(crate) fn make_grouped_df() -> PolarsResult<DataFrame> {
    let time: DatetimeChunked = polars::time::date_range(
        "time".into(),
        NaiveDate::from_ymd_opt(2021, 12, 16).unwrap()
            .and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2021, 12, 16).unwrap().and_hms_opt(3, 0, 0).unwrap(),
        Duration::parse("30m"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None
    )?;

    let df: DataFrame = df!(
        "time" => time,
        "groups" => &["a", "a", "a", "b", "b", "a", "a"]
    )?;
    Ok(df)
}

pub(crate) fn get_annual_avg_closing_price(dataset: &DataFrame) -> PolarsResult<()> {
    let annual_avg_closing_price: DataFrame = dataset
        .clone()
        .lazy()
        .group_by_dynamic(
            col("Date"),
            [],
            DynamicGroupOptions {
                every: Duration::parse("1y"),
                period: Duration::parse("1y"),
                offset: Duration::parse("0"),
                ..Default::default()
            },
        )
        .agg([col("Close").mean()])
        .collect()?;

    let df_with_year: DataFrame = annual_avg_closing_price
        .lazy()
        .with_columns([
            col("Date").dt().year().alias("year")
        ])
        .collect()?;
    println!("{}", df_with_year);
    Ok(())
}

pub(crate) fn calculate_days_between(dataset: &DataFrame) -> PolarsResult<()> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .group_by_dynamic(
            col("time"),
            [],
            DynamicGroupOptions {
                every: Duration::parse("1mo"),
                period: Duration::parse("1mo"),
                offset: Duration::parse("0"),
                closed_window: ClosedWindow::Left,
                ..Default::default()
            },
        )
        .agg([
            col("time")
                .cum_count(false)
                .reverse()
                .head(Some(3))
                .alias("day/eom"),
            ((col("time").last() - col("time").first()).map(
                |series| {
                    Ok(Some(
                        series.duration()?
                            .into_iter()
                            .map(|day| day.map(|v| v / 1000 / 24 / 60 / 60))
                            .collect::<Int64Chunked>()
                            .into_series()
                    ))
                },
                    GetOutput::from_type(DataType::Int64),
            ) + lit(1)).alias("days_in_month"),
        ]
        ).collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn rolling_operations(dataset: &DataFrame) -> PolarsResult<()> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .group_by_dynamic(
            col("time"),
            [col("groups")],
            DynamicGroupOptions {
                every: Duration::parse("1h"),
                period: Duration::parse("1h"),
                offset: Duration::parse("0"),
                include_boundaries: true,
                closed_window: ClosedWindow::Both,
                ..Default::default()
            },
        )
        .agg([len()])
        .collect()?;
    println!("{}", df);
    Ok(())
}
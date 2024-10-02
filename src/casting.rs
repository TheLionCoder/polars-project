use chrono::prelude::*;
use polars::prelude::*;

pub(crate) fn make_dataset() -> Result<DataFrame, PolarsError> {
    let date: Series = date_range(
        "date".into(),
        NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2022, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None,
    )?
    .cast(&DataType::Date)?;

    let datetime = date_range(
        "datetime".into(),
        NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2022, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None,
    )?;

    let df: DataFrame = df!(
        "date" => date,
        "datetime" => datetime,
        "string" => &[
            "2022-01-01",
            "2022-01-02",
            "2022-01-03",
            "2022-01-04",
            "2022-01-05"
        ]
    )?;

    Ok(df)
}

pub(crate) fn cast_dataframe(df: &DataFrame) -> Result<(), PolarsError> {
    let casted_df: DataFrame = df
        .clone()
        .lazy()
        .select([
            col("date").cast(DataType::Int64),
            col("datetime").cast(DataType::Int64),
            col("date").dt().to_string("%Y-%m-%d").alias("date_string"),
            col("string").str().to_datetime(
                Some(TimeUnit::Microseconds),
                None,
                StrptimeOptions::default(),
                lit("raise")
            )
        ])
        .collect()?;

    println!("Casted df: {}", &casted_df);
    Ok(())
}

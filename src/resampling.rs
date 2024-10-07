use chrono::NaiveDate;
use polars::prelude::*;

pub(crate) fn make_dataset() -> PolarsResult<DataFrame> {
    let time: DatetimeChunked = polars::time::date_range(
        "time".into(),
        NaiveDate::from_ymd_opt(2021, 12, 16).unwrap()
            .and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2021, 12, 16).unwrap()
            .and_hms_opt(3, 0, 30).unwrap(),
        Duration::parse("30m"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None
    )?;

    let df: DataFrame = df!(
        "time" => time,
        "groups" => &["a", "a", "a", "b", "b", "a", "a"],
        "values" => &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    )?;

    Ok(df)
}

pub(crate) fn upsample_data_forward(dataset: &DataFrame) -> PolarsResult<()> {
    let df: DataFrame = dataset
        .clone()
        .upsample::<[String; 0]>([], "time", Duration::parse("15m"))?
        .fill_null(FillNullStrategy::Forward(None))?;

    println!("{}", df);
    Ok(())
}

pub(crate) fn upsample_data_linear(dataset: &DataFrame) -> PolarsResult<()> {
    let df: DataFrame = dataset
        .clone()
        .upsample::<[String; 0]>([], "time", Duration::parse("15m"))?
        .lazy()
        .with_columns([
            col("values").interpolate(InterpolationMethod::Linear)
        ])
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?;

    println!("{}", df);
    Ok(())
}



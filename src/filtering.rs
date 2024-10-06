use chrono::NaiveDate;
use polars::prelude::*;

pub(crate) fn load_data(path: &str) -> Result<DataFrame, PolarsError> {
    let dataset: DataFrame = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(path.into()))?
        .finish()?;
    Ok(dataset)
}

pub(crate) fn make_negatives_date_dataset() -> PolarsResult<DataFrame> {
    let df: DataFrame = df!(
        "ts" => &["-1300-05-23", "-1400-03-02"],
        "values" => &[3, 4]
    )?
        .lazy()
        .with_column(col("ts").str().to_date(StrptimeOptions::default()))
        .collect()?;

    Ok(df)
}

pub(crate) fn filter_by_single_date(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .filter(col("Date").eq(lit(NaiveDate::from_ymd_opt(1995, 10, 16).unwrap())))
        .collect()?;

    println!("{}", df);
    Ok(())
}

pub(crate) fn filter_by_date_range(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .filter(
            col("Date")
                .gt(lit(NaiveDate::from_ymd_opt(1995, 7, 1).unwrap()))
                .and(col("Date").lt(lit(NaiveDate::from_ymd_opt(1995, 11, 1).unwrap()))),
        )
        .collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn filter_with_negative_date(dataset: &DataFrame) -> PolarsResult<()>{
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .filter(col("ts").dt().year().lt(lit(-1300)))
        .collect()?;

    println!("{}", df);
    Ok(())

}

use polars::prelude::*;

pub(crate) fn make_dataset() -> PolarsResult<DataFrame> {
    let ts: [&str; 2] = ["2021-03-27 03:00", "2021-03-28 03:00"];
    let tz_naive: Series = Series::new("tz_naive".into(), &ts);
    let time_zones_df: DataFrame = DataFrame::new(vec![tz_naive])?;

    Ok(time_zones_df)
}

pub(crate) fn replace_time_zone(dataset: &DataFrame) -> PolarsResult<DataFrame> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .select([
            col("tz_naive").str().to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions::default(),
                lit("raise")
            )
        ])
        .with_columns([
            col("tz_naive")
                .dt()
                .replace_time_zone(Some("UTC".into()), lit("raise"), NonExistent::Raise)
                .alias("tz_aware")
        ])
        .collect()?;

    Ok(df)
}

pub(crate) fn convert_time_zone(dataset: &DataFrame) -> PolarsResult<()> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .select([
            col("tz_aware")
                .dt()
                .replace_time_zone(
                    Some("Europe/Brussels".into()),
                    lit("raise"),
                    NonExistent::Raise
                )
                .alias("replace_time_zone"),
            col("tz_aware")
                .dt()
                .convert_time_zone("Asia/Kathmandu".into())
                .alias("convert_time_zone"),
            col("tz_aware")
                .dt()
                .replace_time_zone(
                    None,
                    lit("raise"),
                    NonExistent::Raise
                ).alias("unset_time_zone")
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}
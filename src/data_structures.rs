use chrono::NaiveDate;
use polars::error::PolarsError;
use polars::prelude::*;
use polars::series::Series;

pub(crate) fn make_series() {
    let s = Series::new("a".into(), &[1, 2, 3, 4, 5]);
    println!("{}", s);
}

pub(crate) fn make_dataframe() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "integer" => &[1, 2, 3, 4, 5],
        "date" => &[
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 4).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap().and_hms_opt(0, 0, 0).unwrap()
        ],
        "float" => &[4.0, 5.0, 6.0, 7.0, 8.0]
    )?;

    Ok(df)
}

pub(crate) fn view_data(df: &DataFrame) {
    let df_head: DataFrame = df.head(Some(3));
    let df_tail: DataFrame = df.tail(Some(2));

    println!("Dataframe head: {}", df_head);
    println!("Dataframe tail: {}", df_tail);
}

pub(crate) fn generate_sample_data(df: &DataFrame, n: u8) -> Result<DataFrame, PolarsError> {
    let sample_series: Series = Series::new("".into(), &[n as i32]);

    let sample_df: DataFrame = df.sample_n(&sample_series, false, false, None)?;
    println!("Sampled dataframe: {}", sample_df);
    Ok(sample_df)
}

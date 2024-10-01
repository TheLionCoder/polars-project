use polars::error::PolarsError;
use polars::prelude::*;
use rand::{thread_rng, Rng};

pub(crate) fn make_dataframe() -> Result<DataFrame, PolarsError> {
    let mut array: [f64; 5] = [0f64; 5];
    thread_rng().fill(&mut array);

    let df: DataFrame = df!(
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &array,
        "groups" => &["A", "A", "B", "C", "B"]
    )?;

    Ok(df)
}

pub(crate) fn create_dataframe_subset(df: &DataFrame) -> Result<(), PolarsError> {
    let out: DataFrame = df
        .clone()
        .lazy()
        .select([
            sum("nrs"),
            col("names").sort(Default::default()),
            (mean("nrs") * lit(10)).alias("10xnrs"),
        ])
        .collect()?;

    println!("{:?}", out);
    Ok(())
}

pub(crate) fn extract_features(df: &DataFrame) -> Result<(), PolarsError> {
    let out: DataFrame = df
        .clone()
        .lazy()
        .with_columns([
            sum("nrs").alias("nrs_sum"),
            col("random").count().alias("count"),
        ])
        .collect()?;
    println!("{:}", out);

    Ok(())
}

pub(crate) fn filter_dataframe(df: &DataFrame) -> Result<(), PolarsError> {
    let out: DataFrame = df.clone().lazy().filter(col("nrs").gt(lit(2))).collect()?;
    println!("{}", out);
    Ok(())
}

pub(crate) fn aggregate_dataframe(df: &DataFrame) -> Result<(), PolarsError> {
    let out: DataFrame = df
        .clone()
        .lazy()
        .group_by([col("groups")])
        .agg([
            sum("nrs"),
            col("random").count().alias("count"),
            col("random")
                .filter(col("names").is_not_null())
                .sum()
                .name()
                .suffix("_sum"),
            col("names").reverse().alias("reversed names"),
        ])
        .collect()?;
    println!("{:}", out);
    Ok(())
}

use polars::prelude::*;
use rand::{thread_rng, Rng};
use std::ops::{Div, Mul};

pub(crate) fn make_dataset() -> Result<DataFrame, PolarsError> {
    let mut array: [f64; 5] = [0_f64; 5];
    thread_rng().fill(&mut array);
    let df: DataFrame = df!(
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &array,
        "groups" => &["A", "A", "B", "C", "B"],
    )?;

    Ok(df)
}

pub(crate) fn create_numerical_dataframe(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .select([
            (col("nrs") + lit(5)).alias("nrs+5"),
            (col("nrs") - lit(5)).alias("nrs-5"),
            (col("nrs").mul(col("random"))).alias("nrs*random"),
            (col("nrs").div(col("random"))).alias("nrs/random"),
        ])
        .collect()?;

    println!("Numerical Dataframe{}", df);
    Ok(())
}

pub(crate) fn create_logical_dataframe(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .select([
            col("nrs").gt(1).alias("nrs>1"),
            col("random").lt_eq(0.5).alias("random<=0.5"),
            col("nrs").neq(1).alias("nrs!=1"),
            col("nrs").eq(1).alias("nrs==1"),
            (col("random").lt_eq(0.5))
                .and(col("nrs").gt(1))
                .alias("and_expr"),
            (col("random").lt_eq(0.5))
                .or(col("nrs").gt(1))
                .alias("or_expr"),
        ])
        .collect()?;
    println!("Logical Dataframe: {}", df);
    Ok(())
}

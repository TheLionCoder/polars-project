use polars::prelude::*;

pub(crate) fn make_dataset() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "a" => &[1, 2, 3],
        "b" => &[10, 20, 30],
        "c" => &[0, 1, 2]
    )?;
    Ok(df)
}

pub(crate) fn make_dataset_string() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "a" => &["a", "b", "c"],
        "b" => &[1, 2, 3]
    )?;
    Ok(df)
}

pub(crate) fn sum_data(df: &DataFrame) -> Result<(), PolarsError> {
    let data: DataFrame = df
        .clone()
        .lazy()
        .select([fold_exprs(lit(0), |acc, x|
            (acc + x).map(Some), [col("*").alias("sum")])])
        .collect()?;
    println!("{}", data);
    Ok(())
}

pub(crate) fn apply_a_condition(df: &DataFrame) -> Result<(), PolarsError> {
    let data: DataFrame = df
        .clone()
        .lazy()
        .filter(fold_exprs(
            lit(true),
            |acc, x| acc.bitand(&x).map(Some),
            [col("*").gt(1)],
        ))
        .collect()?;
    println!("{}", data);
    Ok(())

}

pub(crate) fn fold_string_data(df: &DataFrame) -> Result<(), PolarsError> {
    let data: DataFrame = df
        .clone()
        .lazy()
        .select([
            concat_str([col("a"), col("b")], "", false)
        ])
        .collect()?;
    println!("{}", data);
    Ok(())
}

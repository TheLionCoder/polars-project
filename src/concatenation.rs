use polars::prelude::*;

pub(crate) fn make_vertical_dataframes() -> Result<(DataFrame, DataFrame, DataFrame),PolarsError> {
    let v1_df: DataFrame = df!(
        "a" => &[1],
        "b" => &[3]
    )?;

    let v2_df: DataFrame = df!(
        "a" => &[2],
        "b" => &[4]
    )?;

    let v3_df: DataFrame = df!(
        "a" => &[2],
        "d" => &[4],
    )?;

    Ok((v1_df, v2_df, v3_df))
}

pub(crate) fn make_horizontal_dataframes() -> Result<(DataFrame, DataFrame), PolarsError> {
    let h1_df: DataFrame = df!(
        "l1" => &[1, 2],
        "l2" => &[3, 4]
    )?;

    let h2_df: DataFrame = df!(
        "r1" => &[5, 6],
        "r2" => &[7, 8],
        "r3" => &[9, 10]
    )?;

    Ok((h1_df, h2_df))
}

pub(crate) fn concat_vertical_dataframes(v1_df: &DataFrame,
                                         v2_df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = concat([
        v1_df.clone().lazy(), v2_df.clone().lazy()
    ], UnionArgs::default())?
        .collect()?;

    println!("Vertical concatenation: {}", df);
    Ok(())
}

pub(crate) fn concat_horizontal_dataframes(h1_df: &DataFrame,
                                           h2_df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = polars::functions::concat_df_horizontal(
        &[h1_df.clone(), h2_df.clone()], true
    )?;

    println!("Horizontal concatenation: {}", df);
    Ok(())
}

pub(crate) fn concat_diagonal_dataframes(d1_df: &DataFrame, d2_df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = polars::functions::concat_df_diagonal(
        &[d1_df.clone(), d2_df.clone()]
    )?;
    println!("Diagonal concatenation: {}", df);
    Ok(())
}
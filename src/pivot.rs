use polars::prelude::pivot::{pivot_stable};
use polars::prelude::*;

pub(crate) fn make_dataframe() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "foo" => &["A", "A", "B", "B", "C"],
        "n" => [1, 2, 2, 4, 2],
        "bar" => &["k", "l", "m", "n", "o"],
    )?;

    Ok(df)
}

pub(crate) fn pivot_dataset(dataset: &DataFrame) -> Result<(), PolarsError> {

    let df: DataFrame = pivot_stable(
        &dataset,
        ["bar"],
        Some(["foo"]),
        Some(["n"]),
        false,
        None,
        None,
    )?;

    println!("{}", df);
    Ok(())
}

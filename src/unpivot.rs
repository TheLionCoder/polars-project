use polars::prelude::*;

pub(crate) fn make_dataframe() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "A" => &["a", "b", "c"],
        "B" => &[1, 3, 5],
        "C" => &[10, 11, 12],
        "D" => &[2, 4, 6]
    )?;

    Ok(df)
}

pub(crate) fn unpivot_dataset(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset.unpivot(["A", "B"], ["C", "D"])?;

    println!("{}", df);
    Ok(())
}
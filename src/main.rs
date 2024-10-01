use polars::error::PolarsError;
use polars::prelude::DataFrame;

mod contexts;

fn main() -> Result<(), PolarsError> {
    let df: DataFrame = contexts::make_dataframe()?;

    contexts::create_dataframe_subset(&df)?;
    contexts::extract_features(&df)?;
    contexts::filter_dataframe(&df)?;
    contexts::aggregate_dataframe(&df)?;

    Ok(())
}

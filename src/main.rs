use polars::prelude::DataFrame;

mod aggregation;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df: DataFrame = aggregation::download_csv()?;
    println!("{}", &df);
    aggregation::group_data(&df)?;
    aggregation::group_with_condition(&df)?;
    aggregation::filter_data_groups(&df)?;
    aggregation::sort_group_data(&df)?;
    Ok(())
}

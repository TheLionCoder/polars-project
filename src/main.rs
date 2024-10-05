use polars::prelude::*;

mod list_and_arrays;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df: DataFrame = list_and_arrays::make_dataset()?;
    let daily_df: DataFrame = list_and_arrays::make_day_dataset()?;
    let array_df: DataFrame = list_and_arrays::make_array_dataset()?;

    println!("{}", &df);
    println!("{}", &daily_df);
    println!("{}", &array_df);

    list_and_arrays::create_list_column(&df)?;
    list_and_arrays::measure_list_column(&df)?;
    list_and_arrays::compute_within_list(&df)?;
    list_and_arrays::calculate_percentage_rank(&daily_df)?;
    Ok(())
}


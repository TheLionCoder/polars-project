mod grouping;

use polars::prelude::DataFrame;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dataset: DataFrame = grouping::load_data("./assets/apple_stock.csv")?;
    let range_date: DataFrame = grouping::make_dataset()?;

    println!("{}", dataset);
    println!("{}", range_date);

    grouping::get_annual_avg_closing_price(&dataset)?;
    grouping::calculate_days_between(&range_date)?;

    Ok(())
}

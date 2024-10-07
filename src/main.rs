mod time_zones;

use polars::prelude::DataFrame;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let range_date: DataFrame = time_zones::make_dataset()?;
    let df: DataFrame = time_zones::replace_time_zone(&range_date)?;

    println!("{}", range_date);
    println!("{}", df);

    time_zones::convert_time_zone(&df)?;

    Ok(())
}


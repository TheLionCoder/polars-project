use polars::error::PolarsError;
use polars::prelude::DataFrame;

mod strings;

fn main() -> Result<(), PolarsError> {
    let df: DataFrame = strings::make_dataset()?;
    println!("{}", df);
    strings::calculate_string_length(&df)?;
    strings::extract_player(&df)?;
    strings::extract_all_number_occurrences(&df)?;
    strings::replace_a_pattern(&df)?;
    Ok(())

}

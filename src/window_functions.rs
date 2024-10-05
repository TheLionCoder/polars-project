use polars::prelude::*;
use reqwest::blocking::Client;
use std::error::Error;
use std::io::Cursor;

pub(crate) fn download_csv() -> Result<DataFrame, Box<dyn Error>> {
    let data: Vec<u8> = Client::new()
        .get("https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv")
        .send()?
        .text()?
        .bytes()
        .collect();

    let file: Cursor<Vec<u8>> = Cursor::new(data);
    let df: DataFrame = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(file)
        .finish()?;

    Ok(df)
}

pub(crate) fn group_by_aggregation(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .select([
            col("Type 1"),
            col("Type 2"),
            col("Attack")
                .mean()
                .over(["Type 1"])
                .alias("avg_attack_by_type"),
            col("Defense")
                .mean()
                .over(["Type 1", "Type 2"])
                .alias("avg_defense_by_type_combination"),
            col("Attack").mean().alias("avg_attack")
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}

pub(crate) fn filter_dataset(dataset: &DataFrame) -> Result<(), PolarsError> {
    let filtered_df: DataFrame = dataset
        .clone()
        .lazy()
        .filter(col("Type 2").eq(lit("Psychic")))
        .select([col("Name"), col("Type 1"), col("Speed")])
        .collect()?;

    println!("Filtered dataset: {}", filtered_df);
    Ok(())
}

pub(crate) fn sort_and_filter_data(dataset: &DataFrame) -> Result<(), PolarsError> {
    let sorted_df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([cols(["Name", "Speed"])
            .sort_by(
                ["Speed"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .over(["Type 1"])
            ])
        .collect()?;

    println!("Sorted dataset: {}", sorted_df);
    Ok(())
}
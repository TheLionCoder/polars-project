use std::io::Cursor;

use polars::prelude::*;
use reqwest::blocking::Client;

pub(crate) fn download_csv() -> Result<DataFrame, Box<dyn std::error::Error>> {
    let url: &str = "https://theunitedstates.io/congress-legislators/legislators-historical.csv";

    let mut schema: Schema = Schema::default();
    schema.with_column(
        "first_name".into(),
        DataType::Categorical(None, Default::default()),
    );
    schema.with_column(
        "gender".into(),
        DataType::Categorical(None, Default::default()),
    );

    schema.with_column(
        "type".into(),
        DataType::Categorical(None, Default::default()),
    );

    schema.with_column(
        "state".into(),
        DataType::Categorical(None, Default::default()),
    );

    schema.with_column(
        "party".into(),
        DataType::Categorical(None, Default::default()),
    );

    schema.with_column("birthday".into(), DataType::Date);

    let data: Vec<u8> = Client::new()
        .get(url)
        .send()?
        .text()?
        .bytes()
        .collect();


    let dataset = CsvReadOptions::default()
        .with_has_header(true)
        .with_schema_overwrite(Some(Arc::new(schema)))
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .into_reader_with_file_handle(Cursor::new(data))
        .finish()?;
    Ok(dataset)
}

pub(crate) fn group_data(df: &DataFrame) -> Result<(), PolarsError> {
    let dataframe: DataFrame = df
        .clone()
        .lazy()
        .group_by(["first_name"])
        .agg([len(), col("gender"), col("last_name").first()])
        .sort(
            ["len"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true)
        )
        .limit(5)
        .collect()?;
    println!("Grouped data: {}", dataframe);
    Ok(())
}

pub(crate) fn group_with_condition(df: &DataFrame) -> Result<(), PolarsError> {
    let dataframe: DataFrame = df
        .clone()
        .lazy()
        .group_by(["state"])
        .agg([
            col("party").eq(lit("Anti-Administration"))
                .sum()
                .alias("anti"),
                col("party").eq(lit("Pro-Administration"))
                .sum()
                .alias("pro")
        ])
        .sort(
            ["pro"],
            SortMultipleOptions::default()
                .with_order_descending(true)
        )
        .limit(5)
        .collect()?;
    println!("Grouped data with condition: {}", dataframe);
    Ok(())
}

pub(crate) fn filter_data_groups(df: &DataFrame) -> Result<(), PolarsError> {
    let dataframe: DataFrame = df
        .clone()
        .lazy()
        .group_by(["state"])
        .agg([
            avg_birthday("M"),
            avg_birthday("F"),
            col("gender").eq(lit("M")).sum().alias("#_male"),
            col("gender").eq(lit("F")).sum().alias("#_female")
        ])
        .limit(5)
        .collect()?;
    println!("Filtered data groups: {}", dataframe);
    Ok(())

}


pub(crate) fn sort_group_data(df: &DataFrame) -> Result<(), PolarsError> {
    let dataframe: DataFrame = df
        .clone()
        .lazy()
        .sort(
            ["birthday"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true)
        )
        .group_by(["state"])
        .agg([
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person()
                .sort(Default::default())
                .first()
                .alias("alphabetical_first"),
            col("gender")
                .sort_by(["first_name"], SortMultipleOptions::default())
                .first()
                .alias("gender")
        ]
        ).limit(5)
        .collect()?;

    println!("Sorted group data: {}", dataframe);
    Ok(())
}



// Auxiliary functions
fn compute_age() -> Expr {
    lit(202) - col("birthday").dt().year()
}

fn avg_birthday(gender: &str) -> Expr {
    compute_age()
        .filter(col("gender").eq(lit(gender)))
        .mean()
        .alias(format!("avg_{}_birthday", gender))
}

fn get_person() -> Expr {
    col("first_name") + lit(" ") + col("last_name")
}

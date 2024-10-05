use polars::prelude::*;


pub(crate) fn make_dataset() -> Result<DataFrame, PolarsError> {
    let stns: Vec<String> = (1..6).map(|i| format!("Station {i}")).collect();

    let weather_df: DataFrame = df!(
        "station" => &stns,
        "temperatures" => &[
           "20 5 5 E1 7 13 19 9 6 20",
            "18 8 16 1123 E2 8 E2 E2 E2 90 70 40",
            "19 24 E9 16 6 12 10 22",
            "E2 E0 15 7 8 10 E1 24 17 13 6",
            "14 8 E0 16 22 24 E1"
        ],
    )?;
Ok(weather_df)
}

pub(crate) fn make_day_dataset() -> Result<DataFrame, PolarsError> {
    let stns: Vec<String> = (1..11).map(|i| format!("Station {i}")).collect();
    let weather_by_day: DataFrame = df!(
        "station" => &stns,
        "day_1" => &[17, 11, 8, 22, 9, 21, 20, 8, 8, 17],
        "day_2" => &[15, 11, 10, 8, 7, 14, 18, 21, 15, 13],
        "day_3" => &[16, 15, 24, 24, 8, 23, 19, 23, 16, 10]
    )?;
    Ok(weather_by_day)
}

pub(crate) fn make_array_dataset() -> Result<DataFrame, PolarsError> {
    let mut col1: ListPrimitiveChunkedBuilder<Int32Type> = ListPrimitiveChunkedBuilder::new(
        "Array_1".into(), 8, 8, DataType::Int32
    );
    col1.append_slice(&[1, 3]);
    col1.append_slice(&[2, 5]);

    let mut col2: ListPrimitiveChunkedBuilder<Int32Type> = ListPrimitiveChunkedBuilder::new(
        "Array_2".into(), 8, 8, DataType::Int32
    );
    col2.append_slice(&[1, 7, 3]);
    col2.append_slice(&[8, 1, 0]);

    let array_df: DataFrame = DataFrame::new(vec![
        col1.finish().into_series(),
        col2.finish().into_series(),
    ])?;

    Ok(array_df)
}

pub(crate) fn create_list_column(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([
            col("temperatures").str().split(lit(" "))
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}

pub(crate) fn measure_list_column(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([col("temperatures").str().split(lit(" "))])
        .with_columns([
            col("temperatures").list().head(lit(3)).alias("top3"),
            col("temperatures")
                .list()
                .slice(lit(-3), lit(3))
                .alias("bottom_3"),
            col("temperatures").list().len().alias("observations")
        ]).collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn compute_within_list(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([col("temperatures")
            .str()
            .split(lit(" "))
            .list()
            .eval(col("").str().contains(lit("(?i)[a-z]"), false), false)
            .list()
            .sum()
            .alias("errors")
        ])
        .collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn calculate_percentage_rank(dataset: &DataFrame) -> Result<(), PolarsError> {
    let rank_pct = (col("")
        .rank(
            RankOptions {
                method: RankMethod::Average,
                descending: true
            },
            None
        )
        .cast(DataType::Float32) / col("*").count().cast(DataType::Float32))
        .round(2);

    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([
            concat_list([all().exclude(["station"])])?.alias("all_temps")
        ]
        )
        .select([
            all().exclude(["all_temps"]),
            col("all_temps")
                .list()
                .eval(rank_pct, true)
                .alias("temps_rank")
        ]).collect()?;
    println!("{}", df);
    Ok(())
}
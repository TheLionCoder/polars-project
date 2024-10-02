use polars::prelude::*;

pub(crate) fn make_dataset() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "id" => &[1, 2, 3, 4],
        "animal" => &[Some("Crab"), Some("cat and dog"), Some("rab$bit"), None],
        "player" => &[
            "http://vote.com/ballon_dor?candidate=messi&red=polars",
            "http://vote.com/ballon_dor?candidat=jroginho&ref=polars",
            "http://vote.com/ballon_dor?candidate=neymar&ref=polars",
            "http://vote.com/ballon_dor?candidate=ronaldo&ref=polars",
        ],
        "keys" => &[
            "123 bla 45 asd",
            "xyz 678 910t",
            "123 xyz 45 asd",
            "xyz 678 ab 910",
        ],
        "text" => &[
            "123abc",
            "abc456",
            "123abc456",
            "456abc123"
        ]
    )?;

    Ok(df)
}

pub(crate) fn calculate_string_length(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .select([
            col("animal").str().len_bytes().alias("bytes_count"),
            col("animal").str().len_chars().alias("chars_count"),
        ])
        .collect()?;

    println!("calculated dataframe {}", df);
    Ok(())
}

pub(crate) fn extract_player(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .select([col("player").str().extract(
            lit(r"candidate=(\w+)"), 1
        )])
        .collect()?;
    println!("extracted player: {}", df);
    Ok(())
}

pub(crate) fn extract_all_number_occurrences(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .select([
            col("keys")
                .str()
                .extract_all(lit(r"(\d+)"))
                .alias("extracted_nrs")
        ])
        .collect()?;
    println!("extracted numbers df: {}", df);
    Ok(())
}

pub(crate) fn replace_a_pattern(df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = df
        .clone()
        .lazy()
        .with_columns([
            col("text").str().replace(lit(r"abc\b"), lit("ABC"), false),
            col("text")
                .str()
                .replace_all(lit("a"), lit("-"), false)
                .alias("text_replace_all")
        ])
        .collect()?;
    println!("{}", df);
    Ok(())
}
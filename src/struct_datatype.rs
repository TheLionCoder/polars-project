use polars::prelude::*;

pub fn make_dataset() -> Result<DataFrame, PolarsError> {
    let dataset: DataFrame = df!(
        "movie" => &["Cars", "IT", "ET", "Cars", "Up", "IT", "Cars", "ET", "Up", "ET"],
        "theatre" => &["NE", "ME", "IL", "ND", "NE", "SD", "NE", "IL", "IL", "SD"],
        "avg_rating" => &[4.5, 4.4, 4.6, 4.3, 4.8, 4.7, 4.7, 4.9, 4.7, 4.6],
        "count" => &[30, 27, 26, 29, 31, 28, 28, 26, 33, 26]
    )?;
    Ok(dataset)
}

pub(crate) fn make_struct_series() -> Result<Series, PolarsError> {
    let rating_series: Series = df!(
        "movie" => &["Cars", "Toy Story"],
        "theatre" => &["NE", "ME"],
        "avg_rating" => &[4.5, 4.9]
    )?
        .into_struct("ratings".into())
        .into_series();

    Ok(rating_series)
}

pub(crate) fn make_as_struct_df() -> Result<DataFrame, PolarsError> {
    let df: DataFrame = df!(
        "keys" => &["a", "a", "b"],
        "values" => &[10, 7, 1]
    )?;
    Ok(df)
}

pub(crate) fn count_movies_by_theatre(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .select([
            col("theatre").value_counts(true, true, "count", false)
        ])
        .collect()?;
   println!("{}", df);
    Ok(())
}

pub(crate) fn extract_individual_values(series: &Series) -> Result<(), PolarsError> {
    let extracted_series: Series = series
        .struct_()?
        .field_by_name("movie")?;
    println!("{}", extracted_series);
    Ok(())
}

pub(crate) fn rename_struct_keys(series: Series) -> Result<(), PolarsError> {
    let renamed_series: DataFrame = DataFrame::new([series.into_series()].into())?
        .lazy()
        .select([
            col("ratings")
                .struct_()
                .rename_fields(["film", "state", "value"].to_vec())
        ])
        .unnest(["ratings"])
        .collect()?;
    println!("{}", renamed_series);
    Ok(())
}

pub(crate) fn identify_duplicate_rows(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        //.filter(as_struct(Vec::from(&[col("movie"), col("theatre")])).is_duplicated())
        // Error: .is_duplicated() not available for a struct type
        .filter(len().over([col("movie"), col("theatre")]).gt(lit(1)))
        .collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn rank_multicolumn(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .with_columns([
            as_struct(vec![col("count"), col("avg_rating")])
                .rank(
                    RankOptions {
                        method: RankMethod::Dense,
                        descending: false
                    },
                    None
                )
                .over([col("movie"), col("theatre")])
                .alias("rank")
        ])
        .filter(len().over([col("movie"), col("theatre")]).gt(lit(1)))
        .collect()?;
    println!("{}", df);
    Ok(())
}

pub(crate) fn apply_struct_operations(dataset: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = dataset
        .clone()
        .lazy()
        .select([
            // pack to struct type
            as_struct(vec![col("keys"), col("values")])
            // compute the len(a) + b
                .apply(|series| {
                // downcast struct
                let casted: &StructChunked = series.struct_()?;
                    // get the fields as series
                    let s_a: &Series = &casted.fields_as_series()[0];
                    let s_b: &Series = &casted.fields_as_series()[1];

                    // downcast the series to their known type
                    let casted_a: &StringChunked = s_a.str()?;
                    let casted_b: &Int32Chunked = s_b.i32()?;

                    // iterate both `ChunkedArrays'
                    let out: Int32Chunked = casted_a
                        .into_iter()
                        .zip(casted_b)
                        .map(|(opt_a, opt_b)|
                            match (opt_a, opt_b){
                            (Some(a), Some(b)) => Some(a.len() as i32 + b),
                                _ => None,
                        })
                        .collect();

                    Ok(Some(out.into_series()))
                },
                       GetOutput::from_type(DataType::Int32))
                .alias("solution_map_elements"),
            (col("keys").str().count_matches(lit("."), false) + col("values"))
                .alias("solution_expr")
        ])
        .collect()?;
    println!("{}", df);
    Ok(())
}
use polars::datatypes::{DataType, Field};
use polars::error::PolarsError;
use polars::prelude::*;

pub(crate) fn process_population_data_lazy(path: &str) -> Result<DataFrame, PolarsError> {
    let region: Field = Field::new("Region".into(), DataType::String);
    let schema: Schema = Schema::from_iter(vec![region]);
    let schema_ref = SchemaRef::new(schema);

    let query: LazyFrame = LazyCsvReader::new(path)
        .with_dtype_overwrite(Option::from(schema_ref))
        .finish()?
        .filter(col("Region").eq(lit("02")))
        .group_by(vec![col("City")])
        .agg([col("Population").sum()]);

    let df: DataFrame = query.collect()?;

    Ok(df)
}

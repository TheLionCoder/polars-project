use polars::prelude::*;

pub(crate) fn make_customers_dataset() -> Result<DataFrame, PolarsError> {
    let customers_df: DataFrame = df!(
        "customer_id" => &[1, 2, 3],
        "name" => &["Alice", "Bob", "Charlie"]
    )?;
    Ok(customers_df)
}

pub(crate) fn make_orders_dataset() -> Result<DataFrame, PolarsError> {
    let orders_df: DataFrame = df!(
        "order_id" => &["a", "b", "c"],
        "customer_id" => &[1, 2, 3],
        "amount" => &[100, 200, 300]
    )?;
    Ok(orders_df)
}

pub(crate) fn join_data(customers_df: &DataFrame, orders_df: &DataFrame) -> Result<(), PolarsError> {
    let df: DataFrame = customers_df
        .clone()
        .lazy()
        .join(
            orders_df.clone().lazy(),
            [col("customer_id")],
            [col("customer_id")],
            JoinArgs::new(JoinType::Inner)
        )
        .collect()?;

    println!("Inner join df: {}", df);
    Ok(())
}

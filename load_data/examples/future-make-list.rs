use tracing::{ info, error};

#[tokio::main]
async fn main() {
    use data::load_data;

    use polars::prelude::*;

    tracing_subscriber::fmt().init();

    let csv_name = "make-list.csv";

    let files = vec![
        "2022-04.csv",
        "2022-05.csv",
        "2022-06.csv",
    ];
    let sigma_for_abnormal = 2.0f64;
    match load_data(files, sigma_for_abnormal) {
        Ok(df) => {
            let csv_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                // .append(true)
                .open(csv_name)
                .unwrap();
            CsvWriter::new(csv_file)
                .has_header(true)
                .finish(&mut df.clone());
        },
        Err(e) => error!("{:?}", e),
    };
}

pub mod data {
    use polars::prelude::*;

    pub fn load_data<'a, I, T>(files: I, sigma: f64) -> Result<DataFrame>
        where
            I: IntoIterator<Item = T> + 'a,
            T: AsRef<str> + 'a
    {
        let rolling_option = RollingOptions {
            window_size: Duration::parse("20i"),
            min_periods: 20,
            weights: None,
            center: false,
            by: None,
            closed_window: None,
        };

        let query = files.into_iter()
                        .map(|file|
                            LazyCsvReader::new(file.as_ref().into()).finish())
                        .collect::<Result<Vec<LazyFrame>>>()?;
        let df = concat(query, true)?.select([
                                        col("openTime")
                                            .cast(
                                                DataType::Datetime(
                                                    TimeUnit::Milliseconds,
                                                    Some("UTC".into())
                                                )
                                            )
                                            .alias("timestamp"),
                                        cols([
                                            "open",
                                            "high",
                                            "low",
                                            "close",
                                            "volume"
                                        ]),
                                        col("volume").shift(1).rolling_mean(rolling_option.clone()).alias("mean volume"),
                                        col("volume").shift(1).rolling_std(rolling_option.clone()).alias("std volume"),
                                        col("high").shift(1).rolling_mean(rolling_option.clone()).alias("mean high"),
                                        col("high").shift(1).rolling_std(rolling_option.clone()).alias("std high"),
                                        col("low").shift(1).rolling_mean(rolling_option.clone()).alias("mean low"),
                                        col("low").shift(1).rolling_std(rolling_option.clone()).alias("std low"),
                                    ])
                                    .with_columns([
                                        when(
                                            col("volume").shift(1).gt_eq(col("mean volume") + lit(sigma) * col("std volume"))
                                        ).then(
                                            true
                                        ).otherwise(
                                            false
                                        ).alias("abnormal volume"),
                                        when(
                                            col("high").shift(1).gt_eq(col("mean high") + lit(sigma) * col("std high"))
                                        ).then(
                                            true
                                        ).otherwise(
                                            false
                                        ).alias("upper band touched"),
                                        when(
                                            col("low").shift(1).lt_eq(col("mean low") - lit(sigma) * col("std low"))
                                        ).then(
                                            true
                                        ).otherwise(
                                            false
                                        ).alias("lower band touched"),
                                    ])
                                    .with_column(
                                        col("abnormal volume").and(col("upper band touched").xor(col("lower band touched")))
                                        .cumsum(false)
                                        // .forward_fill(None)
                                        .alias("group")
                                    )
                                    .with_row_count("index", None)
                                    .with_columns([
                                        count().over([col("group")]).alias("period"),
                                        col("high").max().over([col("group")]).alias("max high for duration"),
                                        col("high").arg_max().over([col("group")]).alias("offset to max high"),
                                        col("low").min().over([col("group")]).alias("min low for duration"),
                                        col("low").arg_min().over([col("group")]).alias("offset to min low"),
                                        (col("high") / col("open").first() - lit(1f64)).over([col("group")]).alias("rising float"),
                                        (col("low") / col("open").first() - lit(1f64)).over([col("group")]).alias("falling float"),
                                    ])
                                    .with_column(
                                        when(
                                            col("rising float").gt_eq(lit(0.01f64)).and(col("falling float").lt_eq(lit(-0.01f64)))
                                        ).then(
                                            lit(3u32) // "Chaos"
                                        ).when(
                                            col("rising float").gt_eq(lit(0.01f64))
                                        ).then(
                                            lit(2u32) // "Bull"
                                        ).when(
                                            col("falling float").lt_eq(lit(-0.01f64))
                                        ).then(
                                            lit(1u32) // "Bear"
                                        ).otherwise(
                                            lit(0u32) // "Unknown"
                                        ).alias("trend"),
                                    )
                                    .with_column(
                                        col("trend").filter(
                                            col("trend").eq(lit(3u32))
                                            .or(
                                                col("trend").eq(lit(2u32))
                                            ).or(
                                                col("trend").eq(lit(1u32))
                                            )
                                        ).first()
                                        .over([col("group")])
                                        .alias("first reached trend")
                                    )
                                    .collect()?;
        Ok(df)
    }
}
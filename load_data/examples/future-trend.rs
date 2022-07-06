use tracing::{ info, error};

#[tokio::main]
async fn main () {
    use tr::trend;
    use polars::prelude::*;

    tracing_subscriber::fmt().init();
    
    let csv_name = "trend.csv";

    let files = vec![
        "2022-04.csv",
        "2022-05.csv",
        "2022-06.csv",
    ];
    let sigma_for_abnormal = 2.0f64;
    match trend(files, sigma_for_abnormal) {
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
    }
}

pub mod tr {
    use polars::prelude::*;

    #[path = "../future-make-list.rs"] mod make_list;
    use make_list::data::load_data;

    pub fn trend<'a, I, T>(files: I, sigma: f64) -> Result<DataFrame>
        where
        I: IntoIterator<Item = T> + 'a,
        T: AsRef<str> + 'a
    {
        let source = load_data(files, sigma)?;

        source.lazy()
            .filter(col("abnormal volume").eq(lit(true)))
            .select([
                cols(["timestamp"]),
                // the naive version
                // (col("max high for duration") / col("open") - lit(1)).alias("rising"),
                // (col("min low for duration") / col("open") - lit(1)).alias("falling"),
                // (col("open") / col("open").shift(1) - lit(1)).alias("primary"),
                // (col("open").shift(1) / col("open").shift(2) - lit(1)).alias("secondary"),
                // the gradient version divided by temporal variables
                // ((col("max high for duration") / col("open") - lit(1)) / (col("offset to max high") + lit(1))).alias("rising"),
                // ((col("min low for duration") / col("open") - lit(1)) / (col("offset to min low") + lit(1))).alias("falling"),
                // first reached trend
                col("first reached trend").alias("trend"),
                (col("open") / col("open").shift(1) - lit(1f64)).alias("primary"),
                (col("open").shift(1) / col("open").shift(2) - lit(1f64)).alias("secondary"),
            ])
            // .with_column((col("primary") / col("secondary")).alias("ratio"))
            // .with_column(
            //     when(bullish())
            //     .then(lit("△"))
            //     .when(bearish())
            //     .then(lit("▼"))
            //     .otherwise(lit(NULL))
            //     .alias("trend")
            // )
            .collect()
    }

    fn hilow(source: &DataFrame) -> Result<DataFrame> {
        source.clone().lazy()
            .groupby_stable([col("group")])
            .agg([
                col("high").max().alias("max high for duration"),
                col("low").min().alias("min low for duration"),
            ])
            .collect()
    }

    fn add_hilow(source: &DataFrame) -> Result<DataFrame> {
        let hilow = hilow(source)?;
        source
            .join(&hilow, ["group"], ["group"], JoinType::Left, None)
    }

    fn bullish() -> Expr {
        col("ratio").gt_eq(lit(0))
    }

    fn bearish() -> Expr {
        col("ratio").lt_eq(lit(-3))
    }
}
use polars::prelude::*;

pub async fn download_montly_candles(start: &str, end: Option<&str>, interval: &str, symbol: &str) {
    use binance::api::*;
    use binance::futures::market::*;
    use binance::rest_model::KlineSummaries;
    use chrono::{DateTime, Utc, Datelike};

    let market: FuturesMarket = Binance::new(None, None);

    let start_time = DateTime::parse_from_str(start, "%+").unwrap();
    let end_time = match end {
        Some(end) => DateTime::parse_from_str(end, "%+").unwrap().timestamp_millis() as u64,
        None => Utc::now().timestamp_millis() as u64,
    };
    // let end_time = Utc.ymd(2022, 5, 31).and_hms(23, 59, 59).timestamp_millis() as u64;

    let limit = 1440u16; // 15 days for 15m tick
    let file_name = format!("{}-{}-{}.csv", start_time.year(), start_time.month(), start_time.day());
    // TODO: distribute months into fortnight or fifteen days with respect to limits and request weights
    // let mut months = Vec::new();
    // match start_time.day() {
    //     1..=15 => ,
    //     16..=30 => ,
    //     31 => ,
    //     _ => pacic!(),
    // }

    match market.get_klines(symbol, interval, limit, Some(start_time.timestamp_millis() as u64), Some(end_time)).await {
        Ok(KlineSummaries::AllKlineSummaries(answer)) => {
            let lf = 
            write_csv(file_name, answer)
        },
        Err(e) => error!("Error: {:?}", e),
    }
}

pub fn aggregate(lf: LazyFrame, sigma: f64, target_pnl: f64, duration: i64) -> LazyFrame
{
    let rolling_option = RollingOptions {
        window_size: Duration::new(duration),
        min_periods: duration as usize,
        weights: None,
        center: false,
        by: None,
        closed_window: None,
    };

    lf.select([
        timestamp(),
        cols(["open", "high", "low", "close", "volume"]),
        mean_volume(&rolling_option),
        std_volume(&rolling_option),
        mean_high(&rolling_option),
        std_high(&rolling_option),
        mean_low(&rolling_option),
        std_low(&rolling_option),
    ])
    .with_columns([
        abnormal(sigma),
        upper_bound_touched(sigma),
        lower_bound_touched(sigma),
    ])
    .with_column(group())
    .with_columns(stat_over_group())
    .with_column(trend_from_base(target_pnl))
    .with_column(trend_forcast_over_group())
}

pub fn write_csv(lf: LazyFrame, file_name: &str) -> Result<()> {
    let csv_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(file_name)
        .unwrap();
    CsvWriter::new(csv_file)
        .has_header(true)
        .finish(&mut lf.collect().unwrap())
}

pub fn read_csvs<'a, I, T>(files: I) -> Result<LazyFrame>
    where
        I: IntoIterator<Item = T> + 'a,
        T: AsRef<str> + 'a
{
    let query = files
        .into_iter()
        .map(|file| LazyCsvReader::new(file.as_ref().into()).finish())
        .collect::<Result<Vec<LazyFrame>>>()?;
    concat(query, true)
}

fn timestamp() -> Expr {
    col("openTime")
        .cast(
            DataType::Datetime(
                TimeUnit::Milliseconds,
                Some("UTC".into())
            )
        )
        .alias("timestamp")
}

fn mean_volume(rolling_option: &RollingOptions) -> Expr {
    col("volume").shift(1).rolling_mean(rolling_option.clone()).alias("mean volume")
}

fn std_volume(rolling_option: &RollingOptions) -> Expr {
    col("volume").shift(1).rolling_std(rolling_option.clone()).alias("std volume")
}

fn mean_high(rolling_option: &RollingOptions) -> Expr {
    col("high").shift(1).rolling_mean(rolling_option.clone()).alias("mean high")
}

fn std_high(rolling_option: &RollingOptions) -> Expr {
    col("high").shift(1).rolling_std(rolling_option.clone()).alias("std high")
}

fn mean_low(rolling_option: &RollingOptions) -> Expr {
    col("low").shift(1).rolling_mean(rolling_option.clone()).alias("mean low")
}

fn std_low(rolling_option: &RollingOptions) -> Expr {
    col("low").shift(1).rolling_std(rolling_option.clone()).alias("std low")
}

fn abnormal(sigma: f64) -> Expr {
    when(
        col("volume").shift(1).gt_eq(col("mean volume") + lit(sigma) * col("std volume"))
    ).then(
        true
    ).otherwise(
        false
    ).alias("abnormal volume")
}

fn upper_bound_touched(sigma: f64) -> Expr {
    when(
        col("high").shift(1).gt_eq(col("mean high") + lit(sigma) * col("std high"))
    ).then(
        true
    ).otherwise(
        false
    ).alias("upper band touched")
}

fn lower_bound_touched(sigma: f64) -> Expr {
    when(
        col("low").shift(1).lt_eq(col("mean low") - lit(sigma) * col("std low"))
    ).then(
        true
    ).otherwise(
        false
    ).alias("lower band touched")
}

fn group() -> Expr {
    col("abnormal volume").and(col("upper band touched").xor(col("lower band touched")))
    .cumsum(false)
    // .forward_fill(None)
    .alias("group")
}

fn stat_over_group() -> Vec<Expr> {
    vec![
        count().over([col("group")]).alias("period"),
        col("high").max().over([col("group")]).alias("max high for duration"),
        col("high").arg_max().over([col("group")]).alias("offset to max high"),
        col("low").min().over([col("group")]).alias("min low for duration"),
        col("low").arg_min().over([col("group")]).alias("offset to min low"),
        (col("high") / col("open").first() - lit(1f64)).over([col("group")]).alias("rising float"),
        (col("low") / col("open").first() - lit(1f64)).over([col("group")]).alias("falling float"),
    ]
}

fn trend_from_base(target_pnl: f64) -> Expr {
    when(
        col("rising float").gt_eq(lit(target_pnl)).and(col("falling float").lt_eq(lit(-target_pnl)))
    ).then(
        lit(3u32) // "Chaos"
    ).when(
        col("rising float").gt_eq(lit(target_pnl))
    ).then(
        lit(2u32) // "Bull"
    ).when(
        col("falling float").lt_eq(lit(-target_pnl))
    ).then(
        lit(1u32) // "Bear"
    ).otherwise(
        lit(0u32) // "Unknown"
    ).alias("trend from base")
}

fn trend_forcast_over_group() -> Expr {
    col("trend from base").filter(
        col("trend from base").eq(lit(3u32))
        .or(
            col("trend from base").eq(lit(2u32))
        ).or(
            col("trend from base").eq(lit(1u32))
        )
    ).first()
    .over([col("group")])
    .alias("trend_forcast_over_group")
}

mod tests {
    use super::*;

    #[test]
    fn test_aggregate() {
        let files = vec![
            "2022-04.csv",
            "2022-05.csv",
            "2022-06.csv",
        ];
        let lf = read_csvs(files).unwrap();
        let sigma = 2.0f64;
        let target_pnl = 0.01f64;
        let duration = 20i64;
        let table = aggregate(lf, sigma, target_pnl, duration);

        write_csv(table, "make-list.csv");
    }
}
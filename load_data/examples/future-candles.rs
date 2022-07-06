use binance::rest_model::KlineSummary;
use tracing::{ info, error, instrument };
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    market_data().await;
}

#[instrument]
async fn market_data() {
    use binance::api::*;
    use binance::futures::market::*;
    use binance::futures::rest_model::*;

    let market: FuturesMarket = Binance::new(None, None);

    use chrono::{Utc, TimeZone};

    let start_time = Utc.ymd(2022, 6, 1).and_hms(0, 0, 0).timestamp_millis() as u64;
    let end_time = Utc::now().timestamp_millis() as u64;
    // let end_time = Utc.ymd(2022, 5, 31).and_hms(23, 59, 59).timestamp_millis() as u64;

    let symbol = "btcusdt";
    let interval = "15m";
    let limit = 1440u16; // 15 days for 15m tick
    let file_name = "temp.csv";

    match market.get_klines(symbol, interval, limit, Some(start_time), Some(end_time)).await {
        Ok(KlineSummaries::AllKlineSummaries(answer)) => write_csv(file_name, answer),//info!("First kline: {:?}", answer),
        Err(e) => error!("Error: {:?}", e),
    }
}

fn write_csv(file_name: &str, records: Vec<KlineSummary>) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    let mut wtr = csv::Writer::from_writer(file);

    for record in records {
        wtr.serialize(record);
    }
    wtr.flush();
}
mod api;
mod bybit_orderbook;
mod bybit_ticker;
mod bybit_trades;
mod load_db;
use anyhow::{Context, Error, Result};
use bybit_orderbook::{BybitOrderbook, BybitOrderbookData};
use bybit_ticker::{BybitTicker, BybitTickerData};
use bybit_trades::{BybitTradeData, BybitTrades};
use clickhouse::{
    self, Client, Row,
    inserter::{self, Inserter},
};
use std::str::FromStr;

use fixnum::{FixedPoint, typenum::U18};
use futures_util::{SinkExt, StreamExt};
use rustls::{client, crypto::CryptoProvider, ticketer};
use serde::{Deserialize, Serialize};
use serde_json;
use std::time::Duration;
use time::{OffsetDateTime, UtcDateTime, UtcOffset, format_description::well_known::Rfc3339};
use tokio::{self, net::TcpStream};
use tokio_tungstenite::{
    self, MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message,
};

pub type Decimal128 = FixedPoint<i128, U18>;

#[derive(Deserialize)]
#[serde(untagged)]
enum Bybit {
    Confirmation { success: bool },
    Topics(BybitTopics),
}

#[derive(Deserialize, Debug)]
pub struct BybitTopics {
    topic: String,
    #[serde(rename = "ts")]
    server_timestamp: u64,
    #[serde(rename = "type")]
    r#type: String,
    data: BybitData,
    #[serde(rename = "cs")]
    cross_sequence: Option<u64>,
    #[serde(rename = "cts")]
    client_timestamp: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum BybitData {
    Trades(Vec<BybitTradeData>),
    Orderbook(BybitOrderbookData),
    Ticker(BybitTickerData),
}

pub async fn fetch_bybit(
    mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    client: Client,
    args: Vec<String>,
) -> Result<()> {
    let sub = serde_json::json!({
        "op": "subscribe",
        "args": args
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    println!("Sub completed");

    let mut orderbook_inserter = client
        .inserter::<BybitOrderbook>("orderbook_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(5)))
        .with_period_bias(0.2);
    let mut trades_inserter = client
        .inserter::<BybitTrades>("trades_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(1)))
        .with_period_bias(0.2);
    let mut ticker_inserter = client
        .inserter::<BybitTicker>("ticker_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(1)))
        .with_period_bias(0.2);

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(message) => {
                let parsed_message: Bybit =
                    serde_json::from_str(&message).expect("failed to parse json");
                match parsed_message {
                    Bybit::Confirmation { success } => {
                        println!("Connection established. {success:?}")
                    }
                    Bybit::Topics(topic) => {
                        let (server_timestamp, received_timestamp) = get_time(&topic).await?;
                        match topic.data {
                            BybitData::Orderbook(orderbook) => {
                                BybitOrderbook::parse_bybit_orderbook(
                                    &server_timestamp,
                                    &received_timestamp,
                                    &topic.client_timestamp,
                                    orderbook,
                                    &mut orderbook_inserter,
                                )
                                .await?;
                            }
                            BybitData::Trades(trades) => {
                                BybitTrades::parse_bybit_trades(
                                    &server_timestamp,
                                    &received_timestamp,
                                    trades,
                                    &mut trades_inserter,
                                )
                                .await?;
                            }
                            BybitData::Ticker(ticker) => {
                                BybitTicker::parse_bybit_ticker(
                                    &server_timestamp,
                                    &received_timestamp,
                                    ticker,
                                    &mut ticker_inserter,
                                    &topic.cross_sequence.expect("No cross_sequence for tick"),
                                )
                                .await?;
                            }
                        }
                    }
                }
            }

            Message::Ping(b) => ws.send(Message::Pong(b)).await?,
            _ => println!("error"),
        }
    }
    println!("end");
    Ok(())
}

pub async fn get_time(parsed_message: &BybitTopics) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let server_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
        (parsed_message.server_timestamp as i128) * 1_000_000,
    )
    .expect("server timestamp out of range");

    let received_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
        (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) * 1_000_000,
    )
    .expect("received timestamp out of range");
    Ok((server_timestamp, received_timestamp))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = load_db::load_db()
        .await
        .expect("error while loading database");
    CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");
    let url = "wss://stream.bybit.com/v5/public/linear";
    let (ws, _) = connect_async(url).await?;
    // Trades::fetch_trades_bybit(ws, client).await?;
    println!("connected via websocket to {:?}", url);
    fetch_bybit(
        ws,
        client,
        vec![
            // "publicTrade.BTCUSDT".to_string(),
            // "orderbook.50.BTCUSDT".to_string(),
            "tickers.BTCUSDT".to_string(),
        ],
    )
    .await?;

    Ok(())
}
// Utf8Bytes(b"{\"topic\":\"tickers.BTCUSDT\",\"type\":\"snapshot\",\"data\":{\"symbol\":\"BTCUSDT\",\"tickDirection\":\"ZeroMinusTick\",\"price24hPcnt\":\"-0.01951\",\"lastPrice\":\"91650.00\",\"prevPrice24h\":\"93473.70\",\"highPrice24h\":\"94405.00\",\"lowPrice24h\":\"91217.20\",\"prevPrice1h\":\"92684.20\",\"markPrice\":\"91652.79\",\"indexPrice\":\"91698.41\",\"openInterest\":\"52112.489\",\"openInterestValue\":\"4776255010.69\",\"turnover24h\":\"8084090865.5083\",\"volume24h\":\"87043.6070\",\"fundingIntervalHour\":\"8\",\"fundingCap\":\"0.005\",\"nextFundingTime\":\"1767801600000\",\"fundingRate\":\"0.00002751\",\"bid1Price\":\"91650.00\",\"bid1Size\":\"6.277\",\"ask1Price\":\"91650.10\",\"ask1Size\":\"2.660\",\"preOpenPrice\":\"\",\"preQty\":\"\",\"curPreListingPhase\":\"\"},\"cs\":508470514103,\"ts\":1767779450652}")

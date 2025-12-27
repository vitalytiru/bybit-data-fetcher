mod api;
mod bybit_trades;
use bybit_trades::{Trades, BybitTradeData};
use anyhow::{Context, Result};
use clickhouse::{self, Client, Row};
use fixnum::{FixedPoint, typenum::U18};
use futures_util::{SinkExt, StreamExt};
use hex;
use ring::{digest, hmac};
use rustls::crypto::CryptoProvider;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    fs,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use time::OffsetDateTime;
use tokio::{self, net::TcpStream};
use tokio_tungstenite::{
    self, MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message,
};

pub type Decimal128 = FixedPoint<i128, U18>;

#[derive(Deserialize, Debug)]
enum BybitData {
    Trades(Vec<BybitTradeData>),
    Orderbook(BybitOrderbookData),
}

pub async fn fetch_bybit(
    mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    client: Client,
    args: Vec<String>,
) -> Result<()> {
    let mut first_message_skipped = false;
    // 3. send subscription message
    let sub = serde_json::json!({
        "op": "subscribe",
        "args": args
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    while let Some(msg) = ws.next().await {
        if !first_message_skipped {
            first_message_skipped = true;
            continue;
        }
        match msg? {
            Message::Text(message) => {
                let parsed_message: Bybit =
                    serde_json::from_str(&message).expect("failed to parse json");
                let server_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                    (parsed_message.server_timestamp as i128) * 1_000_000,
                )
                .expect("server timestamp out of range");
                let received_timestamp = OffsetDateTime::now_utc();

                match &parsed_message.data {
                    BybitData::Orderbook(orderbook) => (),
                    BybitData::Trades(trades) => {
                        let parsed_trades = Trades::parse_bybit_trade(
                            trades.to_vec(),
                            &server_timestamp,
                            &received_timestamp,
                        );
                    }
                }
            }

            Message::Ping(d) => ws.send(Message::Pong(d)).await?,
            _ => (),
        }
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
pub struct Bybit {
    topic: String,
    #[serde(rename = "ts")]
    server_timestamp: u64,
    r#type: String,
    data: BybitData,
}


#[derive(Deserialize, Debug)]
struct BybitOrderbookData {
    #[serde(rename = "cts")]
    client_timestamp: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: Vec<Vec<String>>,
    #[serde(rename = "a")]
    ask: Vec<Vec<String>>,
    #[serde(rename = "u")]
    update: u64,
    seq: u64,
}

// #[derive(Clone, PartialEq, Row, Serialize, Deserialize)]
// pub struct Trades {
//     #[serde(with = "clickhouse::serde::time::datetime64::millis")]
//     pub server_timestamp: OffsetDateTime,
//     #[serde(with = "clickhouse::serde::time::datetime64::millis")]
//     pub received_timestamp: OffsetDateTime,
//     #[serde(with = "clickhouse::serde::time::datetime64::millis")]
//     pub trade_timestamp: OffsetDateTime,
//     pub symbol: String,
//     pub trade_id: String,
//     pub side: u8,
//     pub price: Decimal128,
//     pub volume: Decimal128,
//     pub tick_direction: String,
//     pub is_block_trade: bool,
//     pub is_rpi: bool,
//     pub seq: u64,
//     pub exchange: String,
// }

pub async fn load_db() -> Result<Client> {
    use clickhouse::Client;
    let password = fs::read_to_string("/home/lastgosu/clickhousepass")
        .expect("error with pass reading")
        .trim()
        .to_string();
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_user("nixos")
        .with_password(format!("{}", password));
    println!("db loaded");
    client
        .query(
            r#"
        CREATE TABLE IF NOT EXISTS trades_raw_ml
        (
            server_timestamp       DateTime64(3, 'UTC'),
            received_timestamp       DateTime64(3, 'UTC'),
            trade_timestamp       DateTime64(3, 'UTC'),
            symbol          LowCardinality(String),
            trade_id        LowCardinality(String),
            side            UInt8,
            price           Decimal128(18),
            volume             Decimal128(18),
            tick_direction  LowCardinality(String),
            is_block_trade  Bool,
            is_rpi          Bool,
            seq             UInt64,
            exchange        LowCardinality(String) DEFAULT 'bybit'
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(trade_timestamp)
        ORDER BY (symbol, trade_timestamp, trade_id)
        SETTINGS index_granularity = 8192
        "#,
        )
        .execute()
        .await?;
    println!("table created or existed");
    Ok(client)
}

pub async fn fetch_bybit_orderbook(
    mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<()> {
    let sub = serde_json::json!({
        "op": "subscribe",
        "args": ["orderbook.50.BTCUSDT" ]
    });
    let mut i = 0;
    ws.send(Message::Text(sub.to_string().into())).await?;
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(message) => {
                println!("{message:?}, {i:?}");
                i += 1
            }
            Message::Ping(message) => (),
            _ => (),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = load_db().await.expect("error while loading database");
    CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");
    let url = "wss://stream.bybit.com/v5/public/linear";
    let (ws, _) = connect_async(url).await?;
    // Trades::fetch_trades_bybit(ws, client).await?;
    println!("connected via websocket to {:?}", url);
    fetch_bybit_orderbook(ws).await?;
    Ok(())
}

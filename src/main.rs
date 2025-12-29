mod api;
mod bybit_orderbook;
mod bybit_trades;
mod load_db;
use anyhow::{Context, Result};
use bybit_orderbook::{BybitOrderbook, BybitOrderbookData};
use bybit_trades::{BybitTradeData, BybitTrades};
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
#[serde(untagged)]
enum Bybit {
    Confirmation { success: bool },
    Topics(BybitTopics),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum BybitData {
    Trades(Vec<BybitTradeData>),
    Orderbook(BybitOrderbookData),
}

#[derive(Deserialize, Debug)]
pub struct BybitTopics {
    topic: String,
    #[serde(rename = "ts")]
    server_timestamp: u64,
    r#type: String,
    data: BybitData,
    #[serde(rename = "cts")]
    client_timestamp: Option<u64>,
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
    let mut trades_inserter = client
        .inserter::<BybitTrades>("trades_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(5)))
        .with_period_bias(0.2);

    let mut orderbook_inserter = client
        .inserter::<BybitOrderbook>("orderbook_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(5)))
        .with_period_bias(0.2);
    println!("here");

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(message) => {
                // println!("{message:?}");
                let parsed_message: Bybit =
                    serde_json::from_str(&message).expect("failed to parse json");
                if let Bybit::Confirmation(success) = &parsed_message {
                    println!("{success:?}");
                    continue;
                }
                let parsed_message: BybitTopics =
                    serde_json::from_str(&message).expect("failed to parse json");

                let server_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                    (parsed_message.server_timestamp as i128) * 1_000_000,
                )
                .expect("server timestamp out of range");

                let received_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) * 1_000_000,
                )
                .expect("timestamp out of range");

                match parsed_message.data {
                    BybitData::Orderbook(orderbook) => {
                        let client_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                            (parsed_message
                                .client_timestamp
                                .expect("unable to parse client timestamp")
                                as i128)
                                * 1_000_000,
                        )?;

                        let parsed_orderbook = BybitOrderbook::parse_bybit_orderbook(
                            orderbook,
                            &server_timestamp,
                            &received_timestamp,
                            &client_timestamp,
                        )
                        .expect("failed to parse orderbook");
                        for order in parsed_orderbook {
                            // println!("{order:?}");
                            orderbook_inserter.write(&order).await?
                        }
                        let stats = orderbook_inserter.commit().await?;
                        // if stats.rows > 0 {
                        //     println!(
                        //         "{} bytes, {} rows, {} transactions have been inserted in orderbook",
                        //         stats.bytes, stats.rows, stats.transactions,
                        //     );
                        // }
                    }
                    BybitData::Trades(trades) => {
                        let parsed_trades = BybitTrades::parse_bybit_trade(
                            trades.to_vec(),
                            &server_timestamp,
                            &received_timestamp,
                        );
                        for trade in parsed_trades {
                            trades_inserter.write(&trade).await?
                        }
                        let stats = trades_inserter.commit().await?;
                        // if stats.rows > 0 {

                        // println!(
                        //     "{} bytes, {} rows, {} transactions have been inserted in tradebook",
                        //     stats.bytes, stats.rows, stats.transactions,
                        // );}
                    }
                }
            }

            Message::Ping(d) => ws.send(Message::Pong(d)).await?,
            _ => println!("error"),
        }
    }
    println!("end");
    trades_inserter.end().await?;
    orderbook_inserter.end().await?;
    Ok(())
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
            "publicTrade.BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
        ],
    )
    .await?;

    Ok(())
}

mod api;
mod bybit_orderbook;
mod bybit_ticker;
mod bybit_trades;
mod load_db;
use anyhow::{Context, Result};
use bybit_orderbook::{BybitOrderbook, BybitOrderbookData};
use bybit_ticker::{BybitTicker, BybitTickerData};
use bybit_trades::{BybitTradeData, BybitTrades};
use clickhouse::{self, Client, inserter::Inserter};
use fixnum::{FixedPoint, typenum::U18};
use futures_util::{SinkExt, StreamExt};
use rustls::crypto::CryptoProvider;
use serde::Deserialize;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::{
    self,
    net::TcpStream,
    sync::mpsc,
    sync::mpsc::{Receiver, Sender},
};
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
pub struct BybitTopics {
    topic: String,
    #[serde(rename = "ts")]
    server_timestamp: u64,
    #[serde(rename = "type")]
    ttype: String,
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

pub enum BybitOTT {
    BybitTicker(BybitTicker),
    BybitOrderbook(Vec<BybitOrderbook>),
    BybitTrades(Vec<BybitTrades>),
}

pub async fn async_parse(
    tx: Sender<String>,
    mut parser_rx: Receiver<String>,
    writer_tx: Sender<BybitOTT>,
) -> Result<()> {
    while let Some(message) = parser_rx.recv().await {
        {
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
                            let to_write = BybitOrderbook::parse_bybit_orderbook(
                                server_timestamp,
                                received_timestamp,
                                topic.client_timestamp,
                                orderbook,
                                &topic.ttype,
                                tx.clone(),
                            )
                            .await
                            .context("failed to parse orderbook")?;
                            writer_tx
                                .send(BybitOTT::BybitOrderbook(to_write))
                                .await
                                .context("failed to send orderbook to channel")?;
                        }
                        BybitData::Trades(trades) => {
                            let to_write = BybitTrades::parse_bybit_trades(
                                server_timestamp,
                                received_timestamp,
                                trades,
                            )
                            .await
                            .context("failed to parse trades")?;
                            writer_tx
                                .send(BybitOTT::BybitTrades(to_write))
                                .await
                                .context("failed to send trades to channel")?;
                        }
                        BybitData::Ticker(ticker) => {
                            let to_write = BybitTicker::parse_bybit_ticker(
                                server_timestamp,
                                received_timestamp,
                                ticker,
                                topic.cross_sequence.expect("No cross_sequence for tick"),
                            )
                            .await
                            .context("failed to parse ticker")?;
                            writer_tx
                                .send(BybitOTT::BybitTicker(to_write))
                                .await
                                .context("failed to send ticker to channel")?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn async_write(
    mut writer_rx: Receiver<BybitOTT>,
    mut orderbook_inserter: Inserter<BybitOrderbook>,
    mut trades_inserter: Inserter<BybitTrades>,
    mut ticker_inserter: Inserter<BybitTicker>,
) -> Result<()> {
    while let Some(to_insert) = writer_rx.recv().await {
        match to_insert {
            BybitOTT::BybitTicker(ticker) => {
                ticker_inserter.write(&ticker).await?;
                let stats = ticker_inserter.commit().await?;
                if stats.rows > 0 {
                    println!(
                        "{} bytes, {} rows, {} transactions have been inserted in tickers",
                        stats.bytes, stats.rows, stats.transactions,
                    );
                }
            }
            BybitOTT::BybitOrderbook(orderbook) => {
                for order in orderbook {
                    // println!("{order:?}");
                    orderbook_inserter.write(&order).await?
                }
                let stats = orderbook_inserter.commit().await?;
                if stats.rows > 0 {
                    println!(
                        "{} bytes, {} rows, {} transactions have been inserted in orderbook",
                        stats.bytes, stats.rows, stats.transactions,
                    );
                }
            }
            BybitOTT::BybitTrades(trades) => {
                for trade in trades {
                    trades_inserter.write(&trade).await.expect("err")
                }
                let stats = trades_inserter.commit().await.expect("err");
                if stats.rows > 0 {
                    println!(
                        "{} bytes, {} rows, {} transactions have been inserted in tradebook",
                        stats.bytes, stats.rows, stats.transactions,
                    );
                }
            }
        }
    }
    Ok(())
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
    let (tx, mut rx) = mpsc::channel::<String>(100);
    let (parser_tx, parser_rx) = mpsc::channel::<String>(100000);
    let (writer_tx, writer_rx) = mpsc::channel::<BybitOTT>(100000);

    println!("Sub completed");
    let mut timeout = tokio::time::interval(Duration::from_secs(30));
    let orderbook_inserter = client
        .inserter::<BybitOrderbook>("orderbook_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(5)))
        .with_period_bias(0.2);
    let trades_inserter = client
        .inserter::<BybitTrades>("trades_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(1)))
        .with_period_bias(0.2);
    let ticker_inserter = client
        .inserter::<BybitTicker>("ticker_raw_ml")
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(1)))
        .with_period_bias(0.2);
    tokio::spawn(async move {
        if let Err(e) = async_parse(tx, parser_rx, writer_tx.clone()).await {
            eprintln!("parser task exited: {e}");
        }
    });
    tokio::spawn(async move {
        if let Err(e) = async_write(
            writer_rx,
            orderbook_inserter,
            trades_inserter,
            ticker_inserter,
        )
        .await
        {
            eprintln!("writer task exited: {e}");
        }
    });

    loop {
        tokio::select! {
            Some(Ok(msg))  = ws.next() => match msg {
                Message::Text(message) => parser_tx.send(message.to_string()).await.context("Failed to send to parser channel")?,
            Message::Ping(b) => ws.send(Message::Pong(b)).await?,
            other => println!("Received unexpected message type: {:?}", other),
        },
            Some(msg) = rx.recv() =>
            match msg.as_str() {
                "Reconnect" => break,
                    _ => todo!()

            },
            _ = timeout.tick() =>
            if let Err(_) = ws.send(Message::Ping(Vec::new().into())).await {
                println!("Ping timeout break");
                break;
            }

        }
    }
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
            "publicTrade.BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
            "tickers.BTCUSDT".to_string(),
            "publicTrade.ETHUSDT".to_string(),
            "orderbook.50.ETHUSDT".to_string(),
            "tickers.ETHUSDT".to_string(),
            "tickers.ELSAUSDT".to_string(),
            "publicTrade.ELSAUSDT".to_string(),
            "orderbook.50.ELSAUSDT".to_string(),
        ],
    )
    .await?;

    Ok(())
}

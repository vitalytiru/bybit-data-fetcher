
use crate::bybit_orderbook::{BybitOrderbook, BybitOrderbookData, OrderbookCache};
use crate::bybit_ticker::{BybitTicker, BybitTickerData, TickerCache};
use crate::bybit_trades::{BybitTradeData, BybitTrades};
use anyhow::{Context, Result};
use fixnum::{FixedPoint, typenum::U18};
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

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
pub enum BybitData {
    Trades(Vec<BybitTradeData>),
    Orderbook(BybitOrderbookData),
    Ticker(BybitTickerData),
}

#[derive(Debug)]
pub enum BybitOTT {
    BybitTicker(BybitTicker),
    BybitOrderbook(Vec<BybitOrderbook>),
    BybitTrades(Vec<BybitTrades>),
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

pub async fn async_parse(
    tx: Sender<String>,
    mut parser_rx: Receiver<String>,
    writer_tx: Sender<BybitOTT>,
    orderbook_cache: &mut OrderbookCache,
    ticker_cache: &mut TickerCache,
) -> Result<()> {
    info!("Starting parser task...");

    while let Some(message) = parser_rx.recv().await {
        let parsed_message: Bybit = match serde_json::from_str(&message) {
            Ok(msg) => msg,
            Err(e) => {
                error!(
                    "JSON Deserialization failed: {} | Raw message: {}",
                    e, message
                );
                continue;
            }
        };

        match parsed_message {
            Bybit::Confirmation { success } => {
                info!("Bybit subscription status: success={}", success);
            }
            Bybit::Topics(topic) => {
                if let Err(e) =
                    handle_topic(topic, &tx, &writer_tx, orderbook_cache, ticker_cache).await
                {
                    warn!("Failed to process topic: {:?}", e);
                }
            }
        }
    }

    info!("Parser channel closed. Exiting task.");
    Ok(())
}

async fn handle_topic(
    topic: BybitTopics,
    tx: &Sender<String>,
    writer_tx: &Sender<BybitOTT>,
    orderbook_cache: &mut OrderbookCache,
    ticker_cache: &mut TickerCache,
) -> Result<()> {
    let (server_timestamp, received_timestamp) = get_time(&topic)
        .await
        .context("Failed to calculate timestamps")?;

    match topic.data {
        BybitData::Orderbook(orderbook) => {
            let to_write = BybitOrderbook::parse_bybit_orderbook(
                server_timestamp,
                received_timestamp,
                topic.client_timestamp,
                orderbook,
                &topic.ttype,
                tx.clone(),
                orderbook_cache,
            )
            .await
            .context("Orderbook parse error")?;

            writer_tx
                .send(BybitOTT::BybitOrderbook(to_write))
                .await
                .context("Writer channel closed (Orderbook)")?;
        }

        BybitData::Trades(trades) => {
            let to_write =
                BybitTrades::parse_bybit_trades(server_timestamp, received_timestamp, trades)
                    .await
                    .context("Trades parse error")?;

            writer_tx
                .send(BybitOTT::BybitTrades(to_write))
                .await
                .context("Writer channel closed (Trades)")?;
        }

        BybitData::Ticker(ticker) => {
            let cross_sequence = topic.cross_sequence.ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing cross_sequence for ticker in topic: {}",
                    topic.topic
                )
            })?;

            let to_write = BybitTicker::parse_bybit_ticker(
                server_timestamp,
                received_timestamp,
                ticker,
                cross_sequence,
                &topic.ttype,
                ticker_cache,
            )
            .await
            .context("Ticker parse error")?;

            writer_tx
                .send(BybitOTT::BybitTicker(to_write))
                .await
                .context("Writer channel closed (Ticker)")?;
        }
    }

    Ok(())
}

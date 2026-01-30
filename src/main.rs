mod api;
mod bybit_orderbook;
mod bybit_ticker;
mod bybit_trades;
mod load_db;
mod parser;
mod writer;

use crate::bybit_ticker::BybitTicker;
use crate::bybit_trades::BybitTrades;
use crate::parser::BybitOTT;
use anyhow::{Context, Result};
use bybit_orderbook::{
    BybitCachedOrderbook, BybitOrderbook, BybitOrderbookCachedData, OrderbookCache,
};
use clickhouse::{self, Client};
use futures_util::{SinkExt, StreamExt};
use parser::async_parse;
use rustls::crypto::CryptoProvider;
use std::{collections::HashMap, sync::Arc, time::Duration};
use time::UtcDateTime;
use tokio::{
    self,
    net::TcpStream,
    sync::{Mutex, mpsc},
};
use tokio_tungstenite::{
    self, MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message,
};
use tracing::{Level, info};
use tracing_subscriber::{fmt, prelude::*};

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
    let orderbook_cache = Arc::new(Mutex::new(OrderbookCache::new()));

    tokio::spawn(async move {
        async_parse(tx, parser_rx, writer_tx.clone(), orderbook_cache.clone())
            .await
            .context("parser task exited")
    });

    tokio::spawn(async move {
        writer::async_write(
            writer_rx,
            orderbook_inserter,
            trades_inserter,
            ticker_inserter,
        )
        .await
        .context("writer task exited")
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    fmt().with_max_level(Level::INFO).with_target(false).init();

    info!("Application started");
    let client = load_db::load_db()
        .await
        .expect("error while loading database");
    CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");
    let url = "wss://stream.bybit.com/v5/public/linear";
    let (ws, _) = connect_async(url).await?;
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

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
use bybit_orderbook::{BybitOrderbook, OrderbookCache};
use bybit_ticker::TickerCache;
use clickhouse::{self, Client, inserter::Inserter};
use futures_util::{SinkExt, StreamExt};
use parser::async_parse;
use rustls::crypto::CryptoProvider;
use std::{sync::Arc, time::Duration};
use tokio::{
    self,
    net::TcpStream,
    sync::{
        Mutex,
        mpsc::{Sender, channel},
    },
};
use tokio_tungstenite::{
    self, MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message,
};
use tracing::{Level, error, info};
use tracing_subscriber::fmt;

pub async fn handle_ws(args: Vec<String>) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let url = "wss://stream.bybit.com/v5/public/linear";
    let (mut ws, _) = connect_async(url).await?;
    info!("Connected via websocket to {:?}", url);
    let sub = serde_json::json!({
        "op": "subscribe",
        "args": args
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    info!("Sub completed.");
    Ok(ws)
}

pub async fn setup_inserters(
    client: &Client,
) -> (
    Inserter<BybitOrderbook>,
    Inserter<BybitTrades>,
    Inserter<BybitTicker>,
) {
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
    (orderbook_inserter, trades_inserter, ticker_inserter)
}

pub async fn fetch_bybit(
    mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    parser_tx: Sender<String>,
) -> Result<()> {
    loop {
        tokio::select! {
            Some(Ok(msg))  = ws.next() => match msg {
                Message::Text(message) => {
                    parser_tx.send(message.to_string()).await.context("Failed to send to parser channel.")?},
                Message::Ping(b) => ws.send(Message::Pong(b)).await?,
                Message::Close(_) => {break},
                other => println!("Received unexpected message type: {:?}.", other)},
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt().with_max_level(Level::INFO).with_target(false).init();
    info!("Application started");

    CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("Failed to install ring crypto provider.");

    let symbols = vec![
        "publicTrade.BTCUSDT".to_string(),
        "orderbook.50.BTCUSDT".to_string(),
        "tickers.BTCUSDT".to_string(),
        "publicTrade.ETHUSDT".to_string(),
        "orderbook.50.ETHUSDT".to_string(),
        "tickers.ETHUSDT".to_string(),
        "tickers.ELSAUSDT".to_string(),
        "publicTrade.ELSAUSDT".to_string(),
        "orderbook.50.ELSAUSDT".to_string(),
    ];

    let client = load_db::load_db()
        .await
        .expect("Error while loading database.");
    let (orderbook_inserter, trades_inserter, ticker_inserter) = setup_inserters(&client).await;
    let (tx, mut rx) = channel::<String>(100);
    let (parser_tx, parser_rx) = channel::<String>(100_000);
    let (writer_tx, writer_rx) = channel::<BybitOTT>(100_000);

    let orderbook_cache = Arc::new(Mutex::new(OrderbookCache::new()));
    let mut ticker_cache = TickerCache::new();

    tokio::spawn(async move {
        async_parse(tx, parser_rx, writer_tx, orderbook_cache, &mut ticker_cache).await
    });
    tokio::spawn(async move {
        writer::async_write(
            writer_rx,
            orderbook_inserter,
            trades_inserter,
            ticker_inserter,
        )
        .await
    });

    loop {
        let ws = handle_ws(symbols.clone()).await?;
        tokio::select! {
            res = fetch_bybit(ws, parser_tx.clone()) => {
                if let Err(e) = res {
                    error!("WS Error: {:?}. Reconnecting...", e);
                }
            }
            Some(msg) = rx.recv() => {
                if msg == "Reconnect" {
                    info!("Received Reconnect signal from parser. Restarting WS...");
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

use crate::Decimal128;
use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};
use time::OffsetDateTime;

#[derive(Deserialize, Debug, Clone)]
pub struct BybitTradeData {
    #[serde(rename = "T")]
    trade_timestamp: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "L")]
    tick_direction: String,
    #[serde(rename = "i")]
    trade_id: String,
    #[serde(rename = "BT")]
    is_block_trade: bool,
    #[serde(rename = "RPI")]
    is_rpi: bool,
    seq: u64,
}

#[derive(Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct BybitTrades {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub server_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub received_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub trade_timestamp: OffsetDateTime,
    pub symbol: String,
    pub trade_id: String,
    pub side: String,
    pub price: Decimal128,
    pub volume: Decimal128,
    pub tick_direction: String,
    pub is_block_trade: bool,
    pub is_rpi: bool,
    pub seq: u64,
    pub exchange: String,
}

impl BybitTrades {
    pub async fn parse_bybit_trades(
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
        trade_data: Vec<BybitTradeData>,
        client: Client,
    ) -> Result<()> {
        let parsed_trades =
            Self::parse_bybit_trade(trade_data, server_timestamp, received_timestamp);
        let mut trades_inserter = client
            .inserter::<BybitTrades>("trades_raw_ml")
            .with_max_rows(100)
            .with_period(Some(Duration::from_secs(5)))
            .with_period_bias(0.2);
        for trade in parsed_trades {
            trades_inserter.write(&trade).await?
        }
        let stats = trades_inserter.commit().await?;
        if stats.rows > 0 {
            println!(
                "{} bytes, {} rows, {} transactions have been inserted in tradebook",
                stats.bytes, stats.rows, stats.transactions,
            );
        }

        Ok(())
    }

    fn parse_bybit_trade(
        data: Vec<BybitTradeData>,
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
    ) -> Vec<Self> {
        data.iter()
            .map(|trade| {
                Self::parse_bybit_trade_data(trade, &server_timestamp, &received_timestamp).unwrap()
            })
            .collect()
    }

    fn parse_bybit_trade_data(
        td: &BybitTradeData,
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
    ) -> Result<Self> {
        Ok(Self {
            server_timestamp: *server_timestamp,
            received_timestamp: *received_timestamp,
            trade_timestamp: OffsetDateTime::from_unix_timestamp_nanos(
                (td.trade_timestamp as i128) * 1_000_000,
            )
            .expect("trade timestamp out of range"),
            symbol: td.symbol.clone(),
            trade_id: td.trade_id.clone(),
            side: if td.side == "Buy" { "Buy" } else { "Sell" }.to_string(),
            price: Decimal128::from_str(&td.price)?,
            volume: Decimal128::from_str(&td.volume)?,
            tick_direction: td.tick_direction.clone(),
            is_block_trade: td.is_block_trade,
            is_rpi: td.is_rpi,
            seq: td.seq,
            exchange: "Bybit".to_string(),
        })
    }
}

use anyhow::Result;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};
use time::OffsetDateTime;

use crate::Decimal128;
#[derive(Clone, PartialEq, Row, Serialize, Deserialize, Debug)]
pub struct BybitOrderbook {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub server_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub received_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub client_timestamp: OffsetDateTime,
    pub symbol: String,
    pub side: String,
    pub price: Decimal128,
    pub volume: Decimal128,
    pub update: u64,
    pub seq: u64,
    pub exchange: String,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitOrderbookData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: Vec<[String; 2]>,
    #[serde(rename = "a")]
    ask: Vec<[String; 2]>,
    #[serde(rename = "u")]
    update: u64,
    seq: u64,
}

impl BybitOrderbook {
    pub async fn parse_bybit_orderbook(
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
        client_timestamp: &Option<u64>,
        orderbook: BybitOrderbookData,
        orderbook_inserter: &mut Inserter<Self>,
    ) -> Result<()> {
        let client_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            (client_timestamp.expect("unable to parse client timestamp") as i128) * 1_000_000,
        )?;

        let parsed_orderbook = Self::parse_orderbook(
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
        if stats.rows > 0 {
            println!(
                "{} bytes, {} rows, {} transactions have been inserted in orderbook",
                stats.bytes, stats.rows, stats.transactions,
            );
        }
        Ok(())
    }

    fn parse_orderbook(
        data: BybitOrderbookData,
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
        client_timestamp: &OffsetDateTime,
    ) -> Result<Vec<Self>> {
        let mut orderbook: Vec<Self> = Vec::with_capacity(data.bid.len() + data.ask.len());
        let create_order = |price: &str, volume: &str, side: &str| -> Result<Self> {
            Ok(Self {
                server_timestamp: *server_timestamp,
                received_timestamp: *received_timestamp,
                client_timestamp: *client_timestamp,
                symbol: data.symbol.clone(),
                side: side.to_string(),
                price: Decimal128::from_str(&price)?,
                volume: Decimal128::from_str(&volume)?,
                update: data.update.clone(),
                seq: data.seq.clone(),
                exchange: "Bybit".to_string(),
            })
        };
        for bid in data.bid {
            let [price, volume] = bid;
            orderbook.push(create_order(&price, &volume, "Bid").expect("bid push error"));
        }

        for ask in data.ask {
            let [price, volume] = ask;
            orderbook.push(create_order(&price, &volume, "Ask").expect("ask push error"));
        }

        Ok(orderbook)
    }
}

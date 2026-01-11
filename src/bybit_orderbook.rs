use crate::Decimal128;
use anyhow::Result;
use clickhouse::{Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{
    str::FromStr,
    sync::{LazyLock, Mutex, OnceLock},
    time::Duration,
};
use time::{OffsetDateTime, UtcDateTime};

static ORDERBOOK_CACHED: LazyLock<Mutex<BybitCachedOrderbook>> = LazyLock::new(|| {
    let now_time = UtcDateTime::now().into();
    BybitCachedOrderbook {
        server_timestamp: now_time,
        ttype: "".to_string(),
        data: {
            BybitOrderbookCachedData {
                symbol: "".to_string(),
                bid: HashMap::new(),
                ask: HashMap::new(),
                update: 0,
            }
        },
        client_timestamp: now_time,
        received_timestamp: now_time,
    }
    .into()
});

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitCachedOrderbook {
    server_timestamp: OffsetDateTime,
    ttype: String,
    data: BybitOrderbookCachedData,
    client_timestamp: OffsetDateTime,
    received_timestamp: OffsetDateTime,
}
#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitOrderbookCachedData {
    symbol: String,
    bid: HashMap<String, String>,
    ask: HashMap<String, String>,
    update: u64,
}

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
}

impl BybitOrderbook {
    pub async fn parse_bybit_orderbook(
        server_timestamp: OffsetDateTime,
        received_timestamp: OffsetDateTime,
        client_timestamp: Option<u64>,
        orderbook: BybitOrderbookData,
        orderbook_inserter: &mut Inserter<Self>,
        ttype: &String,
    ) -> Result<()> {
        let client_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            (client_timestamp.expect("unable to parse client timestamp") as i128) * 1_000_000,
        )?;

        let mut cached_orderbook_data = BybitOrderbookCachedData {
            symbol: orderbook.symbol,
            update: orderbook.update,

            bid: orderbook
                .bid
                .into_iter()
                .map(|a| (a[0].to_string(), a[1].to_string()))
                .collect(),

            ask: orderbook
                .ask
                .into_iter()
                .map(|a| (a[0].to_string(), a[1].to_string()))
                .collect(),
        };

        let cached_orderbook = BybitCachedOrderbook {
            server_timestamp: server_timestamp,
            ttype: ttype.to_string(),
            data: cached_orderbook_data,
            client_timestamp: client_timestamp,
            received_timestamp: received_timestamp,
        };
        let mut cached_orderbook_mutex = ORDERBOOK_CACHED.lock().unwrap();

        match ttype {
            s if s == "snapshot" => {
                *cached_orderbook_mutex = cached_orderbook;
            }
            s if s == "delta" => {
                for bid in cached_orderbook_mutex.data.bid.clone() {
                    let (price, volume) = bid;
                    if volume == "0" {
                        cached_orderbook_mutex.data.bid.remove(&price);
                    } else {
                        cached_orderbook_mutex.data.bid.insert(price, volume);
                    }
                }
                for ask in cached_orderbook_mutex.data.ask.clone() {
                    let (price, volume) = ask;
                    if volume == "0" {
                        cached_orderbook_mutex.data.ask.remove(&price);
                    } else {
                        cached_orderbook_mutex.data.ask.insert(price, volume);
                    }
                }
            }

            _ => (),
        }

        let parsed_orderbook = Self::parse_orderbook(
            cached_orderbook_mutex.data.clone(),
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
        data: BybitOrderbookCachedData,
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
                exchange: "Bybit".to_string(),
            })
        };
        for bid in data.bid {
            let (price, volume) = bid;
            println!("{:?} bid update", data.update);
            orderbook.push(create_order(&price, &volume, "Bid").expect("bid push error"));
        }

        for ask in data.ask {
            println!("{:?} ask update", data.update);

            let (price, volume) = ask;
            orderbook.push(create_order(&price, &volume, "Ask").expect("ask push error"));
        }

        Ok(orderbook)
    }
}

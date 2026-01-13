use crate::Decimal128;
use anyhow::Result;
use clickhouse::{Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::mpsc;
use std::{
    str::FromStr,
    sync::{LazyLock, Mutex, OnceLock},
    time::Duration,
};
use time::{OffsetDateTime, UtcDateTime};

static ORDERBOOK_CACHED: LazyLock<Mutex<OrderbookCache>> = LazyLock::new(|| {
    let now_time = UtcDateTime::now().into();
    Mutex::new(OrderbookCache {
        orderbook: HashMap::from([(
            "".to_string(),
            BybitCachedOrderbook {
                server_timestamp: now_time,
                ttype: "".to_string(),
                data: BybitOrderbookCachedData {
                    symbol: "".to_string(),
                    bid: HashMap::new(),
                    ask: HashMap::new(),
                    update: 0,
                },
                client_timestamp: now_time,
                received_timestamp: now_time,
            },
        )]),
    })
});
#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct OrderbookCache {
    orderbook: HashMap<String, BybitCachedOrderbook>,
}

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
        tx: tokio::sync::mpsc::Sender<String>,
    ) -> Result<()> {
        let client_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            (client_timestamp.expect("unable to parse client timestamp") as i128) * 1_000_000,
        )?;
        let symbol = orderbook.symbol;
        let cached_orderbook_data = BybitOrderbookCachedData {
            symbol: symbol.clone(),
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
        println!("{cached_orderbook_data:?}");
        let cached_orderbook = BybitCachedOrderbook {
            server_timestamp: server_timestamp,
            ttype: ttype.to_string(),
            data: cached_orderbook_data,
            client_timestamp: client_timestamp,
            received_timestamp: received_timestamp,
        };
        println!("{cached_orderbook:?}");
        match ttype.as_str() {
            "snapshot" => {
                let mut cached_orderbook_mutex = ORDERBOOK_CACHED.lock().unwrap();
                println!("{cached_orderbook_mutex:?}");
                println!("snapshot");
                cached_orderbook_mutex
                    .orderbook
                    .insert(symbol.clone(), cached_orderbook);
                println!("{cached_orderbook_mutex:?}");
            }
            "delta" => {
                let mut cached_orderbook_mutex = ORDERBOOK_CACHED.lock().unwrap();
                let mutex_unwrapped = &mut cached_orderbook_mutex
                    .orderbook
                    .get_mut(&symbol)
                    .unwrap()
                    .data;
                if cached_orderbook.data.update != (mutex_unwrapped.update + 1) {
                    tx.send("Reconnect".to_string()).await?;
                }
                for bid in cached_orderbook.data.bid {
                    let (price, volume) = bid;
                    if volume == "0" {
                        let _ = &mutex_unwrapped.bid.remove(&price).unwrap();
                    } else {
                        mutex_unwrapped.bid.insert(price, volume);
                        mutex_unwrapped.update = cached_orderbook.data.update
                    }
                }
                for ask in cached_orderbook.data.ask {
                    let (price, volume) = ask;
                    if volume == "0" {
                        mutex_unwrapped.ask.remove(&price);
                    } else {
                        mutex_unwrapped.ask.insert(price, volume);
                        mutex_unwrapped.update = cached_orderbook.data.update
                    }
                }
            }

            _ => {
                println!("ERROR");
                ()
            }
        }

        let parsed_orderbook = Self::parse_orderbook(
            &server_timestamp,
            &received_timestamp,
            &client_timestamp,
            &symbol,
        )
        .expect("failed to parse orderbook");
        println!("{parsed_orderbook:?} parsed orderbook");
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
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
        client_timestamp: &OffsetDateTime,
        symbol: &String,
    ) -> Result<Vec<Self>> {
        let mut cached_orderbook = ORDERBOOK_CACHED.lock().unwrap();
        let mutex_unwrapped = &mut cached_orderbook.orderbook.get_mut(symbol).unwrap().data;

        let update = mutex_unwrapped.update.clone();
        let bid_len = mutex_unwrapped.bid.len();
        let ask_len = mutex_unwrapped.ask.len();
        println!("{mutex_unwrapped:?} mutex unwrapped");
        drop(cached_orderbook);

        let mut orderbook: Vec<Self> = Vec::with_capacity(bid_len + ask_len);
        let create_order = |price: &str, volume: &str, side: &str| -> Result<Self> {
            Ok(Self {
                server_timestamp: *server_timestamp,
                received_timestamp: *received_timestamp,
                client_timestamp: *client_timestamp,
                symbol: symbol.clone(),
                side: side.to_string(),
                price: Decimal128::from_str(&price)?,
                volume: Decimal128::from_str(&volume)?,
                update: update.clone(),
                exchange: "Bybit".to_string(),
            })
        };
        let mut cached_orderbook = ORDERBOOK_CACHED.lock().unwrap();
        let mutex_unwrapped = &mut cached_orderbook.orderbook.get_mut(symbol).unwrap().data;

        for bid in &mutex_unwrapped.bid {
            let (price, volume) = bid;
            println!("{:?} bid update", mutex_unwrapped.update);
            // println!("{price:?}");
            orderbook.push(create_order(&price, &volume, "Bid").expect("bid push error"));
        }

        println!("{orderbook:?}");
        for ask in &mutex_unwrapped.ask {
            // println!("{:?} ask update", data.update);

            let (price, volume) = ask;
            // println!("{price:?}");

            orderbook.push(create_order(&price, &volume, "Ask").expect("ask push error"));
        }

        Ok(orderbook)
    }
}

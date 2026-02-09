use crate::parser::Decimal128;
use anyhow::{Context, Result};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use time::OffsetDateTime;

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct OrderbookCache {
    pub orderbook: HashMap<String, BybitCachedOrderbook>,
}

impl OrderbookCache {
    pub fn new() -> Self {
        Self {
            orderbook: HashMap::new(),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitCachedOrderbook {
    pub server_timestamp: OffsetDateTime,
    pub ttype: String,
    pub data: BybitOrderbookCachedData,
    pub client_timestamp: OffsetDateTime,
    pub received_timestamp: OffsetDateTime,
}
#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitOrderbookCachedData {
    pub symbol: String,
    pub bid: HashMap<Decimal128, Decimal128>,
    pub ask: HashMap<Decimal128, Decimal128>,
    pub update: u64,
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
    pub side: &'static str,
    pub price: Decimal128,
    pub volume: Decimal128,
    pub update: u64,
    pub exchange: &'static str,
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
        // orderbook_inserter: &mut Inserter<Self>,
        ttype: &String,
        tx: tokio::sync::mpsc::Sender<String>,
        orderbook_cache: &mut OrderbookCache,
    ) -> Result<Vec<Self>> {
        let client_timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            (client_timestamp.expect("unable to parse client timestamp") as i128) * 1_000_000,
        )?;
        let symbol = orderbook.symbol;
        let new_cache_data = BybitOrderbookCachedData {
            symbol: symbol.clone(),
            update: orderbook.update,

            bid: orderbook
                .bid
                .into_iter()
                .map(|[price, volume]| {
                    Ok((
                        Decimal128::from_str(&price)?,
                        Decimal128::from_str(&volume)?,
                    ))
                })
                .collect::<Result<HashMap<_, _>>>()?,

            ask: orderbook
                .ask
                .into_iter()
                .map(|[price, volume]| {
                    Ok((
                        Decimal128::from_str(&price)?,
                        Decimal128::from_str(&volume)?,
                    ))
                })
                .collect::<Result<HashMap<_, _>>>()?,
        };
        let new_cache_orderbook = BybitCachedOrderbook {
            server_timestamp,
            ttype: ttype.to_string(),
            data: new_cache_data,
            client_timestamp,
            received_timestamp,
        };
        let cache = orderbook_cache;
        match ttype.as_str() {
            "snapshot" => {
                cache.orderbook.insert(symbol.clone(), new_cache_orderbook);
            }
            "delta" => {
                let mut needs_reconnect = false;

                match cache.orderbook.get_mut(&symbol) {
                    Some(cache) => {
                        let cache_data = &mut cache.data;
                        if new_cache_orderbook.data.update == (cache_data.update + 1) {
                            let zero = Decimal128::from_str("0")?;
                            for bid in new_cache_orderbook.data.bid {
                                let (price, volume) = bid;
                                if volume == zero {
                                    let _ = &cache_data.bid.remove(&price).context("Couldnt remove orderbook price. Update id: {new_cache_orderbook.data.update:?}")?;
                                } else {
                                    cache_data.bid.insert(price, volume);
                                    cache_data.update = new_cache_orderbook.data.update
                                }
                            }
                            for ask in new_cache_orderbook.data.ask {
                                let (price, volume) = ask;
                                if volume == zero {
                                    cache_data.ask.remove(&price);
                                } else {
                                    cache_data.ask.insert(price, volume);
                                    cache_data.update = new_cache_orderbook.data.update
                                }
                            }
                        } else {
                            needs_reconnect = true;
                        }
                    }
                    None => needs_reconnect = true,
                }
                if needs_reconnect {
                    tx.send("Reconnect".to_string()).await?;
                    return Ok(vec![]);
                }
            }
            _ => {
                println!("ERROR");
            }
        }
        let cache = cache
            .orderbook
            .get(&symbol)
            .context("couldnt get cached symbol")?;
        let parsed_orderbook = Self::parse_orderbook(
            &server_timestamp,
            &received_timestamp,
            &client_timestamp,
            &symbol,
            cache,
        )
        .await
        .context("failed to parse orderbook")?;
        Ok(parsed_orderbook)
    }

    async fn parse_orderbook(
        server_timestamp: &OffsetDateTime,
        received_timestamp: &OffsetDateTime,
        client_timestamp: &OffsetDateTime,
        symbol: &String,
        orderbook_cache: &BybitCachedOrderbook,
    ) -> Result<Vec<Self>> {
        let cache_data = &orderbook_cache.data;
        let update = cache_data.update;
        let bid_len = cache_data.bid.len();
        let ask_len = cache_data.ask.len();

        let mut orderbook: Vec<Self> = Vec::with_capacity(bid_len + ask_len);
        let create_order =
            |price: Decimal128, volume: Decimal128, side: &'static str| -> Result<Self> {
                Ok(Self {
                    server_timestamp: *server_timestamp,
                    received_timestamp: *received_timestamp,
                    client_timestamp: *client_timestamp,
                    symbol: symbol.clone(),
                    side,
                    price,
                    volume,
                    update,
                    exchange: "Bybit",
                })
            };

        for bid in &cache_data.bid {
            let (price, volume) = bid;
            orderbook.push(create_order(*price, *volume, "Bid").expect("bid push error"));
        }

        for ask in &cache_data.ask {
            let (price, volume) = ask;
            orderbook.push(create_order(*price, *volume, "Ask").expect("ask push error"));
        }

        Ok(orderbook)
    }
}

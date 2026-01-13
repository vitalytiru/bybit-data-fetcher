use crate::Decimal128;
use anyhow::Result;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, time::Duration};
use time::OffsetDateTime;

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct TickerCache {
    orderbook: HashMap<String, BybitCachedTicker>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct BybitCachedTicker {
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
pub struct BybitTicker {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    server_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    received_timestamp: OffsetDateTime,
    cross_sequence: u64,
    symbol: String,
    tick_direction: String,
    price_24h_pcnt: Decimal128,
    last_price: Decimal128,
    prev_price_24h: Decimal128,
    high_price_24h: Decimal128,
    low_price_24h: Decimal128,
    prev_price_1h: Decimal128,
    mark_price: Decimal128,
    index_price: Decimal128,
    open_interest: Decimal128,
    open_interest_value: Decimal128,
    turnover_24h: Decimal128,
    volume_24h: Decimal128,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    next_funding_time: OffsetDateTime,
    funding_rate: Decimal128,
    bid1_price: Decimal128,
    bid1_size: Decimal128,
    ask1_price: Decimal128,
    ask1_size: Decimal128,
    delivery_time: Option<OffsetDateTime>,
    basis_rate: Option<Decimal128>,
    delivery_fee_rate: Option<i64>,
    predicted_delivery_price: Option<Decimal128>,
    pre_open_price: Option<Decimal128>,
    pre_qty: Option<Decimal128>,
    cur_pre_listing_phase: Option<String>,
    funding_interval_hour: Option<String>,
    funding_cap: Option<Decimal128>,
    basis_rate_year: Option<Decimal128>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BybitTickerData {
    pub symbol: String, // Symbol is always present
    #[serde(rename = "tickDirection", default)]
    pub tick_direction: Option<String>,
    #[serde(rename = "price24hPcnt", default)]
    pub price_24h_pcnt: Option<String>,
    #[serde(rename = "lastPrice", default)]
    pub last_price: Option<String>,
    #[serde(rename = "prevPrice24h", default)]
    pub prev_price_24h: Option<String>,
    #[serde(rename = "highPrice24h", default)]
    pub high_price_24h: Option<String>,
    #[serde(rename = "lowPrice24h", default)]
    pub low_price_24h: Option<String>,
    #[serde(rename = "prevPrice1h", default)]
    pub prev_price_1h: Option<String>,
    #[serde(rename = "markPrice", default)]
    pub mark_price: Option<String>,
    #[serde(rename = "indexPrice", default)]
    pub index_price: Option<String>,
    #[serde(rename = "openInterest", default)]
    pub open_interest: Option<String>,
    #[serde(rename = "openInterestValue", default)]
    pub open_interest_value: Option<String>,
    #[serde(rename = "turnover24h", default)]
    pub turnover_24h: Option<String>,
    #[serde(rename = "volume24h", default)]
    pub volume_24h: Option<String>,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: Option<String>,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: Option<String>,
    #[serde(rename = "bid1Price", default)]
    pub bid1_price: Option<String>,
    #[serde(rename = "bid1Size", default)]
    pub bid1_size: Option<String>,
    #[serde(rename = "ask1Price", default)]
    pub ask1_price: Option<String>,
    #[serde(rename = "ask1Size", default)]
    pub ask1_size: Option<String>,
    #[serde(rename = "deliveryTime", default)]
    pub delivery_time: Option<String>,
    #[serde(rename = "basisRate", default)]
    pub basis_rate: Option<String>,
    #[serde(rename = "deliveryFeeRate", default)]
    pub delivery_fee_rate: Option<String>,
    #[serde(rename = "predictedDeliveryPrice", default)]
    pub predicted_delivery_price: Option<String>,
    #[serde(rename = "preOpenPrice", default)]
    pub pre_open_price: Option<String>,
    #[serde(rename = "preQty", default)]
    pub pre_qty: Option<String>,
    #[serde(rename = "curPreListingPhase", default)]
    pub cur_pre_listing_phase: Option<String>,
    #[serde(rename = "fundingIntervalHour", default)]
    pub funding_interval_hour: Option<String>,
    #[serde(rename = "fundingCap", default)]
    pub funding_cap: Option<String>,
    #[serde(rename = "basisRateYear", default)]
    pub basis_rate_year: Option<String>,
}

impl BybitTicker {
    pub async fn parse_bybit_ticker(
        server_timestamp: OffsetDateTime,
        received_timestamp: OffsetDateTime,
        ticker_data: BybitTickerData,
        ticker_inserter: &mut Inserter<BybitTicker>,
        cross_sequence: u64,
    ) -> Result<()> {
        let to_dec = |opt: Option<String>| -> Decimal128 {
            opt.and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Decimal128::from_str(&s).ok()
                }
            })
            .unwrap_or_default()
        };

        let tick = Self {
            server_timestamp: server_timestamp,
            received_timestamp: received_timestamp,
            cross_sequence: cross_sequence,
            symbol: ticker_data.symbol,
            tick_direction: ticker_data.tick_direction.unwrap_or_default(),
            price_24h_pcnt: to_dec(ticker_data.price_24h_pcnt),
            last_price: to_dec(ticker_data.last_price),
            prev_price_24h: to_dec(ticker_data.prev_price_24h),
            high_price_24h: to_dec(ticker_data.high_price_24h),
            low_price_24h: to_dec(ticker_data.low_price_24h),
            prev_price_1h: to_dec(ticker_data.prev_price_1h),
            mark_price: to_dec(ticker_data.mark_price),
            index_price: to_dec(ticker_data.index_price),
            open_interest: to_dec(ticker_data.open_interest),
            open_interest_value: to_dec(ticker_data.open_interest_value),
            turnover_24h: to_dec(ticker_data.turnover_24h),
            volume_24h: to_dec(ticker_data.volume_24h),
            next_funding_time: ticker_data
                .next_funding_time
                .and_then(|s| s.parse::<i128>().ok())
                .and_then(|ms| OffsetDateTime::from_unix_timestamp_nanos(ms * 1_000_000).ok())
                .unwrap_or(server_timestamp),
            funding_rate: to_dec(ticker_data.funding_rate),
            bid1_price: to_dec(ticker_data.bid1_price),
            bid1_size: to_dec(ticker_data.bid1_size),
            ask1_price: to_dec(ticker_data.ask1_price),
            ask1_size: to_dec(ticker_data.ask1_size),
            delivery_time: ticker_data.delivery_time.and_then(|s| {
                OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339).ok()
            }),
            basis_rate: ticker_data
                .basis_rate
                .and_then(|s| Decimal128::from_str(&s).ok()),
            delivery_fee_rate: ticker_data
                .delivery_fee_rate
                .and_then(|s| s.parse::<i64>().ok()),
            predicted_delivery_price: ticker_data
                .predicted_delivery_price
                .and_then(|s| Decimal128::from_str(&s).ok()),
            pre_open_price: ticker_data
                .pre_open_price
                .and_then(|s| Decimal128::from_str(&s).ok()),
            pre_qty: ticker_data
                .pre_qty
                .and_then(|s| Decimal128::from_str(&s).ok()),
            cur_pre_listing_phase: ticker_data.cur_pre_listing_phase,
            funding_interval_hour: ticker_data.funding_interval_hour,
            funding_cap: ticker_data
                .funding_cap
                .and_then(|s| Decimal128::from_str(&s).ok()),
            basis_rate_year: ticker_data
                .basis_rate_year
                .and_then(|s| Decimal128::from_str(&s).ok()),
        };
        ticker_inserter.write(&tick).await?;
        let stats = ticker_inserter.commit().await?;
        if stats.rows > 0 {
            println!(
                "{} bytes, {} rows, {} transactions have been inserted in tickers",
                stats.bytes, stats.rows, stats.transactions,
            );
        }

        Ok(())
    }
}

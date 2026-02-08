use crate::parser::Decimal128;
use anyhow::{Context, Result};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use time::OffsetDateTime;

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct TickerCache {
    pub ticker: HashMap<String, BybitTicker>,
}

impl TickerCache {
    pub fn new() -> Self {
        Self {
            ticker: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BybitTickerData {
    pub symbol: String,
    pub tick_direction: Option<String>,
    pub price24h_pcnt: Option<String>,
    pub last_price: Option<String>,
    pub prev_price24h: Option<String>,
    pub high_price24h: Option<String>,
    pub low_price24h: Option<String>,
    pub prev_price1h: Option<String>,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
    pub open_interest: Option<String>,
    pub open_interest_value: Option<String>,
    pub turnover24h: Option<String>,
    pub volume24h: Option<String>,
    pub next_funding_time: Option<String>,
    pub funding_rate: Option<String>,
    pub bid1_price: Option<String>,
    pub bid1_size: Option<String>,
    pub ask1_price: Option<String>,
    pub ask1_size: Option<String>,
    pub delivery_time: Option<String>,
    pub basis_rate: Option<String>,
    pub delivery_fee_rate: Option<String>,
    pub predicted_delivery_price: Option<String>,
    pub pre_open_price: Option<String>,
    pub pre_qty: Option<String>,
    pub cur_pre_listing_phase: Option<String>,
    pub funding_interval_hour: Option<String>,
    pub funding_cap: Option<String>,
    pub basis_rate_year: Option<String>,
}

#[derive(Clone, PartialEq, Row, Serialize, Deserialize, Debug)]
pub struct BybitTicker {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub server_timestamp: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub received_timestamp: OffsetDateTime,
    pub cross_sequence: u64,
    pub symbol: String,
    pub tick_direction: String,
    pub price_24h_pcnt: Decimal128,
    pub last_price: Decimal128,
    pub prev_price_24h: Decimal128,
    pub high_price_24h: Decimal128,
    pub low_price_24h: Decimal128,
    pub prev_price_1h: Decimal128,
    pub mark_price: Decimal128,
    pub index_price: Decimal128,
    pub open_interest: Decimal128,
    pub open_interest_value: Decimal128,
    pub turnover_24h: Decimal128,
    pub volume_24h: Decimal128,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub next_funding_time: OffsetDateTime,
    pub funding_rate: Decimal128,
    pub bid1_price: Decimal128,
    pub bid1_size: Decimal128,
    pub ask1_price: Decimal128,
    pub ask1_size: Decimal128,
    pub delivery_time: Option<OffsetDateTime>,
    pub basis_rate: Option<Decimal128>,
    pub delivery_fee_rate: Option<i64>,
    pub predicted_delivery_price: Option<Decimal128>,
    pub pre_open_price: Option<Decimal128>,
    pub pre_qty: Option<Decimal128>,
    pub cur_pre_listing_phase: Option<String>,
    pub funding_interval_hour: Option<String>,
    pub funding_cap: Option<Decimal128>,
    pub basis_rate_year: Option<Decimal128>,
}

impl BybitTicker {
    fn update_dec(field: &mut Decimal128, opt: Option<String>) {
        if let Some(s) = opt
            && !s.is_empty()
                && let Ok(val) = Decimal128::from_str(&s) {
                    *field = val;
                }
    }

    fn update_opt_dec(field: &mut Option<Decimal128>, opt: Option<String>) {
        if let Some(s) = opt
            && !s.is_empty()
                && let Ok(val) = Decimal128::from_str(&s) {
                    *field = Some(val);
                }
    }

    pub fn apply_delta(&mut self, delta: BybitTickerData) {
        if let Some(v) = delta.tick_direction {
            self.tick_direction = v;
        }

        Self::update_dec(&mut self.price_24h_pcnt, delta.price24h_pcnt);
        Self::update_dec(&mut self.last_price, delta.last_price);
        Self::update_dec(&mut self.prev_price_24h, delta.prev_price24h);
        Self::update_dec(&mut self.high_price_24h, delta.high_price24h);
        Self::update_dec(&mut self.low_price_24h, delta.low_price24h);
        Self::update_dec(&mut self.prev_price_1h, delta.prev_price1h);
        Self::update_dec(&mut self.mark_price, delta.mark_price);
        Self::update_dec(&mut self.index_price, delta.index_price);
        Self::update_dec(&mut self.open_interest, delta.open_interest);
        Self::update_dec(&mut self.open_interest_value, delta.open_interest_value);
        Self::update_dec(&mut self.turnover_24h, delta.turnover24h);
        Self::update_dec(&mut self.volume_24h, delta.volume24h);
        Self::update_dec(&mut self.funding_rate, delta.funding_rate);
        Self::update_dec(&mut self.bid1_price, delta.bid1_price);
        Self::update_dec(&mut self.bid1_size, delta.bid1_size);
        Self::update_dec(&mut self.ask1_price, delta.ask1_price);
        Self::update_dec(&mut self.ask1_size, delta.ask1_size);

        Self::update_opt_dec(&mut self.basis_rate, delta.basis_rate);
        Self::update_opt_dec(
            &mut self.predicted_delivery_price,
            delta.predicted_delivery_price,
        );
        Self::update_opt_dec(&mut self.pre_open_price, delta.pre_open_price);
        Self::update_opt_dec(&mut self.pre_qty, delta.pre_qty);
        Self::update_opt_dec(&mut self.funding_cap, delta.funding_cap);
        Self::update_opt_dec(&mut self.basis_rate_year, delta.basis_rate_year);

        if let Some(v) = delta.cur_pre_listing_phase {
            self.cur_pre_listing_phase = Some(v);
        }
        if let Some(v) = delta.funding_interval_hour {
            self.funding_interval_hour = Some(v);
        }

        if let Some(s) = delta.next_funding_time
            && let Ok(ms) = s.parse::<i128>()
                && let Ok(ts) = OffsetDateTime::from_unix_timestamp_nanos(ms * 1_000_000) {
                    self.next_funding_time = ts;
                }

        if let Some(s) = delta.delivery_fee_rate
            && let Ok(v) = s.parse::<i64>() {
                self.delivery_fee_rate = Some(v);
            }

        if let Some(s) = delta.delivery_time
            && let Ok(ts) =
                OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
            {
                self.delivery_time = Some(ts);
            }
    }

    pub async fn parse_bybit_ticker(
        server_timestamp: OffsetDateTime,
        received_timestamp: OffsetDateTime,
        ticker_data: BybitTickerData,
        cross_sequence: u64,
        ttype: &str,
        ticker_cache: &mut TickerCache,
    ) -> Result<BybitTicker> {
        let symbol = ticker_data.symbol.clone();

        match ttype.to_lowercase().as_str() {
            "snapshot" => {
                let mut tick = Self::empty_with_meta(
                    server_timestamp,
                    received_timestamp,
                    cross_sequence,
                    symbol.clone(),
                );
                tick.apply_delta(ticker_data);
                ticker_cache.ticker.insert(symbol, tick.clone());
                Ok(tick)
            }
            "delta" => {
                let cached_tick = ticker_cache
                    .ticker
                    .get_mut(&symbol)
                    .with_context(|| format!("Missing cache for {}", symbol))?;

                cached_tick.server_timestamp = server_timestamp;
                cached_tick.received_timestamp = received_timestamp;
                cached_tick.cross_sequence = cross_sequence;
                cached_tick.apply_delta(ticker_data);

                Ok(cached_tick.clone())
            }
            _ => anyhow::bail!("Unknown type: {}", ttype),
        }
    }

    fn empty_with_meta(st: OffsetDateTime, rt: OffsetDateTime, cs: u64, sym: String) -> Self {
        Self {
            server_timestamp: st,
            received_timestamp: rt,
            cross_sequence: cs,
            symbol: sym,
            tick_direction: String::new(),
            price_24h_pcnt: Decimal128::default(),
            last_price: Decimal128::default(),
            prev_price_24h: Decimal128::default(),
            high_price_24h: Decimal128::default(),
            low_price_24h: Decimal128::default(),
            prev_price_1h: Decimal128::default(),
            mark_price: Decimal128::default(),
            index_price: Decimal128::default(),
            open_interest: Decimal128::default(),
            open_interest_value: Decimal128::default(),
            turnover_24h: Decimal128::default(),
            volume_24h: Decimal128::default(),
            next_funding_time: st,
            funding_rate: Decimal128::default(),
            bid1_price: Decimal128::default(),
            bid1_size: Decimal128::default(),
            ask1_price: Decimal128::default(),
            ask1_size: Decimal128::default(),
            delivery_time: None,
            basis_rate: None,
            delivery_fee_rate: None,
            predicted_delivery_price: None,
            pre_open_price: None,
            pre_qty: None,
            cur_pre_listing_phase: None,
            funding_interval_hour: None,
            funding_cap: None,
            basis_rate_year: None,
        }
    }
}

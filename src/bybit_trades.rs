use crate::Decimal128;
use anyhow::{Context, Result};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
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
    // pub async fn fetch_trades_bybit(
    //     mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    //     client: Client,
    // ) -> Result<()> {
    //     let mut first_message_skipped = false;
    //     // 3. send subscription message
    //     let sub = serde_json::json!({
    //         "op": "subscribe",
    //         "args": ["publicTrade.BTCUSDT" ]
    //     });
    //     ws.send(Message::Text(sub.to_string().into())).await?;
    //     let mut parsed_trades: Vec<Trades> = vec![];
    //     // 4. read whatever arrives
    //     while let Some(msg) = ws.next().await {
    //         if !first_message_skipped {
    //             first_message_skipped = true;
    //             continue;
    //         }
    //         match msg? {
    //             Message::Text(message) => {
    //                 println!("{:?}", &message);
    //                 let parsed_message: Bybit = serde_json::from_str(&message)?;
    //                 println!("{:?}", &parsed_message);
    //                 println!("{:?}", parsed_trades.len());
    //                 let trades = Trades::parse_bybit_data(parsed_message);
    //                 parsed_trades.extend(trades);
    //                 if parsed_trades.len() >= 100 {
    //                     let mut inserter = client.insert::<Trades>("trades_raw_ml").await?;
    //                     for trade in &parsed_trades {
    //                         inserter.write(trade).await?;
    //                     }
    //                     inserter.end().await?;
    //                     parsed_trades.clear();
    //                 }
    //             }
    //             Message::Ping(d) => ws.send(Message::Pong(d)).await?,
    //             _ => {}
    //         }
    //     }
    //     Ok(())
    // }

    pub fn parse_bybit_trade(
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

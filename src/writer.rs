use crate::bybit_orderbook::BybitOrderbook;
use crate::bybit_ticker::BybitTicker;
use crate::bybit_trades::BybitTrades;
use crate::parser::BybitOTT;
use anyhow::Result;
use clickhouse::{self, inserter::Inserter};
use tokio::sync::mpsc::Receiver;
use tracing::info;

pub async fn async_write(
    mut writer_rx: Receiver<BybitOTT>,
    mut orderbook_inserter: Inserter<BybitOrderbook>,
    mut trades_inserter: Inserter<BybitTrades>,
    mut ticker_inserter: Inserter<BybitTicker>,
) -> Result<()> {
    info!("Writer task started.");

    while let Some(to_insert) = writer_rx.recv().await {
        match to_insert {
            BybitOTT::BybitTicker(ticker) => {
                ticker_inserter.write(&ticker).await?;
                let stats = ticker_inserter.commit().await?;
                if stats.rows > 0 {
                    info!(
                        target_db = "ticker_raw_ml",
                        rows = stats.rows,
                        "Data committed:"
                    );
                }
            }
            BybitOTT::BybitOrderbook(orderbook) => {
                for order in orderbook {
                    orderbook_inserter.write(&order).await?;
                }
                let stats = orderbook_inserter.commit().await?;
                if stats.rows > 0 {
                    info!(
                        target_db = "orderbook_raw_ml",
                        rows = stats.rows,
                        "Data committed:"
                    );
                }
            }
            BybitOTT::BybitTrades(trades) => {
                for trade in trades {
                    trades_inserter.write(&trade).await?;
                }
                let stats = trades_inserter.commit().await?;
                if stats.rows > 0 {
                    info!(
                        target_db = "trades_raw_ml",
                        rows = stats.rows,
                        "Data committed:"
                    );
                }
            }
        }
    }
    Ok(())
}

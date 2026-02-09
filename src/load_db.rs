use anyhow::Result;
use clickhouse::Client;
use tracing::info;

pub async fn load_db() -> Result<Client> {
    use clickhouse::Client;
    let password = "yourpassword";
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_user("default")
        .with_password(password.to_string())
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");

    info!("DB loaded.");
    client
        .query(
            r#"
        CREATE TABLE IF NOT EXISTS trades_raw_ml
        (
            server_timestamp       DateTime64(3, 'UTC'),
            received_timestamp       DateTime64(3, 'UTC'),
            trade_timestamp       DateTime64(3, 'UTC'),
            symbol          LowCardinality(String),
            trade_id        LowCardinality(String),
            side            LowCardinality(String),
            price           Decimal128(18),
            volume             Decimal128(18),
            tick_direction  LowCardinality(String),
            is_block_trade  Bool,
            is_rpi          Bool,
            seq             UInt64,
            exchange        LowCardinality(String) DEFAULT 'bybit'
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(trade_timestamp)
        ORDER BY (symbol, trade_timestamp, seq)
        SETTINGS index_granularity = 8192
        "#,
        )
        .execute()
        .await?;
    client
        .query(
            r#"
        CREATE TABLE IF NOT EXISTS orderbook_raw_ml
        (
            server_timestamp       DateTime64(3, 'UTC'),
            received_timestamp       DateTime64(3, 'UTC'),
            client_timestamp       DateTime64(3, 'UTC'),
            symbol          LowCardinality(String),
            side            LowCardinality(String),
            price           Decimal128(18),
            volume             Decimal128(18),
            update             UInt64,
            exchange        LowCardinality(String) DEFAULT 'bybit'
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(server_timestamp)
        ORDER BY (symbol, server_timestamp, update, side, price)
        SETTINGS index_granularity = 8192
        "#,
        )
        .execute()
        .await?;

    client
        .query(
            r#"
        CREATE TABLE IF NOT EXISTS ticker_raw_ml
        (
            server_timestamp       DateTime64(3, 'UTC'),
            received_timestamp       DateTime64(3, 'UTC'),
            cross_sequence       UInt64,
            symbol          LowCardinality(String),
            tick_direction            LowCardinality(String),
            price_24h_pcnt           Decimal128(18),
            last_price             Decimal128(18),
            prev_price_24h             Decimal128(18),
            high_price_24h             Decimal128(18),
            low_price_24h              Decimal128(18),
            prev_price_1h             Decimal128(18),
            mark_price                Decimal128(18),

    index_price Decimal128(18),
    open_interest Decimal128(18),
    open_interest_value Decimal128(18),
    turnover_24h Decimal128(18),
    volume_24h Decimal128(18),
    next_funding_time DateTime64(3, 'UTC'),
    funding_rate Decimal128(18),
    bid1_price Decimal128(18),
    bid1_size Decimal128(18),
    ask1_price Decimal128(18),
    ask1_size Decimal128(18),
    delivery_time Nullable(DateTime64(3, 'UTC')),
    basis_rate Nullable(Decimal128(18)),
    delivery_fee_rate Nullable(UInt64),
    predicted_delivery_price Nullable(Decimal128(18)),
    pre_open_price Nullable(Decimal128(18)),
    pre_qty Nullable(Decimal128(18)),
    cur_pre_listing_phase Nullable(String),
    funding_interval_hour Nullable(String),
    funding_cap Nullable(Decimal128(18)),
    basis_rate_year Nullable(Decimal128(18)),

    exchange        LowCardinality(String) DEFAULT 'bybit',

        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(server_timestamp)
        ORDER BY (symbol, server_timestamp, cross_sequence)
        SETTINGS index_granularity = 8192
        "#,
        )
        .execute()
        .await?;

    info!("Table created or existed.");
    Ok(client)
}

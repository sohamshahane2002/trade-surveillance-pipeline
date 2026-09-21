with trades as (
    select * from {{ ref('stg_trades') }}
),
trader_stats as (
    select
        trader_id,
        trader_desk,
        count(trade_id)             as total_trades,
        sum(quantity)               as total_quantity,
        avg(quantity)               as avg_quantity,
        max(quantity)               as max_quantity,
        sum(trade_value)            as total_trade_value,
        avg(trade_value)            as avg_trade_value,
        count(case when is_suspicious = true then 1 end) as suspicious_trade_count,
        count(case when suspicious_type = 'WASH_TRADE' then 1 end) as wash_trade_count,
        count(case when suspicious_type = 'SPOOFING' then 1 end) as spoofing_count,
        round(
            count(case when is_suspicious = true then 1 end) * 100.0
            / count(trade_id), 2
        ) as suspicious_rate_pct,
        min(ingestion_timestamp) as first_trade_at,
        max(ingestion_timestamp) as last_trade_at
    from trades
    group by trader_id, trader_desk
)
select * from trader_stats
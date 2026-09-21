with flagged as (
    select * from flagged_trades
),
enriched as (
    select
        trade_id,
        trader_id,
        stock_symbol,
        order_type,
        quantity,
        price,
        trade_value,
        suspicious_type,
        risk_score,
        volume_spike_ratio,
        price_deviation_pct,
        rule_volume_spike,
        rule_spoofing,
        rule_price_deviation,
        case
            when risk_score >= 3 then 'HIGH'
            when risk_score = 2  then 'MEDIUM'
            when risk_score = 1  then 'LOW'
            else 'CLEAN'
        end as risk_level
        
    from flagged
)
select * from enriched
order by risk_score desc
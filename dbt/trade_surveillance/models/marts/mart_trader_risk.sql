with trader_features as (
    select * from {{ ref('int_trader_features') }}
),
risk_scored as (
    select
        trader_id,
        trader_desk,
        total_trades,
        total_quantity,
        avg_quantity,
        max_quantity,
        total_trade_value,
        suspicious_trade_count,
        wash_trade_count,
        spoofing_count,
        suspicious_rate_pct,
        first_trade_at,
        last_trade_at,
        case
            when suspicious_rate_pct >= 20 then 'HIGH RISK'
            when suspicious_rate_pct >= 10 then 'MEDIUM RISK'
            when suspicious_rate_pct >= 5  then 'LOW RISK'
            else 'CLEAN'
        end as trader_risk_label
    from trader_features
)
select * from risk_scored
order by suspicious_rate_pct desc
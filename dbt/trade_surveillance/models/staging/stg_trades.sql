with source as (
    select * from raw_trades
),
staged as (
    select
        trade_id,
        trader_id,
        trader_desk,
        upper(stock_symbol)     as stock_symbol,
        upper(exchange)         as exchange,
        upper(order_type)       as order_type,
        quantity,
        price,
        trade_value,
        is_suspicious,
        coalesce(suspicious_type, 'NORMAL') as suspicious_type,
        ingestion_timestamp,
        case
            when order_type = 'BUY'  then quantity * price
            when order_type = 'SELL' then quantity * price * -1
        end as signed_trade_value
    from source
    where trade_id is not null
        and trader_id is not null
        and quantity > 0
        and price > 0
)
select * from staged
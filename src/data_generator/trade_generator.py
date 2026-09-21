import random
import uuid
from datetime import datetime
from faker import Faker
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

# ── Constants ────────────────────────────────────────────
STOCKS = [
    "RELIANCE", "TCS", "INFY", "HDFC", "ICICI",
    "WIPRO", "HCLTECH", "AXISBANK", "SBIN", "BAJFINANCE",
    "MARUTI", "TATAMOTORS", "SUNPHARMA", "DRREDDY", "CIPLA",
    "ONGC", "NTPC", "POWERGRID", "COALINDIA", "TECHM"
]

EXCHANGES = ["NSE", "BSE"]

ORDER_TYPES = ["BUY", "SELL"]

TRADER_IDS = [f"TRD_{str(i).zfill(3)}" for i in range(1, int(os.getenv("NUM_TRADERS", 50)) + 1)]

DESKS = ["Equities", "Derivatives", "Fixed Income", "FX", "Commodities"]


# ── Trade Generator ───────────────────────────────────────
def generate_normal_trade():
    """Generate a completely normal legitimate trade"""

    stock = random.choice(STOCKS)
    base_price = round(random.uniform(100, 5000), 2)

    return {
        "trade_id": f"TRD-{uuid.uuid4().hex[:12].upper()}",
        "trader_id": random.choice(TRADER_IDS),
        "trader_desk": random.choice(DESKS),
        "stock_symbol": stock,
        "exchange": random.choice(EXCHANGES),
        "order_type": random.choice(ORDER_TYPES),
        "quantity": random.randint(10, 1000),
        "price": base_price,
        "trade_value": 0,  # calculated below
        "timestamp": datetime.now().isoformat(),
        "is_suspicious": False,
        "suspicious_type": None
    }


def generate_wash_trade():
    """
    Wash Trade — same trader buys and sells same stock
    to create fake volume
    """
    stock = random.choice(STOCKS)
    trader = random.choice(TRADER_IDS)
    price = round(random.uniform(100, 5000), 2)
    quantity = random.randint(5000, 20000)  # unusually large

    trade1 = {
        "trade_id": f"TRD-{uuid.uuid4().hex[:12].upper()}",
        "trader_id": trader,
        "trader_desk": random.choice(DESKS),
        "stock_symbol": stock,
        "exchange": "NSE",
        "order_type": "BUY",
        "quantity": quantity,
        "price": price,
        "trade_value": 0,
        "timestamp": datetime.now().isoformat(),
        "is_suspicious": True,
        "suspicious_type": "WASH_TRADE"
    }

    trade2 = {
        "trade_id": f"TRD-{uuid.uuid4().hex[:12].upper()}",
        "trader_id": trader,  # same trader
        "trader_desk": trade1["trader_desk"],
        "stock_symbol": stock,  # same stock
        "exchange": "NSE",
        "order_type": "SELL",  # opposite side
        "quantity": quantity,  # same quantity
        "price": price,        # same price
        "trade_value": 0,
        "timestamp": datetime.now().isoformat(),
        "is_suspicious": True,
        "suspicious_type": "WASH_TRADE"
    }

    return [trade1, trade2]


def generate_spoofing_trade():
    """
    Spoofing — large order placed then immediately
    followed by tiny real trade (pattern indicator)
    """
    stock = random.choice(STOCKS)
    trader = random.choice(TRADER_IDS)
    price = round(random.uniform(100, 5000), 2)

    # Large fake order
    spoof_trade = {
        "trade_id": f"TRD-{uuid.uuid4().hex[:12].upper()}",
        "trader_id": trader,
        "trader_desk": random.choice(DESKS),
        "stock_symbol": stock,
        "exchange": random.choice(EXCHANGES),
        "order_type": "SELL",
        "quantity": random.randint(50000, 200000),  # massive quantity
        "price": price,
        "trade_value": 0,
        "timestamp": datetime.now().isoformat(),
        "is_suspicious": True,
        "suspicious_type": "SPOOFING"
    }

    return spoof_trade


def calculate_trade_value(trade):
    """Calculate trade value and return updated trade"""
    trade["trade_value"] = round(trade["quantity"] * trade["price"], 2)
    return trade


def generate_trade_batch(batch_size=10):
    """
    Generate a batch of trades — mix of normal and suspicious
    roughly 90% normal, 5% wash trades, 5% spoofing
    """
    trades = []

    for _ in range(batch_size):
        rand = random.random()

        if rand < 0.90:
            # 90% normal trades
            trade = generate_normal_trade()
            trade = calculate_trade_value(trade)
            trades.append(trade)

        elif rand < 0.95:
            # 5% wash trades (comes as pair)
            wash_trades = generate_wash_trade()
            for t in wash_trades:
                t = calculate_trade_value(t)
                trades.append(t)

        else:
            # 5% spoofing
            trade = generate_spoofing_trade()
            trade = calculate_trade_value(trade)
            trades.append(trade)

    return trades


# ── Main (test run) ───────────────────────────────────────
if __name__ == "__main__":
    logger.add(os.getenv("LOG_PATH", "logs/pipeline.log"), rotation="10 MB")

    logger.info("Starting trade generation test...")

    batch = generate_trade_batch(batch_size=10)

    logger.info(f"Generated {len(batch)} trades")

    for trade in batch:
        status = "🚨 SUSPICIOUS" if trade["is_suspicious"] else "✅ Normal"
        logger.info(
            f"{status} | {trade['suspicious_type'] or 'NORMAL':<12} | "
            f"{trade['trader_id']} | {trade['stock_symbol']:<12} | "
            f"{trade['order_type']} | Qty: {trade['quantity']:>8,} | "
            f"Price: ₹{trade['price']:>8,.2f} | "
            f"Value: ₹{trade['trade_value']:>15,.2f}"
        )

    suspicious_count = sum(1 for t in batch if t["is_suspicious"])
    logger.info(f"Summary — Total: {len(batch)} | Suspicious: {suspicious_count} | Normal: {len(batch) - suspicious_count}")
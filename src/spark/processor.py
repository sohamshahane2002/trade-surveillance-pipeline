import os
#import sys

# Force PySpark to use venv installation
#os.environ["SPARK_HOME"] = r"C:\SOHAM\Trade_Surveillance_Project\trade-surveillance\venv\Lib\site-packages\pyspark"
#os.environ["PYSPARK_PYTHON"] = sys.executable
#os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "trades")
DUCKDB_PATH             = os.getenv("DUCKDB_PATH", "data/trades.db")


# ── Schema ────────────────────────────────────────────────
# Defines the exact structure of our trade data
# Like creating a table definition in SQL
TRADE_SCHEMA = StructType([
    StructField("trade_id",       StringType(),  True),
    StructField("trader_id",      StringType(),  True),
    StructField("trader_desk",    StringType(),  True),
    StructField("stock_symbol",   StringType(),  True),
    StructField("exchange",       StringType(),  True),
    StructField("order_type",     StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("price",          DoubleType(),  True),
    StructField("trade_value",    DoubleType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("is_suspicious",  BooleanType(), True),
    StructField("suspicious_type",StringType(),  True),
])


# ── Spark Session ─────────────────────────────────────────
def create_spark_session():
    """
    Create and return a SparkSession
    SparkSession is the entry point to all PySpark functionality
    Think of it like a database connection but for Spark
    """
    spark = SparkSession.builder \
        .appName("TradeSurveillance") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") \
        .getOrCreate()

    # Reduce Spark's very verbose logging to only show errors
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("SparkSession created successfully")
    return spark


# ── Step 1: Read From Kafka ───────────────────────────────    

def read_from_kafka(spark):
    """
    Read streaming data from Kafka topic
    Returns a streaming DataFrame — think of it as a
    table that keeps growing as new trades arrive
    """
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC}")
    return df    


# ── Step 2: Parse Raw Kafka Messages ─────────────────────
def parse_kafka_messages(df):
    """
    Kafka gives us raw bytes — we need to convert to proper columns

    Raw Kafka message looks like:
    value = b'{"trade_id": "TRD-123", "trader_id": "TRD_047", ...}'

    After parsing we get proper columns:
    trade_id | trader_id | stock_symbol | quantity | price | ...
    """

    # Step 2a: Extract the value column and convert bytes to string
    # Kafka message has many columns (key, value, topic, partition, offset, timestamp)
    # We only need 'value' which contains our trade JSON
    df = df.selectExpr("CAST(value AS STRING) as json_string")

    # Step 2b: Parse JSON string into proper columns using our schema
    # from_json converts the JSON string into a struct (nested object)
    # then we use * to expand all fields into separate columns
    df = df.select(
        F.from_json(F.col("json_string"), TRADE_SCHEMA).alias("trade")
    ).select("trade.*")

    logger.info("Kafka messages parsed into DataFrame")
    return df


# ── Step 3: Clean Data ────────────────────────────────────
def clean_data(df):
    """
    Clean the raw trade data:
    1. Remove rows with missing critical fields
    2. Fix data types
    3. Standardize text fields
    """

    # Remove rows where critical fields are null
    # A trade without these fields is useless for analysis
    df = df.filter(
        F.col("trade_id").isNotNull() &
        F.col("trader_id").isNotNull() &
        F.col("stock_symbol").isNotNull() &
        F.col("quantity").isNotNull() &
        F.col("price").isNotNull()
    )

    # Remove trades with impossible values
    # Quantity and price must be positive numbers
    df = df.filter(
        (F.col("quantity") > 0) &
        (F.col("price") > 0)
    )

    # Convert timestamp string to proper timestamp type
    df = df.withColumn(
        "trade_timestamp",
        F.to_timestamp(F.col("timestamp"))
    )

    # Standardize text — uppercase all stock symbols and order types
    # So "reliance" and "RELIANCE" are treated the same
    df = df.withColumn("stock_symbol", F.upper(F.col("stock_symbol")))
    df = df.withColumn("order_type",   F.upper(F.col("order_type")))

    # Add ingestion timestamp — when did OUR system receive this trade
    # Different from trade_timestamp (when trade actually happened)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    logger.info("Data cleaning completed")
    return df


# ── Step 4: Feature Engineering ──────────────────────────
def engineer_features(df):
    """
    Add basic features that work on streaming DataFrames
    Complex window features calculated in write_to_duckdb using Pandas
    """

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    # Basic trade value recalculation (verify)
    df = df.withColumn(
        "trade_value_calc",
        F.col("quantity") * F.col("price")
    )

    logger.info("Feature engineering completed")
    return df

# ── Step 5: Apply Detection Rules ────────────────────────
def apply_detection_rules(df):
    """
    Detection rules moved to write_to_duckdb (Pandas)
    since streaming DataFrames don't support complex window functions
    Just pass through here
    """
    logger.info("Detection rules will be applied in Pandas during batch write")
    return df


# ── Step 6: Write to DuckDB ───────────────────────────────
def write_to_duckdb(batch_df, batch_id):
    """
    This function runs every micro-batch (every 30 seconds)
    batch_df = the DataFrame of new trades in this batch
    batch_id = auto incrementing batch number

    We write to 3 tables in DuckDB:
    1. raw_trades      → everything as received
    2. cleaned_trades  → after cleaning and features
    3. flagged_trades  → only suspicious ones
    """

    # Convert Spark DataFrame to Pandas for DuckDB writing
    # DuckDB works natively with Pandas DataFrames
    pandas_df = batch_df.toPandas()

    if pandas_df.empty:
        logger.info(f"Batch {batch_id}: No trades to write")
        return

    # ── Pandas Feature Engineering ─────────────────────────
    # Calculate per trader average quantity
    trader_avg = pandas_df.groupby("trader_id")["quantity"].transform("mean")
    pandas_df["avg_trader_quantity"] = trader_avg

    # Volume spike ratio
    pandas_df["volume_spike_ratio"] = (
        pandas_df["quantity"] / pandas_df["avg_trader_quantity"]
    )

    # Per stock average price
    stock_avg = pandas_df.groupby("stock_symbol")["price"].transform("mean")
    pandas_df["avg_stock_price"] = stock_avg

    # Price deviation percentage
    pandas_df["price_deviation_pct"] = abs(
        (pandas_df["price"] - pandas_df["avg_stock_price"]) /
        pandas_df["avg_stock_price"] * 100
    )

    # Trade count per trader in this batch
    trader_count = pandas_df.groupby("trader_id")["trade_id"].transform("count")
    pandas_df["trader_trade_count"] = trader_count

    # ── Detection Rules ─────────────────────────────────────
    pandas_df["rule_volume_spike"] = pandas_df["volume_spike_ratio"] >= 5.0
    pandas_df["rule_spoofing"] = pandas_df["quantity"] >= 50000
    pandas_df["rule_price_deviation"] = pandas_df["price_deviation_pct"] >= 20.0
    pandas_df["rule_generator_flag"] = pandas_df["is_suspicious"]

    pandas_df["risk_score"] = (
        pandas_df["rule_volume_spike"].astype(int) +
        pandas_df["rule_spoofing"].astype(int) +
        pandas_df["rule_price_deviation"].astype(int) +
        pandas_df["rule_generator_flag"].astype(int)
    )

    pandas_df["is_flagged"] = pandas_df["risk_score"] >= 1

    # Connect to DuckDB
    conn = duckdb.connect(DUCKDB_PATH)

    try:
        # Table 1: Raw trades — store everything
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_trades (
                trade_id          VARCHAR,
                trader_id         VARCHAR,
                trader_desk       VARCHAR,
                stock_symbol      VARCHAR,
                exchange          VARCHAR,
                order_type        VARCHAR,
                quantity          INTEGER,
                price             DOUBLE,
                trade_value       DOUBLE,
                timestamp         VARCHAR,
                is_suspicious     BOOLEAN,
                suspicious_type   VARCHAR,
                ingestion_timestamp TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT INTO raw_trades
            SELECT
                trade_id, trader_id, trader_desk, stock_symbol,
                exchange, order_type, quantity, price, trade_value,
                timestamp, is_suspicious, suspicious_type,
                ingestion_timestamp
            FROM pandas_df
        """)

        # Table 2: Cleaned trades with features
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cleaned_trades (
                trade_id              VARCHAR,
                trader_id             VARCHAR,
                trader_desk           VARCHAR,
                stock_symbol          VARCHAR,
                exchange              VARCHAR,
                order_type            VARCHAR,
                quantity              INTEGER,
                price                 DOUBLE,
                trade_value           DOUBLE,
                trade_timestamp       TIMESTAMP,
                ingestion_timestamp   TIMESTAMP,
                avg_trader_quantity   DOUBLE,
                volume_spike_ratio    DOUBLE,
                avg_stock_price       DOUBLE,
                price_deviation_pct   DOUBLE,
                trader_trade_count    BIGINT,
                risk_score            INTEGER,
                is_flagged            BOOLEAN
            )
        """)

        conn.execute("""
            INSERT INTO cleaned_trades
            SELECT
                trade_id, trader_id, trader_desk, stock_symbol,
                exchange, order_type, quantity, price, trade_value,
                trade_timestamp, ingestion_timestamp,
                avg_trader_quantity, volume_spike_ratio,
                avg_stock_price, price_deviation_pct,
                trader_trade_count, risk_score, is_flagged
            FROM pandas_df
        """)

        # Table 3: Flagged suspicious trades only
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flagged_trades (
                trade_id            VARCHAR,
                trader_id           VARCHAR,
                stock_symbol        VARCHAR,
                order_type          VARCHAR,
                quantity            INTEGER,
                price               DOUBLE,
                trade_value         DOUBLE,
                suspicious_type     VARCHAR,
                risk_score          INTEGER,
                volume_spike_ratio  DOUBLE,
                price_deviation_pct DOUBLE,
                trade_timestamp     TIMESTAMP,
                rule_volume_spike   BOOLEAN,
                rule_spoofing       BOOLEAN,
                rule_price_deviation BOOLEAN
            )
        """)

        conn.execute("""
            INSERT INTO flagged_trades
            SELECT
                trade_id, trader_id, stock_symbol,
                order_type, quantity, price, trade_value,
                suspicious_type, risk_score,
                volume_spike_ratio, price_deviation_pct,
                trade_timestamp,
                rule_volume_spike, rule_spoofing, rule_price_deviation
            FROM pandas_df
            WHERE is_flagged = true
        """)

        # Log what we wrote
        total       = len(pandas_df)
        flagged     = len(pandas_df[pandas_df["is_flagged"] == True])
        clean       = total - flagged

        logger.info(
            f"Batch {batch_id} written to DuckDB | "
            f"Total: {total} | "
            f"Flagged: {flagged} | "
            f"Clean: {clean}"
        )

    finally:
        conn.close()


# ── Main Pipeline ─────────────────────────────────────────
def run_pipeline():
    logger.add(os.getenv("LOG_PATH", "logs/pipeline.log"), rotation="10 MB")
    logger.info("Starting Trade Surveillance Pipeline")

    spark = create_spark_session()
    raw_df = read_from_kafka(spark)
    parsed_df = parse_kafka_messages(raw_df)
    cleaned_df = clean_data(parsed_df)
    featured_df = engineer_features(cleaned_df)
    processed_df = apply_detection_rules(featured_df)

    checkpoint_path = os.path.abspath("data/checkpoints")

    # Change trigger to availableNow=True so Spark reads all current Kafka messages and stops
    query = (
        processed_df.writeStream
        .foreachBatch(write_to_duckdb)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    logger.info("Pipeline processing available trades from Kafka...")

    # Wait for Spark to finish processing the batch and exit cleanly
    query.awaitTermination()
    logger.info("Pipeline finished processing batch successfully.")

    # Explicitly stop Spark session so container/process exits with code 0
    spark.stop()

# ── Entry Point ───────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()
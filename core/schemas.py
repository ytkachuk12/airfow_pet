from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DecimalType, FloatType,
    LongType, BooleanType
)


BITFINEX_SCHEMA = StructType(
    [
        StructField("timestamp", IntegerType(), True),
        StructField("tid", IntegerType(), True),
        StructField("price", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True),
    ]
)


POLONIEX_SCHEMA = StructType(
    [
        StructField("globalTradeID", StringType(), True),
        StructField("tradeID", StringType(), True),
        StructField("date", StringType(), True),
        StructField("type", StringType(), True),
        StructField("rate", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("total", StringType(), True),
        StructField("orderNumber", StringType(), True),
    ]
)

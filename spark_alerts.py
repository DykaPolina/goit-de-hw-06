"""Кроки 2–5. Spark Structured Streaming:
читає building_sensors, рахує середні температуру й вологість у ковзному вікні
(1 хв, крок 30 с, watermark 10 с), зіставляє їх з умовами з alerts_conditions.csv
(cross join + фільтр) і пише алерти в топік alerts.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import ALERTS_TOPIC, SENSORS_TOPIC, spark_kafka_options

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONDITIONS_PATH = os.path.join(BASE_DIR, "alerts_conditions.csv")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "alerts")
NOT_USED = -999  # значення в CSV, що означає «межа не використовується»

spark = (
    SparkSession.builder.appName("polina_dyka_sensor_alerts")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------- 1. Вхідний потік ----------
sensor_schema = StructType([
    StructField("id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType()),
])

sensors = (
    spark.readStream.format("kafka")
    .options(**spark_kafka_options)
    .option("subscribe", SENSORS_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("d"))
    .select(
        "d.id",
        "d.temperature",
        "d.humidity",
        F.to_timestamp("d.timestamp").alias("event_time"),
    )
    .where(F.col("event_time").isNotNull())
)

# ---------- 2. Агрегація: sliding window 1 хв / 30 с, watermark 10 с ----------
averages = (
    sensors.withWatermark("event_time", "10 seconds")
    .groupBy(F.window("event_time", "1 minute", "30 seconds"))
    .agg(
        F.avg("temperature").alias("t_avg"),
        F.avg("humidity").alias("h_avg"),
    )
)

# ---------- 3. Умови алертів з файлу ----------
conditions = spark.read.csv(CONDITIONS_PATH, header=True, inferSchema=True)
print("Alert conditions:")
conditions.show(truncate=False)


def in_range(value_col, min_col, max_col):
    """-999 у min / max означає, що відповідна межа не перевіряється."""
    return (
        ((F.col(min_col) == NOT_USED) | (value_col >= F.col(min_col)))
        & ((F.col(max_col) == NOT_USED) | (value_col <= F.col(max_col)))
    )


# ---------- 4. Визначення алертів: cross join + фільтр ----------
alerts = (
    averages.crossJoin(F.broadcast(conditions))
    .where(
        in_range(F.col("t_avg"), "temperature_min", "temperature_max")
        & in_range(F.col("h_avg"), "humidity_min", "humidity_max")
    )
    .select(
        F.struct(
            F.col("window.start").alias("start"),
            F.col("window.end").alias("end"),
        ).alias("window"),
        "t_avg",
        "h_avg",
        F.col("code").cast("string").alias("code"),
        "message",
        F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp"),
    )
)

# ---------- 5. Запис у Kafka ----------
# Ключ — код алерту + початок вікна, значення — JSON у форматі з прикладу завдання.
# outputMode("append"): вікно потрапляє у вихід один раз — коли watermark пройде його кінець,
# тобто середні вже остаточні й пізні дані (до 10 с) враховано.
to_kafka = alerts.select(
    F.concat_ws("_", F.col("code"), F.col("window.start").cast("string")).alias("key"),
    F.to_json(F.struct("window", "t_avg", "h_avg", "code", "message", "timestamp")).alias("value"),
)

query = (
    to_kafka.writeStream.format("kafka")
    .options(**spark_kafka_options)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

# Дублюємо алерти в консоль, щоб бачити, що саме відправлено
console = (
    alerts.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Reading '{SENSORS_TOPIC}', writing alerts to '{ALERTS_TOPIC}'. Ctrl+C to stop.")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    query.stop()
    console.stop()
    spark.stop()

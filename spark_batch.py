import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
DATASET = os.getenv("BIGQUERY_DATASET", "flight_data")
INPUT_TABLE = os.getenv("INPUT_TABLE", f"{PROJECT_ID}.{DATASET}.flight_events")
OUTPUT_TABLE = os.getenv("OUTPUT_TABLE", f"{PROJECT_ID}.{DATASET}.flight_stats")
TEMP_BUCKET = os.getenv("TEMP_BUCKET", "your-temp-bucket")


def build_spark():
    return SparkSession.builder.appName("FlightDelayStats").getOrCreate()


def main():
    spark = build_spark()
    try:
        print("Reading from BigQuery...")
        df = (
            spark.read.format("bigquery")
            .option("table", INPUT_TABLE)
            .option("project", PROJECT_ID)
            .load()
        )

        print(f"Total records: {df.count()}")
        df.printSchema()

        print("\nComputing average delay per airport...")
        airport_stats = df.groupBy("origin").agg(
            F.count("*").alias("total_flights"),
            F.avg("delay_minutes").alias("avg_delay_minutes"),
            F.max("delay_minutes").alias("max_delay_minutes"),
            F.sum(F.when(F.col("status") == "DELAYED", 1).otherwise(0)).alias("delayed_flights"),
            F.sum(F.when(F.col("status") == "ON_TIME", 1).otherwise(0)).alias("ontime_flights"),
        ).orderBy(F.desc("avg_delay_minutes"))

        airport_stats.show(10)

        print("\nComputing worst routes...")
        route_stats = df.groupBy("origin", "destination").agg(
            F.count("*").alias("total_flights"),
            F.avg("delay_minutes").alias("avg_delay_minutes"),
        ).orderBy(F.desc("avg_delay_minutes"))

        route_stats.show(10)

        print("\nComputing delay by airline...")
        airline_stats = df.groupBy("airline").agg(
            F.count("*").alias("total_flights"),
            F.avg("delay_minutes").alias("avg_delay_minutes"),
            F.sum(F.when(F.col("status") == "DELAYED", 1).otherwise(0)).alias("delayed_flights"),
        ).orderBy(F.desc("avg_delay_minutes"))

        airline_stats.show(10)

        print("\nComputing delay by reason...")
        reason_stats = df.groupBy("delay_reason").agg(
            F.count("*").alias("total_occurrences"),
            F.avg("delay_minutes").alias("avg_delay_minutes"),
        ).orderBy(F.desc("total_occurrences"))

        reason_stats.show(10)

        print("\nWriting stats to BigQuery...")
        (
            airport_stats.write.format("bigquery")
            .option("table", OUTPUT_TABLE)
            .option("project", PROJECT_ID)
            .option("temporaryGcsBucket", TEMP_BUCKET)
            .mode("overwrite")
            .save()
        )

        print(f"Done. Stats written to {OUTPUT_TABLE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

# data-intelligence/etl/main.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType
from pyspark.sql.functions import col, lit, current_timestamp

def main():
    # 1. Initialize Spark Session
    # This is the entry point to any Spark functionality.
    spark = SparkSession.builder \
        .appName("ProjectLeviathanETL_v1") \
        .getOrCreate()

    print("Spark Session created successfully.")

    # 2. Define the Canonical Schema
    # This tells Spark what the structure of our incoming data should be.
    # It's based on the 'security_event_v1.json' schema file we created.
    event_schema = StructType([
        StructField("eventId", StringType(), True),
        StructField("eventTime", TimestampType(), True),
        StructField("sourceSystem", StringType(), True),
        StructField("eventName", StringType(), True),
        StructField("eventSource", MapType(StringType(), StringType()), True),
        StructField("eventTarget", MapType(StringType(), StringType()), True),
        StructField("eventDetails", MapType(StringType(), StringType()), True),
        StructField("severity", StringType(), True),
    ])

    # 3. Read Mock Data into a DataFrame
    # We point Spark to our mock data directory. Spark can read multiple files at once.
    # The 'schema' option enforces our defined structure on the data.
    try:
        input_df = spark.read.option("multiLine", True).schema(event_schema).json("../mock-data/*.json")
        print("Successfully read mock data.")
    except Exception as e:
        print(f"Error reading mock data: {e}")
        spark.stop()
        return

    # 4. Perform a Simple Transformation
    # We add a new column to track when the data was processed by this ETL job.
    # This is a common practice in data engineering.
    transformed_df = input_df.withColumn("ingestionTimestamp", current_timestamp())

    # 5. Show the Result
    # This displays the transformed data in the console, so we can verify our work.
    print("Transformed Data Schema:")
    transformed_df.printSchema()

    print("Writing transformed data to Parquet format...")
    transformed_df.write.mode("overwrite").parquet("../output_data")
    print("Write successful.")

    # 6. Stop the Spark Session
    # It's important to release the resources when the job is done.
    spark.stop()

if __name__ == '__main__':
    main()

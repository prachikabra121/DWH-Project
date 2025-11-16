import logging
import gzip
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, explode, lit, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os

from config import BUCKET_NAME, S3_TDZ_PATH
from utils import write_logs_to_parquet
from file_validation import extract_details_from_filename

import boto3

# Initialize AWS S3 client
s3 = boto3.client("s3")


def decompress_and_chunk(content: bytes, chunk_size: int = 520) -> list:
    """Decompresses gzip content and chunks it into 520-byte chunks."""
    try:
        decompressed_data = gzip.decompress(content)
        return [decompressed_data[i:i + chunk_size].decode('utf-8', errors='ignore')
                for i in range(0, len(decompressed_data), chunk_size)]
    except Exception:
        return []


# ✅ Register UDF
decompress_udf_spark = udf(decompress_and_chunk, ArrayType(StringType()))


def load_schema(schema_file_path: str, spark: SparkSession):
    """Loads schema from an Excel file and broadcasts it for efficiency."""
    logging.info(f"Loading schema from {schema_file_path}.")
    
    schema_df = pd.read_excel(schema_file_path)

    required_columns = {'Column', 'Starting_Index', 'Length'}
    if not required_columns.issubset(schema_df.columns):
        raise ValueError(f"Schema file must contain columns: {required_columns}")

    schema_list = schema_df.rename(columns={
        'Column': 'column_name',
        'Starting_Index': 'start_index',
        'Length': 'length'
    }).to_dict(orient='records')

    schema = StructType([
        StructField(field['column_name'], StringType(), True) for field in schema_list
    ])

    logging.info(f"✅ Schema loaded with {len(schema.fields)} fields.")

    # ✅ Broadcast schema for parallel execution
    schema_broadcast = spark.sparkContext.broadcast(schema_list)

    return schema, schema_broadcast


def process_file(spark: SparkSession, gz_key: str, schema_list):
    """Processes a single .gz file, extracts data, and appends to a partitioned Parquet file."""
    logs_tdz = []
    now = datetime.utcnow()

    logs_tdz.append((datetime.now(), "INFO", f"Processing file: {gz_key}"))

    # Read the binary .gz file
    binary_df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.gz") \
        .load(f"s3a://{BUCKET_NAME}/{gz_key}")

    if binary_df.rdd.isEmpty():
        logs_tdz.append((datetime.now(), "ERROR", f"❌ No content found in {gz_key}"))
        return logs_tdz

    # Decompress and explode into chunks
    df = binary_df.withColumn("chunks", decompress_udf_spark(col("content")))
    df = df.select(explode(col("chunks")).alias("chunk"))

    # ✅ Simplified Partitioning: Use Today's Date and Current Hour
    today_date = datetime.utcnow().strftime("%Y%m%d")  # YYYYMMDD
    current_hour = datetime.utcnow().strftime("%H")  # HH

    # Extract fields based on schema
    for field in schema_list:
        df = df.withColumn(
            field['column_name'],
            substring(col("chunk"), field['start_index'], field['length'])
        )

    # Select required columns
    df = df.select([field['column_name'] for field in schema_list])

    # Add partitioning columns
    df = df.withColumn("date", lit(today_date)) \
           .withColumn("hour", lit(current_hour))

    # Debugging: Check data before writing
    df.show(5, truncate=False)
    row_count = df.count()  # ✅ Store count in a variable
    print(f"✅ Number of rows in the DataFrame: {row_count}")

    if row_count == 0:
        logs_tdz.append((datetime.now(), "WARNING", f"⚠️ No valid data found in {gz_key}, skipping write."))
        return logs_tdz

    # ✅ Output path with partitioning based on today's date & hour
    output_s3_path = f"s3a://{BUCKET_NAME}/{S3_TDZ_PATH}/date={today_date}/hour={current_hour}/"
    print(f"🔍 Expected output path: {output_s3_path}")

    try:
        # ✅ Force Spark to commit by caching the DataFrame
        df.cache()

        # ✅ Write to Parquet with simplified partitioning
        df.write.mode("append") \
            .partitionBy("date", "hour") \
            .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/") \
            .parquet(output_s3_path)

        logs_tdz.append((datetime.now(), "INFO", f"✅ Processed {gz_key} and saved to {output_s3_path}"))

        # ✅ Check if files were written
        print(f"🔍 Verifying files at: {output_s3_path}")
        os.system(f"aws s3 ls {output_s3_path} --recursive")  # 🚀 Check if files exist

    except Exception as e:
        logs_tdz.append((datetime.now(), "ERROR", f"❌ Error writing {gz_key} to S3: {e}"))

    return logs_tdz


def validate_and_process_parallel(spark: SparkSession, gz_keys: list, schema_broadcast):
    """Validates and processes multiple .gz files in parallel and appends data to a single Parquet file."""
    logs_tdz = []
    schema_list = schema_broadcast.value

    # ✅ Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_file, spark, gz_key, schema_list): gz_key for gz_key in gz_keys}

        for future in futures:
            try:
                logs_tdz.extend(future.result())
            except Exception as e:
                logs_tdz.append((datetime.now(), "ERROR", f"⚠️ Error processing {futures[future]}: {e}"))

    # ✅ Save logs to a single Parquet file
    write_logs_to_parquet(spark, logs_tdz, "TDZ")

    return True, logs_tdz

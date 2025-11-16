import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import boto3
from pyspark.sql import SparkSession
from utils import write_logs_to_parquet
from file_validation import (
    list_recent_files,
    process_files_batch,
    validate_files_batch,
    move_invalid_files_batch,
    list_all_files,
    get_cutoff_time
)
from config import (
    BUCKET_NAME,
    S3_RAW_PATH,
    TIMEZONE_IST,
    SPARK_APP_NAME,
    S3_ENDPOINT,
    S3_IMPL,
    S3_CONNECTION_MAX,
    S3_CREDENTIALS_PROVIDER,
    SCHEMA_FILE_PATH,
    S3_TDZ_PATH,
)
from backup import backup_files_batch
from schema_validation import load_schema, validate_and_process_parallel

# Initialize AWS S3 client
s3 = boto3.client("s3")


def get_current_partition():
    """Computes today's date and current hour for partitioning."""
    current_utc = datetime.now(timezone.utc)  # ✅ Ensures UTC timezone awareness
    current_date = current_utc.strftime("%Y%m%d")  # Format: YYYYMMDD
    current_hour = current_utc.strftime("%H")  # Format: HH
    return current_date, current_hour


def initialize_spark():
    """Initializes and returns a Spark session with correct S3 settings."""
    try:
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.impl", S3_IMPL) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", S3_CREDENTIALS_PROVIDER) \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.critical(f"💥 Failed to initialize Spark session: {e}")
        sys.exit(1)


def main():
    """Main execution pipeline with optimized batch processing."""
    current_date, current_hour = get_current_partition()
    log_datetime_str = current_date
    log_hour_str = current_hour

    spark = initialize_spark()
    start_time = time.time()

    logs_rdz = []
    logs_tdz = []
    logs_global = []

    try:
        logs_global.append((datetime.now(timezone.utc), "INFO", f"🔄 Processing files for partition: date={current_date}, hour={current_hour}"))

        # ✅ Load Schema Once (Broadcasted for Efficiency)
        schema, schema_broadcast = load_schema(SCHEMA_FILE_PATH, spark)
        logs_global.append((datetime.now(timezone.utc), "INFO", f"✅ Schema loaded with {len(schema.fields)} columns from: {SCHEMA_FILE_PATH}"))

        # ✅ Step 1: List Recent Files (Optimized)
        files = list_recent_files(S3_RAW_PATH, get_cutoff_time())
  # ✅ Now timezone-aware
        if not files:
            logs_global.append((datetime.now(timezone.utc), "WARNING", f"⚠️ No recent files found in {S3_RAW_PATH}. Exiting."))
            return

        # ✅ Step 2: Identify and Move Invalid Files in **Batch**
        try:
            all_files = list_all_files(S3_RAW_PATH)
            logs_global.append((datetime.now(timezone.utc), "DEBUG", f"🛠 Found {len(all_files)} files in S3."))

            if all_files:
                invalid_files, invalid_logs = validate_files_batch(spark, all_files)
                logs_rdz.extend(invalid_logs)

                if invalid_files:
                    move_results = move_invalid_files_batch(spark, invalid_files)
                    logs_rdz.extend(move_results)
            else:
                logs_global.append((datetime.now(timezone.utc), "WARNING", "⚠️ No files found in S3 for validation."))

        except Exception as e:
            logs_global.append((datetime.now(timezone.utc), "ERROR", f"⚠️ Error in file validation: {e}"))
            logs_global.append((datetime.now(timezone.utc), "ERROR", traceback.format_exc()))

        # ✅ Step 3: Process Valid Files in **Batch**
        valid_files, error_logs = process_files_batch(spark, files)
        logs_global.append((datetime.now(timezone.utc), "INFO", f"✅ Found {len(valid_files)} valid file pairs for processing."))
        logs_tdz.extend(error_logs)

        # ✅ Step 4: Schema Validation & Processing in **Parallel**
        valid_gz_keys = [gz_key for gz_key, _, _, _ in valid_files]

        if valid_gz_keys:
            logs_global.append((datetime.now(timezone.utc), "INFO", f"🔍 Processing {len(valid_gz_keys)} files as **single optimized Parquet batch**."))
            
            # **Apply schema validation and parallel processing**
            success, processed_files_data = validate_and_process_parallel(spark, valid_gz_keys, schema_broadcast)
            
            if success:
                logs_tdz.append((datetime.now(timezone.utc), "INFO", f"✅ Successfully processed {len(processed_files_data)} files."))

                # ✅ Processed file path now follows today's date & hour partitioning
                processed_s3_path = f"s3a://{BUCKET_NAME}/{S3_TDZ_PATH}/date={current_date}/hour={current_hour}/"
                logs_tdz.append((datetime.now(timezone.utc), "INFO", f"📂 Processed data stored at: {processed_s3_path}"))

                # Debugging: Print & Check if files exist
                print(f"🔍 Checking S3 output path: {processed_s3_path}")
                os.system(f"aws s3 ls {processed_s3_path} --recursive")

            else:
                logs_tdz.append((datetime.now(timezone.utc), "ERROR", f"⚠️ Processing failed. No files added to the summary."))

        else:
            logs_tdz.append((datetime.now(timezone.utc), "INFO", "✅ No valid files to process for validation."))

        # ✅ Step 5: Backup Process **After Processing**
        backup_results = backup_files_batch(spark, valid_files)
        logs_rdz.extend(backup_results)

        logs_global.append((datetime.now(timezone.utc), "INFO", "✅ All processing completed successfully."))

    except Exception as e:
        logs_global.append((datetime.now(timezone.utc), "CRITICAL", f"💥 A critical error occurred in the ETL pipeline: {e}"))
        logs_global.append((datetime.now(timezone.utc), "ERROR", traceback.format_exc()))

    finally:
        total_time = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))
        logs_global.append((datetime.now(timezone.utc), "INFO", f"⏱ Total processing time: {total_time}"))

        # ✅ Save Logs Without Partitioning in TDZ
        log_path_tdz = f"s3a://{BUCKET_NAME}/LOGS/TDZ/logs.parquet"
        log_path_rdz = f"s3a://{BUCKET_NAME}/LOGS/RDZ/{log_datetime_str}/logs.parquet"
        log_path_global = f"s3a://{BUCKET_NAME}/LOGS/GLOBAL_LOG/{log_datetime_str}/logs.parquet"

        write_logs_to_parquet(spark, logs_tdz, log_path_tdz)
        write_logs_to_parquet(spark, logs_rdz, log_path_rdz)
        write_logs_to_parquet(spark, logs_global, log_path_global)

        # ✅ Stop Spark **Only After Logs Are Saved**
        if spark:
            spark.stop()
            logs_global.append((datetime.now(timezone.utc), "INFO", "✅ Spark session stopped."))


if __name__ == "__main__":
    main()

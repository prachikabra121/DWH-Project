import re
import logging
import boto3
from typing import List, Tuple, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from config import BUCKET_NAME, S3_ERROR_PATH, GLOBAL_LOG_PATH

# Initialize AWS S3 client
s3 = boto3.client("s3")

from datetime import timezone, timedelta

# ✅ Define IST Timezone (UTC+5:30)
TIMEZONE_IST = timezone(timedelta(hours=5, minutes=30))



def get_log_s3_path(zone: str, log_datetime_str: str) -> str:
    """Returns the correct S3 path for storing logs based on the zone."""
    safe_log_datetime_str = log_datetime_str.replace(":", "").replace(" ", "").replace("-", "")
    return f"{GLOBAL_LOG_PATH}{zone}/{safe_log_datetime_str}/"


def extract_details_from_filename(file_path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extracts store_code, transaction_date, and transaction_hour from the filename.
    Expected format: R520XXXXYYYYMMDDHHMMSS.gz or R520XXXXYYYYMMDDHHMMSS_END.
    """

    filename = file_path.strip().split("/")[-1]  # Extract only the filename

    # ✅ Match both `.gz` and `_END` files with strict regex
    match = re.match(r"^R520(\d{4})(\d{8})(\d{2})\d{4}(_END|\.gz)$", filename)

    if match:
        store_code, transaction_date, transaction_hour, _ = match.groups()
        return store_code.strip(), transaction_date.strip(), transaction_hour.strip()
    else:
        logging.warning(f"⚠️ Filename format invalid: {filename}")
        return None, None, None


def handle_error(
    spark: SparkSession, gz_key: Optional[str], end_key: Optional[str], log_datetime_str: str
) -> List[Tuple[bool, str]]:
    """
    Handles moving files to the error folder based on their availability and logs errors in Parquet.
    If the filename is invalid, it logs an error instead of moving the file.
    """
    logs_rdz = []

    for key in [gz_key, end_key]:
        if key:
            try:
                store_code, transaction_date, transaction_hour = extract_details_from_filename(key)

                if not transaction_date or not transaction_hour:
                    logs_rdz.append((datetime.now(), "ERROR", f"❌ Skipping move: Invalid filename format {key}"))
                    continue

                error_folder = f"{S3_ERROR_PATH}{transaction_date[:4]}/{transaction_date}/{transaction_hour}/"
                destination_key = f"{error_folder}{key.split('/')[-1]}"

                print(f"📂 Moving {key} ➡ {destination_key}")

                # ✅ Improved S3 copy error handling
                try:
                    s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=destination_key)
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                    logs_rdz.append((datetime.now(), "INFO", f"✅ Moved file {key} to error folder: {destination_key}"))
                except Exception as s3_error:
                    logs_rdz.append((datetime.now(), "ERROR", f"⚠️ Failed to move {key} to error folder: {s3_error}"))

            except Exception as e:
                logs_rdz.append((datetime.now(), "ERROR", f"⚠️ Unexpected error processing {key}: {e}"))

    # ✅ Save logs to Parquet
    write_logs_to_parquet(spark, logs_rdz, get_log_s3_path("error", log_datetime_str))

    return logs_rdz


def write_logs_to_parquet(spark: SparkSession, logs: List[Tuple[datetime, str, str]], log_path: str):
    """
    Writes logs to a Parquet file in the specified S3 location.
    """
    if not logs:
        print(f"⚠️ No logs to write for {log_path}.")
        return

    # ✅ Enforcing Schema for logs
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("level", StringType(), True),
        StructField("message", StringType(), True)
    ])

    df = spark.createDataFrame(logs, schema=schema)

    # ✅ Optimize write for efficiency
    df.coalesce(1).write.mode("append") \
        .option("fs.s3a.endpoint", "s3.ap-northeast-1.amazonaws.com") \
        .parquet(log_path)

    print(f"✅ Logs successfully written to {log_path}")

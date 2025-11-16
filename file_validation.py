import re
import logging
from datetime import datetime
from typing import Dict, Tuple, List
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from config import BUCKET_NAME, S3_ERROR_PATH
from utils import extract_details_from_filename, write_logs_to_parquet

# Initialize AWS S3 client
s3 = boto3.client("s3")

# ✅ Define Filename Validation Patterns
VALID_GZ_PATTERN = r'^R520\d{4}\d{8}\d{6}\.gz$'
VALID_END_PATTERN = r'^R520\d{4}\d{8}\d{6}_END$'


from datetime import datetime, timezone,timedelta
import boto3
from typing import Dict

# Initialize AWS S3 client
s3 = boto3.client("s3")
from config import TIMEZONE_IST  # ✅ Ensure TIMEZONE_IST is imported

def get_cutoff_time():
    """Returns the cutoff time as one hour before the current time in UTC."""
    return datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=1)

from datetime import datetime, timezone, timedelta
import boto3
from typing import Dict

# Initialize AWS S3 client
s3 = boto3.client("s3")

def list_recent_files(prefix: str, cutoff_time: datetime) -> Dict[str, Dict[str, str]]:
    """Lists files in S3 modified between the last hour and the current hour."""

    print(f"🔍 DEBUG: Searching for files in {prefix} from {cutoff_time} (UTC) to now.")

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    files = {}
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]  # ✅ Already timezone-aware (UTC from S3)

                print(f"🔍 DEBUG: Found file {key} - Last Modified (UTC): {last_modified}")

                # ✅ Skip directories
                if key.endswith("/") or not key.strip():
                    print(f"⚠️ WARNING: Skipping invalid entry {key}")
                    continue

                # ✅ Only process .gz and _END files
                if not (key.endswith(".gz") or key.endswith("_END")):
                    print(f"⚠️ WARNING: Skipping non-matching file {key}")
                    continue

                # ✅ Ensure proper datetime comparison (last hour to current time)
                if cutoff_time <= last_modified <= datetime.utcnow().replace(tzinfo=timezone.utc):
                    base_filename = key.split("/")[-1]
                    file_type = "gz" if base_filename.endswith(".gz") else "end"
                    base_key = base_filename.replace(".gz", "").replace("_END", "")
                    files.setdefault(base_key, {})[file_type] = key

    print(f"✅ DEBUG: Found {len(files)} valid file pairs.")
    return files

def list_all_files(prefix: str) -> List[str]:
    """Lists all files under the given prefix in S3."""
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    all_files = [obj["Key"] for page in page_iterator if "Contents" in page for obj in page["Contents"]]

    print(f"✅ DEBUG: Found {len(all_files)} total files in {prefix}.")
    return all_files


def validate_files_batch(spark: SparkSession, all_files: List[str]) -> Tuple[List[str], List[Tuple[bool, str]]]:
    """
    Validates files in batch using Spark for parallel processing.
    Identifies invalid files and logs them efficiently.
    """
    logs_rdz = []
    invalid_files = []

    print(f"🔍 DEBUG: Validating {len(all_files)} files for correctness.")

    schema = StructType([
        StructField("file_key", StringType(), True),
        StructField("filename", StringType(), True)
    ])

    df = spark.createDataFrame([(file, file.split('/')[-1]) for file in all_files], schema)

    df = df.withColumn("is_valid", col("filename").rlike(VALID_GZ_PATTERN) | col("filename").rlike(VALID_END_PATTERN))
    invalid_df = df.filter(~col("is_valid")).select("file_key")

    invalid_files = [row.file_key for row in invalid_df.collect()]
    logs_rdz.extend([(datetime.now(), "ERROR", f"❌ Invalid filename format: {file_key}") for file_key in invalid_files])

    # ✅ Save logs in a single daily Parquet file
    write_logs_to_parquet(spark, logs_rdz, "RDZ")

    print(f"✅ DEBUG: Identified {len(invalid_files)} invalid files.")
    return invalid_files, logs_rdz


def move_invalid_files_batch(spark: SparkSession, invalid_files: List[str]):
    """
    Moves all invalid files to the error folder in S3 in batch.
    """
    logs_rdz = []
    results = []

    if not invalid_files:
        print("✅ No invalid files found to move.")
        return results

    print(f"📂 Moving {len(invalid_files)} invalid files to the error folder...")

    for file_key in invalid_files:
        try:
            store_code, transaction_date, transaction_hour = extract_details_from_filename(file_key)
            if not transaction_date or not transaction_hour:
                print(f"⚠️ ERROR: Invalid file format, skipping: {file_key}")
                continue

            error_folder = f"{S3_ERROR_PATH}{transaction_date[:4]}/{transaction_date}/{transaction_hour}/"
            destination_key = f"{error_folder}{file_key.split('/')[-1]}"

            print(f"📂 DEBUG: Copying file {file_key} ➡ {destination_key}")

            response = s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': file_key}, Key=destination_key)

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"✅ Successfully copied: {file_key} to {destination_key}")
                s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
                print(f"🗑 Deleted original file: {file_key}")

                logs_rdz.append((datetime.now(), "INFO", f"✅ Moved invalid file {file_key} to {destination_key}"))
                results.append((True, f"✅ Moved {file_key} to {destination_key}"))
            else:
                logs_rdz.append((datetime.now(), "ERROR", f"⚠️ Failed to copy {file_key}, skipping delete."))
                results.append((False, f"⚠️ Copy failed for {file_key}."))

        except Exception as e:
            print(f"⚠️ ERROR moving file {file_key}: {e}")
            logs_rdz.append((datetime.now(), "ERROR", f"⚠️ Failed to move {file_key}: {e}"))
            results.append((False, f"⚠️ Failed to move {file_key}: {e}"))

    # ✅ Save logs in a single daily Parquet file
    write_logs_to_parquet(spark, logs_rdz, "RDZ")

    print(f"✅ DEBUG: Completed moving invalid files.")
    return results


def process_files_batch(spark: SparkSession, files: Dict[str, Dict[str, str]]) -> Tuple[List[Tuple[str, str, str, str]], List[Tuple[bool, str]]]:
    """
    Processes valid files and moves invalid ones to the error folder in batch.
    Returns a list of **valid files** in the format [(gz_key, end_key, transaction_date, transaction_hour)].
    """

    valid_files = []
    logs_rdz = []
    invalid_files = []

    for base_key, file_info in files.items():
        gz_key = file_info.get("gz")
        end_key = file_info.get("end")

        if gz_key and end_key:
            # ✅ Use `extract_details_from_filename` from utils.py
            store_code, transaction_date, transaction_hour = extract_details_from_filename(gz_key)

            if not transaction_date or not transaction_hour:
                logs_rdz.append((datetime.now(), "ERROR", f"❌ Invalid filename format: {gz_key}"))
                continue  # Skip invalid file

            valid_files.append((gz_key, end_key, transaction_date, transaction_hour))
            logs_rdz.append((datetime.now(), "INFO", f"✅ File validated: {gz_key} and {end_key}"))

        else:
            if gz_key:
                invalid_files.append(gz_key)
                logs_rdz.append((datetime.now(), "ERROR", f"❌ Missing _END file for {gz_key}, moving to error folder."))

            if end_key:
                invalid_files.append(end_key)
                logs_rdz.append((datetime.now(), "ERROR", f"❌ Missing .gz file for {end_key}, moving to error folder."))

    if invalid_files:
        move_invalid_files_batch(spark, invalid_files)

    write_logs_to_parquet(spark, logs_rdz, "RDZ")

    return valid_files, logs_rdz  # ✅ Now returns 4 values per tuple!

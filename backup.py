import boto3
from typing import List, Tuple
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from config import BUCKET_NAME, S3_BACKUP_PATH
from utils import write_logs_to_parquet

# Initialize AWS S3 client
s3 = boto3.client("s3")


def backup_single_file(file_info: Tuple[str, str, str, str]) -> List[Tuple[datetime, str, str]]:
    """
    Handles a **single** file backup in parallel execution.
    Returns logs for success/failure.
    """

    # ✅ Validate the input
    if len(file_info) != 4:
        error_message = f"❌ ERROR: Invalid file tuple {file_info}. Expected 4 values (gz_key, end_key, date, hour)."
        print(error_message)
        return [(datetime.now(), "ERROR", error_message)]

    gz_key, end_key, transaction_date, transaction_hour = file_info
    logs = []
    
    backup_folder = f"{S3_BACKUP_PATH}{transaction_date[:4]}/{transaction_date}/{transaction_hour}/"

    for key in [gz_key, end_key]:
        if not key:
            logs.append((datetime.now(), "WARNING", "⚠️ WARNING: Empty file key provided, skipping backup."))
            continue  # Skip empty file keys

        dest_key = f"{backup_folder}{key.split('/')[-1]}"

        try:
            # ✅ Perform S3 copy in a single step
            s3.copy_object(
                Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": key}, Key=dest_key
            )

            # ✅ If copy successful, delete original
            s3.delete_object(Bucket=BUCKET_NAME, Key=key)

            success_message = f"✅ Successfully backed up {key} to {dest_key}"
            logs.append((datetime.now(), "INFO", success_message))
            print(success_message)

        except Exception as e:
            error_message = f"⚠️ Backup failed for {key}: {e}"
            logs.append((datetime.now(), "ERROR", error_message))
            print(error_message)

    return logs


def backup_files_batch(spark: SparkSession, files: List[Tuple[str, str, str, str]]) -> List[Tuple[datetime, str, str]]:
    """
    Backs up multiple .gz and _END files **in one go** using parallel execution.
    Inserts all backup logs into a **single RDZ log file**.
    """

    logs_rdz = []  # ✅ Collect logs for all backup operations
    valid_files = []  # ✅ Only process valid files

    if not files:
        logs_rdz.append((datetime.now(), "WARNING", "⚠️ No files to back up."))
        write_logs_to_parquet(spark, logs_rdz, "RDZ")
        return logs_rdz  # No files to process

    print(f"📂 DEBUG: Starting bulk backup for {len(files)} file pairs.")

    # ✅ Validate file structure before processing
    for file_info in files:
        if len(file_info) != 4:
            error_message = f"❌ ERROR: Invalid file entry {file_info}. Expected 4 values (gz_key, end_key, date, hour)."
            logs_rdz.append((datetime.now(), "ERROR", error_message))
            print(error_message)
        else:
            valid_files.append(file_info)  # ✅ Only add valid files

    # ✅ If no valid files remain, stop execution
    if not valid_files:
        logs_rdz.append((datetime.now(), "CRITICAL", "💥 No valid files found for backup. Backup process aborted."))
        write_logs_to_parquet(spark, logs_rdz, "RDZ")
        return logs_rdz

    # ✅ Use ThreadPoolExecutor for Parallel Backup
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(backup_single_file, valid_files))

    # ✅ Consolidate logs from all parallel executions
    for log_set in results:
        logs_rdz.extend(log_set)

    # ✅ Append backup logs into the RDZ log file **only once**
    write_logs_to_parquet(spark, logs_rdz, "RDZ")

    print(f"✅ DEBUG: Bulk backup process completed.")
    return logs_rdz  # ✅ Return logs for further use if needed


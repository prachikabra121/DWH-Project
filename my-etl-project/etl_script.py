import boto3
import gzip
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from collections import defaultdict
import time
import sys
import argparse

# ------------------------------
# Configuration
# ------------------------------

# Argument Parsing for Flexibility
parser = argparse.ArgumentParser(description='S3-Only ETL Script for POS Data')
parser.add_argument('--date', type=str, required=True, help='Target date in YYYYMMDD format')
parser.add_argument('--hour', type=str, required=True, help='Target hour in HH format (24-hour)')
parser.add_argument('--month', type=str, required=True, help='Target month in YYYYMM format')

args = parser.parse_args()

TARGET_DATE = args.date
TARGET_HOUR = args.hour
S3_RAW_PATH = f"Pos-Raw-Files/{args.month}/"

# Initialize AWS S3 client
s3 = boto3.client('s3')

# S3 Bucket and Paths
BUCKET_NAME = "belc-dwh-poc"
S3_BACKUP_PATH = "RDZ/backup/"              # Path for valid processed files
S3_ERROR_PATH = "RDZ/error-files/"          # Path for invalid or unmatched files
S3_LOG_PATH = "RDZ/logs/"                    # Path for storing logs

# Logging configuration
LOG_FILENAME = f"processing_log_{TARGET_DATE}.log"
LOG_FILE_PATH = LOG_FILENAME  # Logs will be stored locally and then uploaded to S3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)

# ------------------------------
# Initialize Spark Session
# ------------------------------

spark = SparkSession.builder \
    .appName("BinaryParserETL_Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

logging.info("Spark session initialized.")

# ------------------------------
# Helper Functions
# ------------------------------

def extract_date_time_from_filename(filename):
    """
    Extracts the transaction date and hour from the given filename.
    Assumes filename structure where date starts at the 9th character and time follows.
    Example: R520000220241018120107.gz
             |----|----|----|----|
             1    2    3    4    5
             2024-04-18 12:01:07
    """
    try:
        if filename.endswith('.gz'):
            base_filename = filename.split('/')[-1].split('.')[0]
        elif filename.endswith('_END'):
            base_filename = filename.split('/')[-1].split('_END')[0]
        else:
            logging.warning(f"Unexpected file format: {filename}. Skipping.")
            return None, None, None

        transaction_date = base_filename[8:16]  # Extract YYYYMMDD
        transaction_time = base_filename[16:22]  # Extract HHMMSS
        hour = transaction_time[:2]
        return transaction_date, hour, base_filename
    except IndexError as e:
        logging.error(f"Filename parsing error for {filename}: {e}")
        return None, None, None

def process_file_content(compressed_bytes, filename):
    """
    Decompresses the gzipped file content.
    """
    try:
        file_content = gzip.decompress(compressed_bytes)
        logging.info(f"Successfully decompressed file: {filename}")
    except OSError:
        file_content = compressed_bytes
        logging.warning(f"Failed to decompress file: {filename}. Using original bytes.")
    return file_content

def upload_log_to_s3():
    """
    Uploads the local log file to S3 logs path.
    """
    log_filename = LOG_FILENAME
    try:
        s3.upload_file(
            log_filename,
            BUCKET_NAME,
            f"{S3_LOG_PATH}{log_filename}",
            ExtraArgs={'ServerSideEncryption': 'AES256'}
        )
        logging.info(f"Successfully uploaded log file to s3://{BUCKET_NAME}/{S3_LOG_PATH}{log_filename}")
    except Exception as e:
        logging.error(f"Failed to upload log file. Error: {e}")

def move_s3_file(source_key, destination_folder, retries=3, delay=5):
    """
    Moves an S3 file from the raw path to the backup folder with retry logic.
    """
    destination_key = f"{destination_folder}{source_key.split('/')[-1]}"
    attempt = 0
    while attempt < retries:
        try:
            # Copy file to backup location
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
                Key=destination_key
            )

            # Delete the original file from raw path
            s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)
            
            logging.info(f"Moved {source_key} to {destination_key}")
            return
        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt}: Error moving file {source_key}: {e}")
            time.sleep(delay)
    logging.error(f"Failed to move file {source_key} after {retries} attempts.")

def move_s3_file_to_error(gz_key, end_key, retries=3, delay=5):
    """
    Moves .gz and _END files to the error-files path in S3 with retry logic.
    """
    def move_file(source_key, error_key):
        attempt = 0
        while attempt < retries:
            try:
                s3.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
                    Key=error_key
                )
                s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)
                logging.info(f"Moved {source_key} to {error_key}")
                return
            except Exception as e:
                attempt += 1
                logging.error(f"Attempt {attempt}: Error moving file {source_key} to {error_key}: {e}")
                time.sleep(delay)
        logging.error(f"Failed to move file {source_key} to {error_key} after {retries} attempts.")

    try:
        if gz_key:
            gz_filename = gz_key.split('/')[-1]
            error_key_gz = f"{S3_ERROR_PATH}{gz_filename}"
            move_file(gz_key, error_key_gz)
        
        if end_key:
            end_filename = end_key.split('/')[-1]
            error_key_end = f"{S3_ERROR_PATH}{end_filename}"
            move_file(end_key, error_key_end)
    except Exception as e:
        logging.error(f"Error moving files to error path: {e}")

def validate_and_group_files(files):
    """
    Groups files by base filename and validates the presence of both .gz and _END files.
    Returns a dictionary with base_filename as keys and their corresponding file types.
    """
    files_dict = {}
    for file_key in files:
        if file_key.endswith('.gz'):
            base_filename = file_key.split('/')[-1][:-3]
            file_type = 'gz'
        elif file_key.endswith('_END'):
            base_filename = file_key.split('/')[-1][:-4]
            file_type = 'end'
        else:
            logging.warning(f"Unexpected file format: {file_key}. Skipping.")
            continue
        
        if base_filename not in files_dict:
            files_dict[base_filename] = {'gz': False, 'end': False, 'gz_key': None, 'end_key': None}
        
        files_dict[base_filename][file_type] = True
        files_dict[base_filename][f"{file_type}_key"] = file_key
    return files_dict

def log_summary_statistics(date_counters, hour_counters):
    """
    Logs the summary statistics for total files, processed files, unprocessed files,
    and total processed file sizes based on daily and hourly counts.
    """
    logging.info("----- Summary Statistics -----")

    # Daily Summary
    logging.info("Daily Summary:")
    for date, counts in date_counters.items():
        total = counts['total']
        processed = counts['processed']
        unprocessed = counts['unprocessed']
        processed_size = format_size(counts['processed_size'])
        logging.info(f"Date: {date} | Total Files: {total} | Processed: {processed} | Unprocessed: {unprocessed} | Total Processed Size: {processed_size}")

    # Hourly Summary
    logging.info("Hourly Summary:")
    for hour, counts in hour_counters.items():
        total = counts['total']
        processed = counts['processed']
        unprocessed = counts['unprocessed']
        processed_size = format_size(counts['processed_size'])
        logging.info(f"Hour: {hour} | Total Files: {total} | Processed: {processed} | Unprocessed: {unprocessed} | Total Processed Size: {processed_size}")

    logging.info("----- End of Summary -----")

def format_size(bytes_size):
    """
    Formats the size from bytes to a more readable format (KB, MB, GB).
    """
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} PB"

# ------------------------------
# Processing Functions
# ------------------------------

def process_s3_files_with_spark():
    """
    Processes files from S3 using Spark and moves them to backup or error paths based on validation.
    Additionally, logs total file counts, processed counts, unprocessed counts, and total processed file sizes
    on both daily and hourly bases.
    """
    logging.info("Starting S3 file processing with Spark.")

    # Initialize counters
    date_counters = defaultdict(lambda: {'total': 0, 'processed': 0, 'unprocessed': 0, 'processed_size': 0})
    hour_counters = defaultdict(lambda: {'total': 0, 'processed': 0, 'unprocessed': 0, 'processed_size': 0})

    try:
        # List all relevant files in the S3 raw path
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_RAW_PATH)
        if 'Contents' not in response:
            logging.info(f"No files found in s3://{BUCKET_NAME}/{S3_RAW_PATH}")
            return
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.gz') or obj['Key'].endswith('_END')]

        if not files:
            logging.info(f"No .gz or _END files found in s3://{BUCKET_NAME}/{S3_RAW_PATH}")
            return

        # Group files by base filename
        files_dict = validate_and_group_files(files)

        for base_filename, file_info in files_dict.items():
            gz_key = file_info.get('gz_key')
            end_key = file_info.get('end_key')

            # Validation: Check if both .gz and _END files exist
            if file_info['gz'] and file_info['end']:
                # Extract date and hour from filename
                gz_filename = gz_key.split('/')[-1]
                transaction_date, hour, _ = extract_date_time_from_filename(gz_filename)

                if not transaction_date or not hour:
                    logging.error(f"Failed to extract date and hour from filename: {gz_filename}. Skipping file pair.")
                    move_s3_file_to_error(gz_key, end_key)
                    date_counters['Unknown']['unprocessed'] += 1
                    hour_counters['Unknown']['unprocessed'] += 1
                    continue

                # Update total counts
                date_counters[transaction_date]['total'] += 1
                hour_counters[hour]['total'] += 1

                # Validation: Check if .gz file is not 0KB
                try:
                    gz_obj = s3.head_object(Bucket=BUCKET_NAME, Key=gz_key)
                    gz_size = gz_obj['ContentLength']
                    if gz_size == 0:
                        logging.warning(f"S3 file {gz_key} is 0KB. Moving to error path.")
                        move_s3_file_to_error(gz_key, end_key)
                        # Update unprocessed counts
                        date_counters[transaction_date]['unprocessed'] += 1
                        hour_counters[hour]['unprocessed'] += 1
                        continue
                except Exception as e:
                    logging.error(f"Error accessing S3 file {gz_key}: {e}")
                    move_s3_file_to_error(gz_key, end_key)
                    # Update unprocessed counts
                    date_counters[transaction_date]['unprocessed'] += 1
                    hour_counters[hour]['unprocessed'] += 1
                    continue

                # Validation: Check if date and hour match the target
                if transaction_date == TARGET_DATE and hour == TARGET_HOUR:
                    try:
                        input_path = f"s3a://{BUCKET_NAME}/{gz_key}"
                        binary_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.gz").load(input_path)
                        
                        # Process binary content
                        parsed_rdd = binary_df.rdd.map(lambda row: process_file_content(row.content, gz_filename))
                        
                        # Placeholder for further data processing
                        # Example: Collecting the data (use with caution for large datasets)
                        # data = parsed_rdd.collect()
                        # logging.info(f"Processed data from {gz_filename}: {data}")

                        # Example: Calculate total processed file size
                        processed_size = gz_size  # Assuming gz_size is the size of the .gz file
                        
                        # Update processed counts and sizes
                        date_counters[transaction_date]['processed'] += 1
                        date_counters[transaction_date]['processed_size'] += processed_size
                        hour_counters[hour]['processed'] += 1
                        hour_counters[hour]['processed_size'] += processed_size

                        # After processing, move the files to backup
                        s3_folder = f"{S3_BACKUP_PATH}year={transaction_date[:4]}/month={transaction_date[4:6]}/day={transaction_date[6:]}/hour={hour}/"
                        move_s3_file(gz_key, s3_folder)
                        move_s3_file(end_key, s3_folder)
                    
                    except Exception as e:
                        logging.error(f"Failed to process {gz_key}. Error: {e}")
                        move_s3_file_to_error(gz_key, end_key)
                        # Update unprocessed counts
                        date_counters[transaction_date]['unprocessed'] += 1
                        hour_counters[hour]['unprocessed'] += 1
                else:
                    logging.warning(f"S3 file {gz_key} has different date/hour. Moving to error path.")
                    move_s3_file_to_error(gz_key, end_key)
                    # Update unprocessed counts
                    date_counters[transaction_date]['unprocessed'] += 1
                    hour_counters[hour]['unprocessed'] += 1
            else:
                # Missing pair, move existing files to error path
                if file_info['gz']:
                    logging.warning(f"Missing _END file for {gz_key}. Moving to error path.")
                    move_s3_file_to_error(gz_key, None)
                    # Extract date and hour from filename
                    gz_filename = gz_key.split('/')[-1]
                    transaction_date, hour, _ = extract_date_time_from_filename(gz_filename)
                    if not transaction_date or not hour:
                        logging.error(f"Failed to extract date and hour from filename: {gz_filename}. Skipping file.")
                        date_counters['Unknown']['unprocessed'] += 1
                        hour_counters['Unknown']['unprocessed'] += 1
                        continue
                    # Update counts
                    date_counters[transaction_date]['unprocessed'] += 1
                    hour_counters[hour]['unprocessed'] += 1
                if file_info['end']:
                    logging.warning(f"Missing .gz file for {end_key}. Moving to error path.")
                    move_s3_file_to_error(None, end_key)
                    # Extract date and hour from filename
                    end_filename = end_key.split('/')[-1]
                    transaction_date, hour, _ = extract_date_time_from_filename(end_filename)
                    if not transaction_date or not hour:
                        logging.error(f"Failed to extract date and hour from filename: {end_filename}. Skipping file.")
                        date_counters['Unknown']['unprocessed'] += 1
                        hour_counters['Unknown']['unprocessed'] += 1
                        continue
                    # Update counts
                    date_counters[transaction_date]['unprocessed'] += 1
                    hour_counters[hour]['unprocessed'] += 1

    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
        # Optionally, you can re-raise the exception or handle it as needed

    # After processing all files, log the summary statistics
    log_summary_statistics(date_counters, hour_counters)

    logging.info("Completed S3 file processing with Spark.")

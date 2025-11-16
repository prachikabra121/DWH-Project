# config.py

import pytz

# -----------------------------------
# AWS S3 Configuration
# -----------------------------------
BUCKET_NAME = "belc-dwh-poc"
S3_RAW_PATH = "RDZ/Raw-Data/"          # No date-based subfolders
S3_BACKUP_PATH = "RDZ/backup/"
S3_ERROR_PATH = "RDZ/error-files/"
S3_LOG_PATH = "RDZ/logs/"
S3_TDZ_PATH = "TDZ/"    # Path for saving processed CSV files
GLOBAL_LOG_PATH = "LOGS/"

               

# -----------------------------------
# Schema Configuration
# -----------------------------------
SCHEMA_FILE_PATH = "/app/schema.xlsx"  # Path to the Excel schema file

# -----------------------------------
# Timezone Configuration
# -----------------------------------
TIMEZONE_IST = pytz.timezone("Asia/Kolkata")

# -----------------------------------
# Spark Configuration
# -----------------------------------
SPARK_APP_NAME = "BinaryParserETL"
S3_ENDPOINT = "s3.ap-northeast-1.amazonaws.com"  # Update if using a different region or endpoint
S3_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem"
S3_CONNECTION_MAX = "100"
S3_CREDENTIALS_PROVIDER = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

# -----------------------------------
# Logging Configuration
# -----------------------------------
# Note: Logging paths are managed within the scripts to include date-based folder structures.
# No additional logging configurations are needed here.

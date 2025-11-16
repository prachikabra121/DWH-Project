import boto3


def copy_s3_files(bucket_name, source_folder, destination_folder, aws_access_key=None, aws_secret_key=None, region_name='us-east-1'):
    """
    Copies all files from source_folder to destination_folder within the same S3 bucket.
    
    :param bucket_name: Name of the S3 bucket
    :param source_folder: Source folder path in S3 (prefix)
    :param destination_folder: Destination folder path in S3 (prefix)
    :param aws_access_key: AWS access key ID (optional, if not using IAM roles)
    :param aws_secret_key: AWS secret access key (optional, if not using IAM roles)
    :param region_name: AWS region name
    """
    
    # Initialize S3 client
    if aws_access_key and aws_secret_key:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
    else:
        s3_client = boto3.client('s3')
    
    # List objects in the source folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']
            
            # Extract the filename
            filename = source_key.split('/')[-1]
            
            # Define destination key
            destination_key = f"{destination_folder}/{filename}"
            
            # Copy object
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
            print(f"Copied: {source_key} -> {destination_key}")
    else:
        print("No files found in source folder.")

# Example usage
if __name__ == "__main__":
    copy_s3_files(
        bucket_name='belc-dwh-poc',
        source_folder='Pos-Raw-Files/20250120/',
        destination_folder='RDZ/Raw-Data'
    )

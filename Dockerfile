# Start from the Bitnami Spark base image
FROM bitnami/spark:3.3.1

# Switch to root user to install necessary packages
USER root

# Fix missing apt-get directories, install curl, unzip, and Python packages
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y curl unzip && \
    pip install --no-cache-dir pandas openpyxl boto3 python-dotenv numpy requests

# Set Hadoop AWS JAR for S3 support
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    mv hadoop-aws-3.3.1.jar /opt/bitnami/spark/jars/

RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    mv aws-java-sdk-bundle-1.11.1026.jar /opt/bitnami/spark/jars/

# Set the working directory
WORKDIR /app

# Copy application files with correct casing
COPY file_validation.py .
COPY backup.py .
COPY config.py .
COPY schema_validation.py .
COPY main.py .
COPY utils.py .
COPY schema.xlsx .  

# Copy the requirements.txt file for additional dependencies
COPY requirements.txt .

# Uncomment the following line if you want to install additional Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install AWS CLI for debugging or manual checks (optional)
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip ./aws

# Allow mounting AWS credentials
# Specify AWS_REGION as fallback
ENV AWS_REGION=ap-northeast-1

# Default command to run Spark ETL
CMD ["bash", "-c", "spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.1 main.py"]
# Use Bitnami Spark as the base image
FROM bitnami/spark:3.3.1

# Switch to root user
USER root

# Install necessary system dependencies
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y python3 python3-pip curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir pandas openpyxl boto3 python-dotenv numpy requests awscli

# Download required Hadoop AWS JARs for S3 support
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    mv hadoop-aws-3.3.1.jar aws-java-sdk-bundle-1.11.1026.jar /opt/bitnami/spark/jars/

# Set working directory
WORKDIR /app

# Copy ETL script into the container
COPY etl_script.py .

# Set the default command to execute spark-submit with the script
ENTRYPOINT ["spark-submit", "--master", "local[*]", "--packages", "org.apache.hadoop:hadoop-aws:3.3.1", "etl_script.py"]

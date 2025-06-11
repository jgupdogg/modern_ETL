FROM apache/airflow:2.10.5

# Switch to root to install system packages
USER root

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.2.10 \
    boto3==1.35.82
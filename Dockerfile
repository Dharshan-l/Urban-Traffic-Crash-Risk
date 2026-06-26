FROM python:3.10-slim

# Install Java (FIXED VERSION)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY . /app

RUN pip install pyspark

CMD ["python", "data_ingestion/spark_jobs/cleaning_jobs.py"]
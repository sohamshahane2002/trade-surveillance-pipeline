FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars

RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -O /opt/spark/jars/spark-sql-kafka.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    -O /opt/spark/jars/spark-token-provider-kafka.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    -O /opt/spark/jars/kafka-clients.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -O /opt/spark/jars/commons-pool2.jar && \
    wget -q https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar \
    -O /opt/spark/jars/snappy-java.jar && \
    wget -q https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
    -O /opt/spark/jars/lz4-java.jar

WORKDIR /app

COPY requirements_spark.txt .

RUN pip3 install -r requirements_spark.txt
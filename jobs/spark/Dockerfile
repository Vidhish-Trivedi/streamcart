FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3.12 python3.12-venv wget ca-certificates bash \
    && ln -sf /usr/bin/python3.12 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.12 /usr/bin/python \
    && python3.12 -m ensurepip --upgrade \
    && rm -rf /var/lib/apt/lists/*

RUN python3.12 -m pip install --no-cache-dir \
      pyspark==3.5.3 fastavro==1.9.7 psycopg2-binary==2.9.10 orjson==3.10.11 python-dotenv==1.0.1

WORKDIR /opt/jars
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && wget -q https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
 && wget -q https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

WORKDIR /opt/app
COPY streamcart /opt/app/streamcart
COPY schemas /opt/app/schemas
COPY jobs/spark /opt/app/jobs
COPY jobs/spark/entrypoint.sh /opt/entrypoint.sh
ENV PYTHONPATH=/opt/app
ENV PYTHONUNBUFFERED=1
WORKDIR /opt/app/jobs
RUN sed -i 's/\r$//' /opt/entrypoint.sh && chmod +x /opt/entrypoint.sh
ENTRYPOINT ["bash", "/opt/entrypoint.sh"]

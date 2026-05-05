FROM apache/spark:3.3.1

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir "numpy<2" pyspark==3.3.1 pandas clickhouse-connect pyarrow



RUN mkdir -p /opt/spark/jars_external && \
    mkdir -p /opt/spark/checkpoints && \
    mkdir -p /app

COPY spark/ /app/


COPY jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar /opt/spark/jars_external/
COPY jars/spark-sql-kafka-0-10_2.12-3.3.1.jar /opt/spark/jars_external/
COPY jars/spark-token-provider-kafka-0-10_2.12-3.3.1.jar /opt/spark/jars_external/
COPY jars/kafka-clients-2.8.1.jar /opt/spark/jars_external/
COPY jars/commons-pool2-2.11.1.jar /opt/spark/jars_external/
COPY jars/hadoop-aws-3.3.2.jar /opt/spark/jars_external/
COPY jars/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars_external/

RUN chmod -R 777 /app /opt/spark/jars_external /opt/spark/checkpoints

WORKDIR /app

CMD ["/bin/bash"]
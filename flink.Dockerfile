FROM --platform=linux/amd64 flink:2.2.1-scala_2.12

USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==2.2.1 --break-system-packages

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.1.0/kafka-clients-4.1.0.jar

USER flink
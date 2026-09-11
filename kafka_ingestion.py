import json

from kafka import KafkaProducer


class KafkaTelemetryProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value, default=str
            ).encode("utf-8"),
        )

    def send(self, telemetry):
        print(f"Sending telemetry to Kafka: {len(telemetry)}")

        for record in telemetry:
            self.producer.send(
                self.topic,
                key=record["server_id"].encode("utf-8"),
                value=record,
            )

        self.producer.flush()

    def close(self):
        self.producer.close()
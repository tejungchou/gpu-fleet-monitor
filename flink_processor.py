import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)

def parse_telemetry(raw_record):
    return json.loads(raw_record)


def check_temperature(record):
    temperature = record["gpu_temperature"]

    if temperature >= 90:
        return {
            "rack_id": record["rack_id"],
            "server_id": record["server_id"],
            "gpu_id": record["gpu_id"],
            "issue_type": "HIGH_TEMPERATURE",
            "severity": "CRITICAL",
            "temperature": temperature,
            "collect_time": record["collect_time"],
        }

    if temperature > 85:
        return {
            "rack_id": record["rack_id"],
            "server_id": record["server_id"],
            "gpu_id": record["gpu_id"],
            "issue_type": "HIGH_TEMPERATURE",
            "severity": "WARNING",
            "temperature": temperature,
            "collect_time": record["collect_time"],
        }

    return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("gpu-telemetry")
        .set_group_id("gpu-health-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "gpu-telemetry-source",
    )

    telemetry_stream = raw_stream.map(
        parse_telemetry,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    issue_stream = (
        telemetry_stream
        .map(
            check_temperature,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(lambda issue: issue is not None)
    )

    issue_stream.print()

    env.execute("GPU Fleet Health Processor")


if __name__ == "__main__":
    main()
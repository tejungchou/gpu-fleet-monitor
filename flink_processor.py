from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


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

    telemetry_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "gpu-telemetry-source",
    )

    telemetry_stream.print()

    env.execute("GPU Fleet Health Processor")


if __name__ == "__main__":
    main()
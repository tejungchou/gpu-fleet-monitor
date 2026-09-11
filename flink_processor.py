import json
import psycopg2

from pyflink.datastream.functions import MapFunction
from datetime import datetime
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)

def parse_telemetry(raw_record):
    return json.loads(raw_record)

def issue_to_row(issue):
    return (
        issue["rack_id"],
        issue["server_id"],
        issue["gpu_id"],
        issue["issue_type"],
        issue["severity"],
        issue.get("temperature"),
        issue.get("previous_count"),
        issue.get("current_count"),
        datetime.fromisoformat(issue["collect_time"])
    )

class GPUHealthProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.previous_single_bit_errors = runtime_context.get_state(
            ValueStateDescriptor(
                "previous_single_bit_errors",
                Types.INT()
            )
        )

        self.previous_double_bit_errors = runtime_context.get_state(
            ValueStateDescriptor(
                "previous_double_bit_errors",
                Types.INT()
            )
        )

    def process_element(
        self,
        record,
        ctx: "KeyedProcessFunction.Context"
    ):
        temperature = record["gpu_temperature"]

        # Temperature rule
        if temperature >= 90:
            yield {
                "rack_id": record["rack_id"],
                "server_id": record["server_id"],
                "gpu_id": record["gpu_id"],
                "issue_type": "HIGH_TEMPERATURE",
                "severity": "CRITICAL",
                "temperature": temperature,
                "collect_time": record["collect_time"],
            }

        elif temperature > 85:
            yield {
                "rack_id": record["rack_id"],
                "server_id": record["server_id"],
                "gpu_id": record["gpu_id"],
                "issue_type": "HIGH_TEMPERATURE",
                "severity": "WARNING",
                "temperature": temperature,
                "collect_time": record["collect_time"],
            }

        # Current ECC counters
        current_single = record["gpu_ecc_single_bit_errors"]
        current_double = record["gpu_ecc_double_bit_errors"]

        # Previous ECC counters stored by Flink
        previous_single = self.previous_single_bit_errors.value()
        previous_double = self.previous_double_bit_errors.value()

        # Only compare if we already have a previous sample
        if previous_double is not None and current_double > previous_double:
            yield {
                "rack_id": record["rack_id"],
                "server_id": record["server_id"],
                "gpu_id": record["gpu_id"],
                "issue_type": "UNCORRECTABLE_ECC_ERROR",
                "severity": "CRITICAL",
                "previous_count": previous_double,
                "current_count": current_double,
                "collect_time": record["collect_time"],
            }

        if previous_single is not None and current_single > previous_single:
            yield {
                "rack_id": record["rack_id"],
                "server_id": record["server_id"],
                "gpu_id": record["gpu_id"],
                "issue_type": "CORRECTABLE_ECC_ERROR",
                "severity": "WARNING",
                "previous_count": previous_single,
                "current_count": current_single,
                "collect_time": record["collect_time"],
            }

        # Save current counters for the next telemetry record
        self.previous_single_bit_errors.update(current_single)
        self.previous_double_bit_errors.update(current_double)

class PostgreSQLIssueWriter(MapFunction):

    def open(self, runtime_context):
        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            database="gpu_monitor",
            user="gpu_user",
            password="gpu_password",
        )
        self.cursor = self.connection.cursor()

    def map(self, issue):
        self.cursor.execute(
            """
            INSERT INTO gpu_issues (
                rack_id,
                server_id,
                gpu_id,
                issue_type,
                severity,
                temperature,
                previous_count,
                current_count,
                collect_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                issue["rack_id"],
                issue["server_id"],
                issue["gpu_id"],
                issue["issue_type"],
                issue["severity"],
                issue.get("temperature"),
                issue.get("previous_count"),
                issue.get("current_count"),
                issue["collect_time"],
            ),
        )

        self.connection.commit()
        return issue

    def close(self):
        self.cursor.close()
        self.connection.close()

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

    keyed_stream = telemetry_stream.key_by(
        lambda record: f'{record["server_id"]}:{record["gpu_id"]}',
        key_type=Types.STRING(),
    )

    issue_stream = keyed_stream.process(
        GPUHealthProcessFunction(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    issue_stream.print()

    stored_issue_stream = issue_stream.map(
    PostgreSQLIssueWriter(),
    output_type=Types.PICKLED_BYTE_ARRAY(),
)

    stored_issue_stream.print()

    env.execute("GPU Fleet Health Processor")


if __name__ == "__main__":
    main()
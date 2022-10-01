from invoke import Collection, task

from .build import build_spark_streaming_apps
from .utils import run_java_lib


@task()
def run_parquet_to_hudi(ctx):
    run_java_lib(ctx, "spark-streaming-apps", "spark.batch.runners.ParquetToHudi")


@task()
def run_hive_db_creation(ctx):
    run_java_lib(ctx, "spark-streaming-apps", "spark.utils.HiveDatabasesCreator")


@task()
def run_stream_and_table_joiner(ctx):
    run_java_lib(
        ctx, "spark-streaming-apps", "spark.stream.runners.hudi.JoinTable2WithHudiTable"
    )


@task()
def run_kafka_to_hudi(ctx, args=""):
    run_java_lib(
        ctx, "spark-streaming-apps", "spark.stream.runners.hudi.KafkaToHudi", args=args
    )


spark_streaming_collection = Collection()
spark_streaming_collection.add_task(build_spark_streaming_apps, name="build")
spark_streaming_collection.add_task(run_parquet_to_hudi, name="parquet_to_hudi")
spark_streaming_collection.add_task(run_hive_db_creation, name="hive_db_creation")
spark_streaming_collection.add_task(
    run_stream_and_table_joiner, name="stream_and_table_joiner"
)
spark_streaming_collection.add_task(run_kafka_to_hudi, name="kafka_to_hudi")

from invoke import Collection, task

from .build import build_spark_streaming_apps
from .utils import run_java_lib


@task()
def run_kafka_to_hudi(ctx, args=""):
    run_java_lib(
        ctx, "spark-streaming-apps", "spark.stream.runners.KafkaToHudi", args=args
    )


@task()
def run_db_creator(ctx, args=""):
    run_java_lib(
        ctx, "spark-streaming-apps", "spark.utils.HiveDatabasesCreator", args=args
    )


spark_streaming_collection = Collection()
spark_streaming_collection.add_task(build_spark_streaming_apps, name="build")
spark_streaming_collection.add_task(run_kafka_to_hudi, name="kafka_to_hudi")
spark_streaming_collection.add_task(run_db_creator, name="create_db")

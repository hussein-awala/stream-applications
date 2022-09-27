from invoke import Collection, task

from .build import build_kafka_producers
from .utils import run_java_lib


@task()
def run_csv_to_kafka(ctx):
    run_java_lib(ctx, "kafka-producers", "csv.CsvToKafka")


@task()
def run_topic_faker(ctx):
    run_java_lib(ctx, "kafka-producers", "fakers.MessagesFaker")


@task()
def run_multi_topic_faker(ctx):
    run_java_lib(ctx, "kafka-producers", "fakers.MessagesFakerMultipleTable")


kafka_collection = Collection()
kafka_collection.add_task(build_kafka_producers, name="build")
kafka_collection.add_task(run_csv_to_kafka, name="csv_to_kafka")
kafka_collection.add_task(run_topic_faker, name="messages_faker")
kafka_collection.add_task(run_multi_topic_faker, name="multi_messages_faker")

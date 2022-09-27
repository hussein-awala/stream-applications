from invoke import task, Collection

from .utils import run_java_lib
from .build import build_kafka_producers


@task()
def run_csv_to_kafka(ctx):
    run_java_lib(ctx, "kafka-producers", "csv.CsvToKafka")


@task()
def run_messages_faker(ctx):
    run_java_lib(ctx, "kafka-producers", "fakers.MessagesFaker")


kafka_collection = Collection()
kafka_collection.add_task(run_csv_to_kafka, name="csv_to_kafka")
kafka_collection.add_task(run_messages_faker, name="messages_faker")
kafka_collection.add_task(build_kafka_producers, name="build")

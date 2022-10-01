from invoke import Collection, task

from .utils import clean_java_lib


@task(help={"project-name": "The name of the project you want to clean"})
def clean_java(ctx, project_name):
    clean_java_lib(ctx, project_name)


@task
def clean_kafka_producers(ctx):
    clean_java(ctx, "kafka-producers")


@task
def clean_kstreams_apps(ctx):
    clean_java(ctx, "kstream-apps")


@task
def clean_spark_streaming_apps(ctx):
    clean_java(ctx, "spark-streaming-apps")


@task
def clean_all(ctx):
    clean_kafka_producers(ctx)
    clean_kstreams_apps(ctx)
    clean_spark_streaming_apps(ctx)


clean_collection = Collection()
clean_collection.add_task(clean_java, name="java")
clean_collection.add_task(clean_kafka_producers, name="kafka_producers")
clean_collection.add_task(clean_kstreams_apps, name="kstreams_apps")
clean_collection.add_task(clean_spark_streaming_apps, name="spark_streaming_apps")
clean_collection.add_task(clean_all, name="all")

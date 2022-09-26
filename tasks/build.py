from invoke import task, Collection

from .utils import build_java_lib


@task(help={
    "project-name": "The name of the project you want to build"
})
def build_java(ctx, project_name):
    build_java_lib(ctx, project_name)


@task
def build_kafka_producers(ctx):
    build_java(ctx, "kafka-producers")


@task
def build_kstream_apps(ctx):
    build_java(ctx, "kstream-apps")


@task
def build_spark_streaming_apps(ctx):
    build_java(ctx, "spark-streaming-apps")


@task
def build_all(ctx):
    build_kafka_producers(ctx)
    build_kstream_apps(ctx)
    build_spark_streaming_apps(ctx)


build_collection = Collection()
build_collection.add_task(build_java, name="java")
build_collection.add_task(build_kafka_producers, name="kafka_producers")
build_collection.add_task(build_kstream_apps, name="kstream_apps")
build_collection.add_task(build_spark_streaming_apps, name="spark_streaming_apps")
build_collection.add_task(build_all, name="all")

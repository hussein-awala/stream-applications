from invoke import Collection, task

from .build import build_kstream_apps
from .utils import run_java_lib


@task()
def run_topic_transformer(ctx):
    run_java_lib(ctx, "kstream-apps", "kstreams.Table1TopicTransformer")


@task()
def run_topics_joiner(ctx):
    run_java_lib(ctx, "kstream-apps", "kstreams.JoinTable1AndTable2Topics")


kstreams_collection = Collection()
kstreams_collection.add_task(build_kstream_apps, name="build")
kstreams_collection.add_task(run_topic_transformer, name="topic_transformer")
kstreams_collection.add_task(run_topics_joiner, name="topics_joiner")

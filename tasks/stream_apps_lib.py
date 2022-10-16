from invoke import Collection, task

from .build import build_stream_apps_lib
from .utils import run_java_lib, test_java_lib


@task()
def test(ctx):
    test_java_lib(ctx, "lib")


stream_apps_lib_collection = Collection()
stream_apps_lib_collection.add_task(build_stream_apps_lib, name="build")
stream_apps_lib_collection.add_task(test, name="test")

from invoke import Collection, task

from .docker_compose import docker_compose_command, up


@task()
def build_docker_image(ctx):
    docker_compose_command(ctx, "build reddit-connector")


@task()
def run_connector(ctx):
    up(ctx, extra_services=["reddit-connector"])


reddit_collection = Collection()
reddit_collection.add_task(build_docker_image, name="build")
reddit_collection.add_task(run_connector, name="run")

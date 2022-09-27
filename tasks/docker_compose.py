from invoke import task, Collection
from .utils import docker_compose_command


@task
def up(ctx, version=2):
    docker_compose_command(ctx, "up -d", version=version)


@task
def down(ctx, volumes=False, version=2):
    docker_compose_command(ctx, f"down {'--volumes' if volumes else ''}", version=version)


@task
def command(ctx, cmd, version=2):
    docker_compose_command(ctx, cmd, version=version)


docker_compose_collection = Collection()
docker_compose_collection.add_task(up, name="up")
docker_compose_collection.add_task(down, name="down")
docker_compose_collection.add_task(command, name="cmd")

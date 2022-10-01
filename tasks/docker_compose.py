from invoke import Collection, task

from .utils import docker_compose_command


@task
def up(ctx, version=2, extra_services=None):
    services = [
        "hive-metastore",
        "broker",
        "schema-registry",
        "control-center",
        "minio",
    ]
    if extra_services is not None:
        services += extra_services
    docker_compose_command(ctx, f"up -d {' '.join(services)}", version=version)


@task
def down(ctx, volumes=False, version=2):
    docker_compose_command(
        ctx, f"down {'--volumes' if volumes else ''}", version=version
    )


@task
def command(ctx, cmd, version=2):
    docker_compose_command(ctx, cmd, version=version)


docker_compose_collection = Collection()
docker_compose_collection.add_task(up, name="up")
docker_compose_collection.add_task(down, name="down")
docker_compose_collection.add_task(command, name="cmd")

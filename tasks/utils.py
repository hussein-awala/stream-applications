from pathlib import Path


def gradle_task(ctx, project_name, task, args):
    root_project_path = get_project_dir(ctx).joinpath("stream-apps")
    ctx.run(f"cd {root_project_path} && ./gradlew :{project_name}:{task} {args}")


def build_java_lib(ctx, project_name, args=""):
    gradle_task(ctx=ctx, project_name=project_name, task="shadowJar", args=args)


def clean_java_lib(ctx, project_name, args=""):
    gradle_task(ctx=ctx, project_name=project_name, task="clean", args=args)


def run_java_lib(ctx, project_name, main_class):
    root_project_path = get_project_dir(ctx).joinpath("stream-apps")
    jar_path = root_project_path.joinpath(
        project_name, "build/libs", f"{project_name}-all.jar"
    )
    ctx.run(f"java -cp {jar_path} {main_class}")


def get_project_dir(ctx) -> Path:
    return Path(ctx.run("git rev-parse --show-toplevel").stdout.strip())


def docker_compose_command(ctx, command, version=2):
    docker_compose_folder_path = get_project_dir(ctx).joinpath("docker-compose")
    compose_files = ["kafka", "minio", "hive"]
    compose_cmd = "docker compose" if version == 2 else "docker-compose"
    ctx.run(
        f"{compose_cmd} {' '.join([f'-f {docker_compose_folder_path.joinpath(file)}.yml' for file in compose_files])}"
        f" {command}"
    )

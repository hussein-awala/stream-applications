from os.path import join


def build_java_lib(ctx, project_name):
    root_project_path = join(get_project_dir(ctx), "stream-apps")
    ctx.run(f"cd {root_project_path} && ./gradlew :{project_name}:shadowJar")


def clean_java_lib(ctx, project_name):
    root_project_path = join(get_project_dir(ctx), "stream-apps")
    ctx.run(f"cd {root_project_path} && ./gradlew :{project_name}:clean")


def run_java_lib(ctx, project_name, main_class):
    root_project_path = join(get_project_dir(ctx), "stream-apps")
    jar_path = join(root_project_path, project_name, f"build/libs/{project_name}-all.jar")
    ctx.run(f"java -cp {jar_path} {main_class}")


def get_project_dir(ctx):
    return ctx.run("git rev-parse --show-toplevel").stdout.strip()

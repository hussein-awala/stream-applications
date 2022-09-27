from invoke import Collection

from .kafka_producers import kafka_collection
from .build import build_collection
from .clean import clean_collection
from .docker_compose import docker_compose_collection

ns = Collection()
ns.add_collection(kafka_collection, name="kafka_producers")
ns.add_collection(build_collection, name="build")
ns.add_collection(clean_collection, name="clean")
ns.add_collection(docker_compose_collection, name="compose")

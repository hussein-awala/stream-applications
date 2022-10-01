from invoke import Collection

from .build import build_collection
from .clean import clean_collection
from .docker_compose import docker_compose_collection
from .kafka_producers import kafka_collection
from .kstreams import kstreams_collection
from .reddit import reddit_collection
from .spark_streaming import spark_streaming_collection

ns = Collection()
ns.add_collection(kafka_collection, name="kafka_producers")
ns.add_collection(build_collection, name="build")
ns.add_collection(clean_collection, name="clean")
ns.add_collection(docker_compose_collection, name="compose")
ns.add_collection(kstreams_collection, name="kstreams")
ns.add_collection(spark_streaming_collection, name="spark_streaming")
ns.add_collection(reddit_collection, name="reddit")

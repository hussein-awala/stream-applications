from invoke import Collection

from .kafka import kafka_collection
from .build import build_collection
from .clean import clean_collection

ns = Collection()
ns.add_collection(kafka_collection, name="kafka")
ns.add_collection(build_collection, name="build")
ns.add_collection(clean_collection, name="clean")
# Stream processing demo

## Requirements
- java [jdk](https://www.oracle.com/fr/java/technologies/javase/jdk11-archive-downloads.html) 11
- [python](https://www.python.org/downloads/) 3.8+
- [docker](https://docs.docker.com/engine/install/) and [docker compose](https://docs.docker.com/compose/install/)
- install the python requirements:
```shell
pip install -r requirememts.txt
```
## Docker compose stack

### Description
This project uses different services, to simplify the testing and the configuration of these services, some docker
compose files are used to run them:
- Minio: A S3 service used for storage (used as hadoop DFS for spark jobs, and as a sink for some kafka connectors)
- Hive: A metastore use by spark to store and manage the metadata of persistent relational entities (DBs, tables, ...),
and it uses a postgres DB as a backend storage.
- Kafka stack with following services:
  - Zookeeper: used to track the status of nodes in the Kafka cluster and maintain a list of Kafka topics and messages
  - broker: it's a kafka node used to store the messages log, it handles the client requests (produce, consume, ...)
  - schema registry: a service used to store avro schema in order to use them later to deserialize the messages
  - control center: a UI used to manage the kafka cluster
  - create reddit topics: a simple container used to init the cluster by creating the topics we need for our app
  - reddit connector: a kafka connector running in standalone mode to stream reddit posts and comments

### Running
```shell
invoke compose.up
```

### Reddit kafka connector
A kafka connector used to call the reddit API and read posts and comments for a list of subreddits, then write them to
two kafka topics. [Here](reddit/README.md) is the documentation of this service.

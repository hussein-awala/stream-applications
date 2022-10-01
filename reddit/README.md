# Reddit Kafka Connector
In this project, a kafka connector is used to call the reddit API and read posts and comments for a list of subreddits,
then write them to two separate kafka topics. This connector is lunched in a docker container using the
[standalone mode](https://docs.confluent.io/kafka-connectors/self-managed/userguide.html#standalone-mode), and it's
configured using a [reddit-connector.properties](reddit-kafka-connect/reddit-connector.properties) file.

## Creating kafka topics
When the kafka producer doesn't find a kafka topic, the topic will be created with the default configurations
(1 partition and 1 replication factor). It is recommended to create the topics manually, in order to better configure
them.
A docker container is used to create the topic with 6 partitions each, and it's added as a dependency for the connector
container, to make sure that the topics are created before starting the message producing.

## Running the reddit kafka connector
A task invoke is developed to run the reddit kafka connector container, and it will lunch the main docker stack services
if they are not running.
```shell
invoke reddit.run
```

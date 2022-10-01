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

## Storing reddit data in Hudi tables on S3
In this project, reddit posts and comments are exported to S3 (datalake/lakehouse) and stored as
[apache Hudi](https://hudi.apache.org/) tables, this export allows us to query the data using spark (and other tools),
in order to apply some OLAP queries on this data.

### Run the spark structured streaming app
```shell
# Build spark-streaming module
invoke spark-streaming.build
# Run kafka-to-hudi main to export reddit-posts topic to Hudi table reddit_posts
inv spark-streaming.kafka-to-hudi --args="-topicName reddit-posts -hudiTableName reddit_posts -hudiKeys id -hudiPrecombineKey kafka_timestamp -partitionsKeys subreddit -trigger 120 -schemaPath ${project_path}/reddit/spark-schema/reddit-posts-spark-schema.json"
# Run kafka-to-hudi main to export reddit-comments topic to Hudi table reddit_comments
inv spark-streaming.kafka-to-hudi --args="-topicName reddit-comments -hudiTableName reddit_comments -hudiKeys id -hudiPrecombineKey kafka_timestamp -partitionsKeys subreddit -trigger 120 -schemaPath ${project_path}/reddit/spark-schema/reddit-comments-spark-schema.json"
```

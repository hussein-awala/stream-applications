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
# Create reddit_db: hive_sync.auto_create_database seems doesn't work, we need to create the db manually
invoke spark-streaming.create-db --args="-hiveDatabaseName reddit_db"
# Run kafka-to-hudi main to export reddit-posts topic to Hudi table reddit_posts
invoke spark-streaming.kafka-to-hudi --args="-topicName reddit-posts -hiveDatabaseName reddit_db -hudiTableName reddit_posts -hudiKeys id -hudiPrecombineKey kafka_timestamp -partitionsKeys subreddit -trigger 120 -schemaPath ${project_path}/reddit/spark-schema/reddit-posts-spark-schema.json"
# Run kafka-to-hudi main to export reddit-comments topic to Hudi table reddit_comments
invoke spark-streaming.kafka-to-hudi --args="-topicName reddit-comments -hiveDatabaseName reddit_db -hudiTableName reddit_comments -hudiKeys id -hudiPrecombineKey kafka_timestamp -partitionsKeys subreddit -trigger 120 -schemaPath ${project_path}/reddit/spark-schema/reddit-comments-spark-schema.json"
```

## Querying Spark SQL tables

### Requirements
Install pyspark
```shell
pip install pyspark==3.3.0
```

### Python script
```python
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

project_path = os.getenv("project_path")

spark_conf = (
    SparkConf()
    .setAppName("Reddit DB SQL")
    .setMaster("local[1]")
    .setAll([
        ("spark.jars", f"{project_path}/stream-apps/spark-streaming-apps/build/libs/spark-streaming-apps-all.jar"),
        ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        ("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A"),
        ("spark.hadoop.fs.s3a.path.style.access", "true"),
        ("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
        ("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"),
        ("spark.hadoop.fs.s3a.access.key", "minio_root"),
        ("spark.hadoop.fs.s3a.secret.key", "minio_pass"),
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"),
        ("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"),
        ("spark.sql.warehouse.dir", "s3a://spark/hive"),
        ("hive.metastore.uris", "thrift://localhost:9083"),
    ])
)
spark_session = (
    SparkSession.builder
    .config(conf=spark_conf)
    .enableHiveSupport()
    .getOrCreate()
)

spark_session.sql("SHOW TABLES IN reddit_db").collect()

df = spark_session.sql("SELECT * FROM reddit_db.reddit_posts")
df.show(10)
```

# Confluent Spark Avro

Spark UDFs to deserialize Avro messages with schemas stored in Schema Registry.
More details about Schema Registry on the [official website](http://docs.confluent.io/current/schema-registry/docs/intro.html#quickstart).

## Usages

We expect that you use it together with native [Spark Kafka Reader](https://spark.apache.org/docs/2.1.1/structured-streaming-kafka-integration.html#creating-a-kafka-source-batch).
```
val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "topic1")
    .load()

val utils = ConfluentSparkAvroUtils("http://schema-registry.my-company.com:8081")
val keyDeserializer = utils.deserializerForSubject("topic1-key")
val valueDeserialzer = utils.deserializerForSubject("topic1-value")

df.select(
    keyDeserializer(col("key").alias("key")),
    valueDeserializer(col("value").alias("value"))
).show(10)
```

## Build

The tool is designed to be used with Spark >= 2.0.2.
```
sbt assembly
ll target/scala-2.11/confluent-spark-avro-assembly-1.0.jar
```

## Testing

We haven't added unit tests, but you can test UDFs with the next command:
```
sbt "project confluent-spark-avro" "run kafka.host:9092 http://schema-registry.host:8081 kafka.topic"
```

## TODO
[ ] Spark UDFs to serialize messages.

## License

The project is licensed under the Apache 2 license.

package de.haw.tweetspace

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.SparkSession

object Recommender extends LazyLogging {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(AppConfig.value("app.name").get)
      .master(AppConfig.value("spark.master").get)
      .getOrCreate()

    val basePath = AppConfig.value("hdfs.uri").get + "/"
    val tweets = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_tweets")
      .select("twitter_user_id", "in_reply_to_twitter_user_id")
    val registrations = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_registrations")
      .select("twitter_user_id", "name")
    val joined = UserDataFunctions.join(tweets, registrations)
    val registryClient = new CachedSchemaRegistryClient(
      AppConfig.value("schema_registry.url").get,
      256)
    val kafkaProducerReady = UserDataFunctions.mapToKafkaProducerRecord(joined, registryClient)
    UserDataFunctions.publishToKafka(kafkaProducerReady)
  }
}

package de.haw.tweetspace

import com.typesafe.scalalogging.LazyLogging

object Recommender extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = HDFSHelper.spark
    val basePath = AppConfig.value("hdfs.uri").get + "/"
    val tweets = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_tweets")
      .select("twitter_user_id", "in_reply_to_twitter_user_id")
    val registrations = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_registrations")
      .select("twitter_user_id", "name")
    val joined = UserDataFunctions.join(tweets, registrations)

    val kafkaProducerReady = UserDataFunctions.mapToKafkaProducerRecord(joined)
    UserDataFunctions.publishToKafa(kafkaProducerReady)

    joined.show(10)
    logger.info("Successfully did stuff: " + joined.count)
  }
}

package de.haw.tweetspace

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

object Recommender extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = HDFSHelper.spark
    val basePath = "hdfs://namenode-0.namenode.aca534.svc.cluster.local:9000/"
    val tweets = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_tweets")
    val registrations = UserDataSet.load(spark, basePath + "gobblin-kafka-avro/job-output/user_registrations")
    val joined = UserDataFunctions.join(tweets, registrations)

    joined.show(10)
    logger.info("Successfully did stuff: " + joined.count)
  }
}

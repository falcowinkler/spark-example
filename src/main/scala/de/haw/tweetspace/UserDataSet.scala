package de.haw.tweetspace

import org.apache.spark.sql.{DataFrame, SparkSession}

 object UserDataSet {

  def load(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .format("com.databricks.spark.avro")
      .load(path)
  }

  def loadAll(sparkSession: SparkSession, basePath: String): DataFrame = {
    load(sparkSession, basePath + "/user_registrations").union(load(sparkSession, basePath + "/user_tweets"))
  }

}

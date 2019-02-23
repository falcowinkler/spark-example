package de.haw.tweetspace

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._

class UserDataFunctionsSpec extends FeatureSpec with Matchers with BeforeAndAfterAll {
  feature("Load data from avro files") {
    scenario("load user and registration data") {
      val spark: SparkSession = SparkSession.builder
        .appName("test")
        .master("local")
        .getOrCreate()

      TestTools.createUserData()

      val registrations: DataFrame = UserDataSet.load(spark, "src/test/resources/gobblin-kafka-avro/job-output/user_registrations/")
      registrations.columns.length shouldBe 6
      registrations.count() shouldBe 100
      registrations.drop("timestamp").head() shouldBe Row(
        150, "Klaus Peter", true, "en", "Klaus Peter mag Bausparverträge")

      val tweets: DataFrame = UserDataSet.load(spark, "src/test/resources/gobblin-kafka-avro/job-output/user_tweets")
      tweets.columns.length shouldBe 9
      tweets.count() shouldBe 100
      tweets.drop("timestamp").drop("created_at").head() shouldBe
        Row(150, 150, "A test tweet", "www.example.com", 149, 151, "en")
    }
  }

  override def afterAll(): Unit = {
    TestTools.clearUserData()
  }

}

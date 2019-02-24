package de.haw.tweetspace

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Matchers}

class UserDataFunctionsSpec extends FeatureSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder
    .appName("test")
    .master("local")
    .getOrCreate()
  import spark.implicits._


  feature("Join functions") {
    scenario("join tweets and registrations") {
      val testRegistrations =
        Seq((1L, "A", 10), (2L, "A", 5), (3L, "B", 56))
          .toDF("twitter_user_id", "username", "some_other_data")
      val testTweets = Seq((1L, "bla"), (2L, "blubb"), (3L, "foo"))
        .toDF("twitter_user_id", "tweetcontent")
      val expected =
        Seq((1L, "bla", "A", 10), (2L, "blubb", "A", 5), (3L, "foo", "B", 56))
          .toDF("twitter_user_id", "tweetcontent", "username", "some_other_data")
      val actual = UserDataFunctions.join(testTweets, testRegistrations)
      actual.schema shouldBe expected.schema
      actual.show()
      expected.intersect(actual).count() shouldBe 3 // All three rows were equal
    }
  }


  feature("creates an avro column ") {
    scenario("gets joined data frame and appends extra column with friend reccomendation avro record") {
      val testData =
        Seq((1L, 2L, "Klaus"), (1L, 4L, "Peter"),(3L, 5L, "Sigfried"))
          .toDF("twitter_user_id", "in_reply_to_twitter_user_id", "name")
      val expected = UserDataFunctions.mapToKafkaProducerRecord(testData, spark.sparkContext.broadcast(1))
      expected.schema.length shouldBe 2
    }
    scenario("in_reply is null") {
      val testData =
        Seq(
          (1L, null.asInstanceOf[java.lang.Long], "Klaus"),
          (1L, new java.lang.Long(123), "Peter"),
          (3L,  null.asInstanceOf[java.lang.Long], "Sigfried"))
          .toDF("twitter_user_id", "in_reply_to_twitter_user_id", "name")
      val expected = UserDataFunctions.mapToKafkaProducerRecord(testData, spark.sparkContext.broadcast(1))
      expected.schema.length shouldBe 2
    }
  }
}

package de.haw.tweetspace

import java.io.File

import de.haw.tweetspace.avro.{CustomerRegistration, CustomerTweet}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime

object TestTools {
  val avroBasePath = "src/test/resources/gobblin-kafka-avro/job-output/"

  def writeAvro[A <: SpecificRecord](testUserData: List[A], path: String) {
    val userDatumWriter = new SpecificDatumWriter[A]()
    val dataFileWriter = new DataFileWriter[A](userDatumWriter)
    dataFileWriter.create(
      testUserData.head.getSchema,
      new File(path))
    for (elem <- testUserData) {
      dataFileWriter.append(elem)
    }
    dataFileWriter.close()
  }

  def createTestTweetData(): List[CustomerTweet] = {
    var testUserData: List[CustomerTweet] = List()
    for (climbingNumber <- 100L to 199L) {
      val testTweet = new CustomerTweet(
        climbingNumber,
        new DateTime(123),
        300 - climbingNumber,
        "A test tweet",
        new DateTime(123),
        "www.example.com",
        299 - climbingNumber,
        climbingNumber + 1,
        "en")
      testUserData = testUserData :+ testTweet
    }
    testUserData
  }

  def createTestRegistrationData(): List[CustomerRegistration] = {
    var testUserData: List[CustomerRegistration] = List()
    for (climbingNumber <- 100L to 199L) {
      val testRegistration = new CustomerRegistration(
        climbingNumber,
        new DateTime(123),
        "Klaus Peter", true, "en",
        "Klaus Peter mag Bausparverträge")
      testUserData = testUserData :+ testRegistration
    }
    testUserData
  }

  def createUserData(): Unit = {
    val registrations = createTestRegistrationData()
    val tweets = createTestTweetData()

    writeAvro(registrations.drop(registrations.size / 2), avroBasePath + "user_registrations/part.job.123.avro")
    writeAvro(registrations.dropRight(registrations.size / 2), avroBasePath + "user_registrations/part.job.321.avro")
    writeAvro(tweets.drop(tweets.size / 2), avroBasePath + "user_tweets/part.job.123.avro")
    writeAvro(tweets.dropRight(tweets.size / 2), avroBasePath + "user_tweets/part.job.321.avro")
  }

  def clearUserData(): Unit = {
    FileUtils.deleteQuietly(new File(avroBasePath + "user_registrations/part.job.123.avro"))
    FileUtils.deleteQuietly(new File(avroBasePath + "user_registrations/part.job.321.avro"))
    FileUtils.deleteQuietly(new File(avroBasePath + "user_tweets/part.job.123.avro"))
    FileUtils.deleteQuietly(new File(avroBasePath + "user_tweets/part.job.321.avro"))
  }

}

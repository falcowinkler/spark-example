package de.haw.tweetspace

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import de.haw.tweetspace.avro.FriendReccomendation
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime

object UserDataFunctions {
  private val MAGIC_BYTE = 0
  val idSize = 4

  def join(tweetData: DataFrame, registrationData: DataFrame): DataFrame = {
    tweetData.join(registrationData, "twitter_user_id")
  }

  // There are cool libraries for this but only for scala 2.11 :(
  // And i want to stay on 2.12 because i am to lazy to rebuild the spark docker image
  def toAvro(reccomendation: FriendReccomendation, id: Int): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(MAGIC_BYTE)
    out.write(ByteBuffer.allocate(idSize).putInt(id).array)
    val userDatumWriter = new SpecificDatumWriter[FriendReccomendation](classOf[FriendReccomendation])
    val dataFileWriter = new DataFileWriter[FriendReccomendation](userDatumWriter)
    dataFileWriter.create(FriendReccomendation.getClassSchema, out)
    dataFileWriter.append(reccomendation)
    dataFileWriter.close()
    out.toByteArray
  }

  def mapToKafkaProducerRecord(joinedDataFrame: DataFrame, registryClient: SchemaRegistryClient): DataFrame = {
    val id = registryClient.register("friend_recommendations-value", FriendReccomendation.getClassSchema)
    // Spark needs a row encoder for data serialization.
    // Here we explicitly state that we want the map function to produce a BinaryType
    var structType = new StructType
    structType = structType.add("value", DataTypes.BinaryType, nullable = false)
    val encoder = RowEncoder(structType)

    def getValues(row: Row): Row = {
      val specificRecord = FriendReccomendation.newBuilder
        .setReccomendationReceiverTwitterUserId(row.getLong(0))
        .setReccomendedTwitterUserId(row.getLong(1))
        .setReccomendationReceiverName(row.getString(2))
        .setMatchPercentage(0.5F)
        .setTimestamp(DateTime.now()).build()
      Row(toAvro(specificRecord, id))
    }

    joinedDataFrame
      .filter(col("in_reply_to_twitter_user_id").isNotNull)
      .map(getValues, encoder)
      // Key column is the kafka key, which is used for partitioning,
      // Not necessarily needed so we just specify a constant here
      .withColumn("key", lit(""))
  }

  def publishToKafka(dataFrame: DataFrame): Unit = {
    dataFrame.write.
      format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.value("kafka.bootstrap_servers").get)
      .option("topic", "friend_recommendations")
      .save()
  }

}

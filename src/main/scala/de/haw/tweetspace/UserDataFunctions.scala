package de.haw.tweetspace

import de.haw.tweetspace.avro.FriendReccomendation
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}

object UserDataFunctions {
  def join(tweetData: DataFrame, registrationData: DataFrame): DataFrame = {
    tweetData.join(registrationData, "twitter_user_id")
  }

  def mapToKafkaProducerRecord(joinedDataFrame: DataFrame): DataFrame = {
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
      // TODO: caching?
      val registryClient = new CachedSchemaRegistryClient(
        AppConfig.value("schema_registry.url").get,
        256)
      val serializer = new KafkaAvroSerializer(registryClient)
      registryClient.register("friend_recommendations-value", specificRecord.getSchema)
      Row(serializer.serialize("friend_recommendations", specificRecord))
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

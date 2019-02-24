package de.haw.tweetspace

import de.haw.tweetspace.avro.FriendReccomendation
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime
import org.apache.spark.sql.avro._

object UserDataFunctions {
  def join(tweetData: DataFrame, registrationData: DataFrame): DataFrame = {
    tweetData.join(registrationData, "twitter_user_id")
  }

  def mapToKafkaProducerRecord(joinedDataFrame: DataFrame): DataFrame = {
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    import org.apache.spark.sql.types.{DataTypes, StructType}
    var structType = new StructType
    structType = structType.add("value", DataTypes.BinaryType, false)
    val encoder = RowEncoder(structType)

    def getValues(row: Row): Row = {
      val specificRecord = FriendReccomendation.newBuilder
        .setReccomendationReceiverTwitterUserId(row.getLong(0))
        .setReccomendedTwitterUserId(row.getLong(1))
        .setReccomendationReceiverName(row.getString(2))
        .setMatchPercentage(0.5F)
        .setTimestamp(DateTime.now()).build()
      val registryClient = new CachedSchemaRegistryClient(
        AppConfig.value("schema_registry.url").get,
        10000)
      val serializer = new KafkaAvroSerializer(registryClient)
      registryClient.register("friend_recommendations-value", specificRecord.getSchema)
      Row(serializer.serialize("friend_recommendations", specificRecord))
    }
    joinedDataFrame.map(getValues, encoder)
      .withColumn("key", lit(""))
  }

  def publishToKafa(dataFrame: DataFrame): Unit = {
    dataFrame.write.
      format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.value("kafka.bootstrap_servers").get)
      .option("topic", "friend_recommendations")
      .save()
  }
}

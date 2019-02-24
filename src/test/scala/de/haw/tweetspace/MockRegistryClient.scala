package de.haw.tweetspace

import java.util

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema

class MockRegistryClient extends SchemaRegistryClient{
  override def register(s: String, schema: Schema): Int = 123

  override def getByID(i: Int): Schema = ???

  override def getById(i: Int): Schema = ???

  override def getBySubjectAndID(s: String, i: Int): Schema = ???

  override def getBySubjectAndId(s: String, i: Int): Schema = ???

  override def getLatestSchemaMetadata(s: String): SchemaMetadata = ???

  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = ???

  override def getVersion(s: String, schema: Schema): Int = ???

  override def getAllVersions(s: String): util.List[Integer] = ???

  override def testCompatibility(s: String, schema: Schema): Boolean = ???

  override def updateCompatibility(s: String, s1: String): String = ???

  override def getCompatibility(s: String): String = ???

  override def getAllSubjects: util.Collection[String] = ???

  override def getId(s: String, schema: Schema): Int = ???

  override def deleteSubject(s: String): util.List[Integer] = ???

  override def deleteSubject(map: util.Map[String, String], s: String): util.List[Integer] = ???

  override def deleteSchemaVersion(s: String, s1: String): Integer = ???

  override def deleteSchemaVersion(map: util.Map[String, String], s: String, s1: String): Integer = ???
}

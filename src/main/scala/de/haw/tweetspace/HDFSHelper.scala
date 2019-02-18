package de.haw.tweetspace


import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object HDFSHelper {

  val spark: SparkSession = SparkSession.builder
    .appName(AppConfig.value("app.name").get)
    .master(AppConfig.value("spark.master").get)
    .getOrCreate()

  def defaultFS(): FileSystem = {
    val conf = new Configuration()
    FileSystem.get(new URI(AppConfig.value("hdfs.uri").get),
      conf)
  }

  def createOutputIfNotAvailable(fileSystem: FileSystem, pathName: String): Boolean = {
    val path = new Path(pathName)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    } else {
      false
    }
  }
}
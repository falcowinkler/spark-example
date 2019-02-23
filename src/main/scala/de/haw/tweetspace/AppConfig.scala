package de.haw.tweetspace

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  private val defaultConfig: Config = ConfigFactory.load("default.conf")

  def value(property: String): Option[String] = {
    val environmentValue = System.getenv(toEnvName(property))
    if (environmentValue != null) {
      return Option(environmentValue)
    }

    var defaultValue = fromConfigFile(defaultConfig, property)

    if (System.getenv("CONFIG_FILE") != null) {
      val config = ConfigFactory.load(System.getenv("CONFIG_FILE"))
      val configFileValue = fromConfigFile(config, property)
      if (configFileValue.isDefined) {
        defaultValue = configFileValue
      }
    }

    defaultValue
  }

  private def fromConfigFile(config: Config, property: String): Option[String] = {
    try {
      Option(config.getString(property))
    } catch {
      case _: Exception => None
    }
  }

  private def toEnvName(property: String): String = {
    property.toUpperCase().replace("-", "_").replace(".", "_")
  }
}
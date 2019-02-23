package de.haw.tweetspace

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

object UserDataFunctions {
  def join(tweetData: DataFrame, registrationData: DataFrame): DataFrame = {
    tweetData.join(registrationData, "twitter_user_id")
  }
}

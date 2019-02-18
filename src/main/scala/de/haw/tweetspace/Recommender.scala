package de.haw.tweetspace

import com.typesafe.scalalogging.LazyLogging

object Recommender extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val fs = HDFSHelper.defaultFS()
    logger.info("fs initialized: " + fs)
    fs.close()
  }
}

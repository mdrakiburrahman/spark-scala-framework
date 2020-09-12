package com.sparky.attractionsrecommender.common.feeds.io

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Actions on a CSV feed.
  */
class CsvFeedIO(spark: SparkSession) extends FeedIO[DataFrame] {

  // Write CSV to path.
  override def write(update: DataFrame, path: String): Unit = {
    update.write.csv(path)
  }

  // Read CSV from path.
  override def read(path: String): DataFrame = spark.read.csv(path)
}

/**
  * Definition of a CSV feed.
  */
object CsvFeedIO {

  def apply(spark: SparkSession): CsvFeedIO = new CsvFeedIO(spark)
}

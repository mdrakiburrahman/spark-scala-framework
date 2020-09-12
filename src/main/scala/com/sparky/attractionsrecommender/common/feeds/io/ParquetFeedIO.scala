package com.sparky.attractionsrecommender.common.feeds.io

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Actions on a Parquet feed.
  */
class ParquetFeedIO(spark: SparkSession) extends FeedIO[DataFrame] {

  // Write Parquet to path.
  override def write(update: DataFrame, path: String): Unit = {
    update.write.parquet(path)
  }

  // Read Parquet from path.
  override def read(path: String): DataFrame = spark.read.parquet(path)
}

/**
  * Definition of a Parquet feed.
  */
object ParquetFeedIO {

  def apply(spark: SparkSession): ParquetFeedIO = new ParquetFeedIO(spark)
}

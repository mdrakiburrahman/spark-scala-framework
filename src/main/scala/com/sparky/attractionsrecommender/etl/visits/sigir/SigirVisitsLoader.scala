package com.sparky.attractionsrecommender.etl.visits.sigir

import com.sparky.attractionsrecommender.common.config.EnvironmentConfiguration
import com.sparky.attractionsrecommender.common.feeds.visits.VisitsColumnNames
import com.sparky.attractionsrecommender.etl.visits.VisitsLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.count

/**
  * Implementation of a visits loader that reads and transforms Sigir 17 data.
  */
class SigirVisitsLoader(reader: SigirVisitsReader, transformer: SigirVisitsTransformer)
  extends VisitsLoader {
  
  // Extend Loader trait and override generic load behavior.
  override def load(): DataFrame = {
    // Source specific read behavior, defined below.
    val rawVisits = reader.read()
    // Perform source specific transformations on raw dataset as data prep for pipeline.
    transformer.transform(rawVisits)
  }
}

object SigirVisitsLoader {
  
  // Instantiate class.
  def apply(spark: SparkSession): SigirVisitsLoader =
    new SigirVisitsLoader(new SigirVisitsReader(spark), new SigirVisitsTransformer)
}

class SigirVisitsReader(spark: SparkSession) {
  
  // Read raw dataset from source (e.g. Parsed Zone).
  def read(): DataFrame =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("sep", ";")
      .csv(s"${EnvironmentConfiguration().SigirRawDataPath}userVisits-sigir17/*.csv")
}

class SigirVisitsTransformer {

  // Transform raw DataFrame as the particular source demands.
  def transform(rawVisits: DataFrame): DataFrame =
    rawVisits
      .withColumnRenamed("nsid", VisitsColumnNames.UserId)
      .withColumnRenamed("poiID", VisitsColumnNames.AttractionId)
      .groupBy(VisitsColumnNames.UserId, VisitsColumnNames.AttractionId)
      .agg(count("*").as(VisitsColumnNames.VisitsCount))
      .select(
        VisitsColumnNames.UserId,
        VisitsColumnNames.AttractionId,
        VisitsColumnNames.VisitsCount)
}

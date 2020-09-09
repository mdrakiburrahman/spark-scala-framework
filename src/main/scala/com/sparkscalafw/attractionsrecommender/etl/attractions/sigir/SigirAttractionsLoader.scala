package com.sparkscalafw.attractionsrecommender.etl.attractions.sigir

import com.sparkscalafw.attractionsrecommender.common.config.EnvironmentConfiguration
import com.sparkscalafw.attractionsrecommender.common.feeds.attractions.AttractionsColumnNames
import com.sparkscalafw.attractionsrecommender.etl.attractions.AttractionsLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Implementation of an attractions loader that reads and transforms Sigir 17 data.
  */
class SigirAttractionsLoader(
    reader: SigirAttractionsReader,
    transformer: SigirAttractionsTransformer)
  extends AttractionsLoader {

  override def load(): DataFrame = {
    val rawAttractions = reader.read()
    transformer.transform(rawAttractions)
  }
}

object SigirAttractionsLoader {

  def apply(spark: SparkSession): SigirAttractionsLoader =
    new SigirAttractionsLoader(new SigirAttractionsReader(spark), new SigirAttractionsTransformer)
}

class SigirAttractionsReader(spark: SparkSession) {

  def read(): DataFrame =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("sep", ";")
      .csv(s"${EnvironmentConfiguration().SigirRawDataPath}poiList-sigir17/*.csv")
}

class SigirAttractionsTransformer {

  def transform(rawAttractions: DataFrame): DataFrame =
    rawAttractions
      .withColumnRenamed("poiID", AttractionsColumnNames.Id)
      .withColumnRenamed("poiName", AttractionsColumnNames.Name)
      .select(AttractionsColumnNames.Id, AttractionsColumnNames.Name)
}

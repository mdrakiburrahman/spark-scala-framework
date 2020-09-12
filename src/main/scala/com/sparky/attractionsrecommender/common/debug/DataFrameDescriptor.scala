package com.sparky.attractionsrecommender.common.debug

import com.sparky.attractionsrecommender.common.config.EnvironmentConfiguration
import org.apache.spark.sql.DataFrame

trait DataFrameDescriptor {

  /**
    * Describes the given data frame.
    *
    * @param df the data frame to be described.
    */
  def describe(df: DataFrame): Unit
}

object DataFrameDescriptor {

  /**
    * @return a debug descriptor is debug is enabled, a no-op descriptor otherwise.
    */
  def apply(): DataFrameDescriptor =
    if (EnvironmentConfiguration().DebugEnabled) {
      new DebugDataFrameDescriptor
    } else {
      new NoOpDataFrameDescriptor
    }
}

/**
  * Override the describe command to perform no actions.
  * Useful for Production Pipelines containing PII data, or large datasets that are expensive to materialize on Spark Driver.
  */
class NoOpDataFrameDescriptor extends DataFrameDescriptor {

  override def describe(df: DataFrame): Unit = {}
}

/**
  * Override the describe command to show the dataset, as well as the statistics.
  * Can be expanded upon to formulate an aligned set of actions, including usage of external libraries.
  */
class DebugDataFrameDescriptor extends DataFrameDescriptor {

  override def describe(df: DataFrame): Unit = {
    df.show()
    df.describe().show()
  }
}

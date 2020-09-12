package com.sparky.attractionsrecommender.common.spark

import com.sparky.attractionsrecommender.common.config.EnvironmentConfiguration
import org.apache.spark.sql.SparkSession

/**
  * Utility object managing the creation and provisioning of a spark session.
  */
object SparkSessionManager {

  // Creates a public SparkSession object that gets executed on first access (i.e. lazily).
  // Depending on Environment Configuration, able to run locally or in Cluster.
  lazy val session: SparkSession = if (EnvironmentConfiguration().LocalSpark) {
    local
  } else {
    provided
  }

  // Creates a private local SparkSession object that gets executed on first access.
  // Used to create a new Spark session with a distinguished App Name on the local machine.
  private lazy val local: SparkSession = SparkSession
    .builder()
    .appName("attractions-recommender")
    .master(s"local[5]")
    .getOrCreate()
  
  // Retrieves the SparkSession object from the Cluster.
  // In environments that this has been created upfront (e.g. Databricks), uses the builder to get an existing session.
  private lazy val provided: SparkSession = SparkSession.builder().getOrCreate()
}

package com.sparkscalafw.attractionsrecommender.etl

import com.sparkscalafw.attractionsrecommender.common.feeds.attractions.AttractionsFeed
import com.sparkscalafw.attractionsrecommender.common.feeds.visits.VisitsFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.etl.attractions.sigir.SigirAttractionsLoader
import com.sparkscalafw.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparkscalafw.attractionsrecommender.etl.visits.sigir.SigirVisitsLoader

/**
  * Implementation of an ETL training pipeline driver that prepares and transforms the datasets.
  * Transformed datasets are persisted to Feeds.
  */
object EtlDriver extends App {

  // Initialize SparkSession from Common library
  val spark = SparkSessionManager.session

  // Initialize Vists Feed
  val visits = SigirVisitsLoader(spark).load()
  DataFrameDescriptor().describe(visits)
  VisitsFeed(spark).put(visits)

  // Initialize Attractions Feed
  val attractions = SigirAttractionsLoader(spark).load()
  DataFrameDescriptor().describe(attractions)
  AttractionsFeed(spark).put(attractions)
}

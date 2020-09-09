package com.sparkscalafw.attractionsrecommender.etl

import com.sparkscalafw.attractionsrecommender.common.feeds.attractions.AttractionsFeed
import com.sparkscalafw.attractionsrecommender.common.feeds.visits.VisitsFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.etl.attractions.sigir.SigirAttractionsLoader
import com.sparkscalafw.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparkscalafw.attractionsrecommender.etl.visits.sigir.SigirVisitsLoader

object EtlDriver extends App {

  val spark = SparkSessionManager.session

  val visits = SigirVisitsLoader(spark).load()
  DataFrameDescriptor().describe(visits)
  VisitsFeed(spark).put(visits)

  val attractions = SigirAttractionsLoader(spark).load()
  DataFrameDescriptor().describe(attractions)
  AttractionsFeed(spark).put(attractions)
}

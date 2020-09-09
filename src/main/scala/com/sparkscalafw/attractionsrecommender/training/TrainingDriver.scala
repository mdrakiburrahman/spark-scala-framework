package com.sparkscalafw.attractionsrecommender.training

import com.sparkscalafw.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparkscalafw.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparkscalafw.attractionsrecommender.common.feeds.visits.VisitsFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.training.spark.AlsAttractionsRecommenderTrainer

object TrainingDriver extends App {

  val spark = SparkSessionManager.session
  val data = VisitsFeed(spark).get()
  DataFrameDescriptor().describe(data)
  val model = AlsAttractionsRecommenderTrainer().train(data)
  AlsModelFeed().put(model)
}

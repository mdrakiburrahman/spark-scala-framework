package com.sparkscalafw.attractionsrecommender.training

import com.sparkscalafw.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparkscalafw.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.etl.visits.sigir.SigirVisitsLoader
import com.sparkscalafw.attractionsrecommender.training.spark.AlsAttractionsRecommenderTrainer

object EtlAndTrainingDriver extends App {

  val data = SigirVisitsLoader(SparkSessionManager.session).load()
  DataFrameDescriptor().describe(data)
  val model = AlsAttractionsRecommenderTrainer().train(data)
  AlsModelFeed().put(model)
}

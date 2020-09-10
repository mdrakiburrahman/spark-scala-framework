package com.sparkscalafw.attractionsrecommender.training

import com.sparkscalafw.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparkscalafw.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.etl.visits.sigir.SigirVisitsLoader
import com.sparkscalafw.attractionsrecommender.training.spark.AlsAttractionsRecommenderTrainer

/**
  * Implementation of a model training pipeline driver that reads directly from raw source.
  * This is useful if the ETL portion of training isn't costly, and/or we want to rerun it for recalculating a feed.
  */
object EtlAndTrainingDriver extends App {
 
  // Read data from source, transform into a DataFrame.
  val data = SigirVisitsLoader(SparkSessionManager.session).load()
  
  // Display statistics on training data.
  DataFrameDescriptor().describe(data)

  // Train ALS model by fitting to training data.
  val model = AlsAttractionsRecommenderTrainer().train(data)

  // Persist Model Predictions as Feed (i.e. Batch Prediction).
  AlsModelFeed().put(model)
}

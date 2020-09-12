package com.sparky.attractionsrecommender.training

import com.sparky.attractionsrecommender.common.debug.DataFrameDescriptor
import com.sparky.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparky.attractionsrecommender.common.feeds.visits.VisitsFeed
import com.sparky.attractionsrecommender.common.spark.SparkSessionManager
import com.sparky.attractionsrecommender.training.spark.AlsAttractionsRecommenderTrainer

/**
  * Implementation of a model training pipeline driver that reads transformed data from a feed.
  * This is useful if the ETL portion of training is time consuming, and we don't want to rerun it for model retraining.
  */
object TrainingDriver extends App {

  // Initialize SparkSession from Common library.
  val spark = SparkSessionManager.session

  // Read Data from Vists Feed.
  val data = VisitsFeed(spark).get()

  // Display statistics on training data.
  DataFrameDescriptor().describe(data)

  // Train ALS model by fitting to training data.
  val model = AlsAttractionsRecommenderTrainer().train(data)
  
  // Persist Model Predictions as Feed (i.e. Batch Prediction).
  AlsModelFeed().put(model)
}

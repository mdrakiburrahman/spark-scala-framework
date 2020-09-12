package com.sparky.attractionsrecommender

import com.sparky.attractionsrecommender.common.spark.SparkSessionManager
import com.sparky.attractionsrecommender.etl.attractions.sigir.SigirAttractionsLoader
import com.sparky.attractionsrecommender.etl.visits.sigir.SigirVisitsLoader
import com.sparky.attractionsrecommender.serving.recommender.spark.AlsAttractionsRecommender
import com.sparky.attractionsrecommender.training.spark.AlsAttractionsRecommenderTrainer

/**
  * This is an alternate implementation of FileSystemDriver.scala, without us using the Drivers or Feeds at all.
  * Rather, we are mixing and matching the different classes and traits to run the entire pipeline via Spark DataFrames - rather than Feeds.
  * In other words, we are running everything on Spark Memory.
  * We note that this doesn't allow us to decouple the key steps of our pipeline.
  */
object InMemoryDriver extends App {

  val spark = SparkSessionManager.session
  val visits = SigirVisitsLoader(spark).load()
  val attractions = SigirAttractionsLoader(spark).load()
  val model = AlsAttractionsRecommenderTrainer().train(visits)
  val recommender = new AlsAttractionsRecommender(model, spark)
  val users = Seq("10004778@N07")
  val recommendations = users.map(userId => (userId, recommender.recommend(userId)))
  println(recommendations)
}

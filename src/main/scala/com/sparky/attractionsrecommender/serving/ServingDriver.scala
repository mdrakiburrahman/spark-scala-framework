package com.sparky.attractionsrecommender.serving

import com.sparky.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparky.attractionsrecommender.common.spark.SparkSessionManager
import com.sparky.attractionsrecommender.serving.recommender.mapping.MappingAttractionsRecommender
import com.sparky.attractionsrecommender.serving.recommender.spark.AlsAttractionsRecommender
import com.sparky.attractionsrecommender.serving.recommender.paneling.PaneledAttractionsRecommender

/**
  * Implementation of a model serving pipeline driver that takes a list of users as the input for the prediction serving.
  * 
  * The idea here is:
  * A: For the first half of our audience, we return a static, albeit attractive, recmmendation (to watch Ignite!). This is the control.
  * B: For the second half of our audience, we will use the previously trained model to serve recommendations.
  * This will be our A/B test, rather than picking one recommender over the other.
  */
object ServingDriver extends App {

  // Initialize iterable seq with user ID features.
  val UsersToPredict = Seq("10004778@N07", "101445497@N05", "10295241@N02")

  // Initialize SparkSession from common library.
  val spark = SparkSessionManager.session

  // Initialize Recommender Trait with ALS model output feed.
  val alsRecommender = AlsAttractionsRecommender(AlsModelFeed().get(), spark)

  // A: Return a static recommendation for first half.
  val mappingsRecommender = new MappingAttractionsRecommender(
    UsersToPredict
      .map(userId => userId -> Seq("Attractions?! Go watch Microsoft Ignite on September 22nd!"))
      .toMap)
  
  // B: Return a paneled recommendation as per the trained ALS model.
  val paneledRecommender = new PaneledAttractionsRecommender(
    Map(0 -> alsRecommender, 1 -> mappingsRecommender))

  // Print recommendations in console.
  val recommendations = UsersToPredict.map(userId => (userId, paneledRecommender.recommend(userId)))
  println(recommendations)
}

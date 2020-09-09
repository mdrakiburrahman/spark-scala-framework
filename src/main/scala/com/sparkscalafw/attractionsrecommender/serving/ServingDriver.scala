package com.sparkscalafw.attractionsrecommender.serving

import com.sparkscalafw.attractionsrecommender.common.feeds.alsmodel.AlsModelFeed
import com.sparkscalafw.attractionsrecommender.common.spark.SparkSessionManager
import com.sparkscalafw.attractionsrecommender.serving.recommender.mapping.MappingAttractionsRecommender
import com.sparkscalafw.attractionsrecommender.serving.recommender.spark.AlsAttractionsRecommender
import com.sparkscalafw.attractionsrecommender.serving.recommender.paneling.PaneledAttractionsRecommender

object ServingDriver extends App {

  val UsersToPredict = Seq("10004778@N07", "101445497@N05", "10295241@N02")

  val spark = SparkSessionManager.session
  val alsRecommender = AlsAttractionsRecommender(AlsModelFeed().get(), spark)
  val mappingsRecommender = new MappingAttractionsRecommender(
    UsersToPredict
      .map(userId => userId -> Seq("Attractions?! Go watch the Spark+AI Summit!"))
      .toMap)
  val paneledRecommender = new PaneledAttractionsRecommender(
    Map(0 -> alsRecommender, 1 -> mappingsRecommender))

  val recommendations = UsersToPredict.map(userId => (userId, paneledRecommender.recommend(userId)))
  println(recommendations)
}

package com.sparkscalafw.attractionsrecommender.serving.recommender.mapping

import com.sparkscalafw.attractionsrecommender.serving.recommender.AttractionsRecommender

/**
  * Extends Attraction Recommender and overwrites the recommend method.
  * Receives and produces a static mapping from users to recommendations for serving.
  */
class MappingAttractionsRecommender(val mappings: Map[String, Seq[String]])
  extends AttractionsRecommender {

  override def recommend(user: String): Seq[String] = mappings(user)
}

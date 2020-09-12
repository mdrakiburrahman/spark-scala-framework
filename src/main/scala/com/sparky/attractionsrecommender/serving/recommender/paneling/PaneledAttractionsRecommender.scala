package com.sparky.attractionsrecommender.serving.recommender.paneling

import java.util.Calendar

import scala.util.hashing.MurmurHash3

import com.sparky.attractionsrecommender.serving.recommender.AttractionsRecommender

/**
  * Different decorations or permutations of the reccomender service. Allows for multiple
  * renditions of the recommender in one single service - allows us to determine under which panel
  * a User falls, and which reccomendation to use. Useful for A/B testing multiple model feeds.
  *
  * We do this by leveraging a hash of the User ID suffix with a salt on every prediction, to determine
  * which reccomendation the user should be exposed to.
  */
class PaneledAttractionsRecommender(val panelToRecommender: Map[Int, AttractionsRecommender])
  extends AttractionsRecommender {

  override def recommend(user: String): Seq[String] =
    panelToRecommender(panelFromUser(user)).recommend(user)

  private def panelFromUser(user: String): Int = hash(user + salt) % amountOfPanels

  private def salt: String = Calendar.getInstance().get(Calendar.MINUTE).toString

  private def hash(string: String): Int = MurmurHash3.stringHash(string).abs

  private val amountOfPanels: Int = panelToRecommender.keys.size
}

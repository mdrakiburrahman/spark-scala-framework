package com.sparkscalafw.attractionsrecommender.training.spark

import com.sparkscalafw.attractionsrecommender.common.feeds.visits.VisitsColumnNames
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash}

/**
  * Implementation of an ALS Spark training pipeline that fits training dataset against the model.
  */
class AlsAttractionsRecommenderTrainer {

  // Initialize als Spark ML object and define model specific parameters, features and labels.
  private lazy val als = new ALS()
    .setUserCol(VisitsColumnNames.UserId)
    .setItemCol(VisitsColumnNames.AttractionId)
    .setRatingCol(VisitsColumnNames.VisitsCount)
    .setColdStartStrategy("drop")
    .setSeed(321L)

  // Define training activities - can be easily expanded to multi-step/iterations, as desired.
  def train(visits: DataFrame): ALSModel = als.fit(trainingData(visits))

  private def trainingData(visits: DataFrame): DataFrame =
    visits.withColumn(VisitsColumnNames.UserId, hash(col(VisitsColumnNames.UserId)))
}

object AlsAttractionsRecommenderTrainer {

  // Instantiate class.
  def apply(): AlsAttractionsRecommenderTrainer = new AlsAttractionsRecommenderTrainer
}

package com.sparky.attractionsrecommender.etl.attractions

import org.apache.spark.sql.DataFrame

trait AttractionsLoader {

  /**
    * @return the attractions loaded as a spark data frame.
    */
  def load(): DataFrame
}

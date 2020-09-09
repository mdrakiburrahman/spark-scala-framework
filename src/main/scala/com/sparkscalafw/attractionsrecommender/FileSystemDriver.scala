package com.sparkscalafw.attractionsrecommender

import com.sparkscalafw.attractionsrecommender.etl.EtlDriver
import com.sparkscalafw.attractionsrecommender.serving.ServingDriver
import com.sparkscalafw.attractionsrecommender.training.TrainingDriver

object FileSystemDriver extends App {

  EtlDriver.main(Array())
  TrainingDriver.main(Array())
  ServingDriver.main(Array())
}

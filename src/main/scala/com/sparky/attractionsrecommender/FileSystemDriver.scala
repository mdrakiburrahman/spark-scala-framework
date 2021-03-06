package com.sparky.attractionsrecommender

import com.sparky.attractionsrecommender.etl.EtlDriver
import com.sparky.attractionsrecommender.serving.ServingDriver
import com.sparky.attractionsrecommender.training.TrainingDriver

/**
  * Gluing together our different pipelines to run under a single program driver.
  * We run our three individual drivers in sequence, here we are relying on the feeds framework (i.e. FileSystem)
  * to take care of inputs and outputs. This allows us to decouple the key steps of our pipeline, while still maintaining tight integration.
  */
object FileSystemDriver extends App {

  EtlDriver.main(Array())
  TrainingDriver.main(Array())
  ServingDriver.main(Array())
}

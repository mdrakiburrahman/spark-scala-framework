package com.sparkscalafw.attractionsrecommender

import java.io.File

import com.sparkscalafw.attractionsrecommender.common.config.EnvironmentConfiguration
import com.sparkscalafw.attractionsrecommender.etl.EtlDriver
import com.sparkscalafw.attractionsrecommender.serving.ServingDriver
import com.sparkscalafw.attractionsrecommender.training.TrainingDriver
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

/**
  * Implementation of an integration & unit testing pipeline that is able to perform automated tests by enforcing environment configurations.
  * This not only allows us to perform assertions that all the supported fields are populated, but also:
  * 1. Allows unit testing and ETL logic to be tightly integrated and implemented within one framework.
  * 2. Although the below dummy implementation only asserts that the desired feeds are all populated, we can expand this to add as many assertions as we like.
  * 3. We are forcing an environment configuration programmatically here, means this code can be parameterized to run on any dataset as part of a CICD pipeline - by simply respecting the parameters.
  * 4. The integration tests are isolated from any previous ones, means they're stateless (directory can be cleaned after testing is done).
  * 5. The pipeline and testing can run on local Spark Cluster, Databricks or Synapse.
  */
class FullIntegrationTest extends AnyFunSpec with Matchers {

  // Force environment configuration in local mode for unit test - can be set to an environment configuration as part of CI/CD Pipeline.
  // Note that we're also passing in the root path containing our raw or parsed data for unit testing - SigirRawDataPath.
  EnvironmentConfiguration.force(new EnvironmentConfiguration {
    override val LocalSpark: Boolean = true
    override val FeedsRootPath
      : String = "file://" + new File("target/integration-test/feeds").getAbsolutePath + "/"
    override val SigirRawDataPath
      : String = "file://" + new File("src/test/data/sigir17").getAbsolutePath + "/"
    override val DebugEnabled: Boolean = false
  })

  describe("the attractions recommender") {
    it("should run all the components successfully") {
      // Run the Pipeline via Drivers
      EtlDriver.main(Array())
      TrainingDriver.main(Array())
      ServingDriver.main(Array())
      // Enforce unit testing assertions by ensuring data is populated
      new File("target/integration-test/feeds/etl/visits/").exists() must be(true)
      new File("target/integration-test/feeds/etl/attractions/").exists() must be(true)
      new File("target/integration-test/feeds/training/als/").exists() must be(true)
    }
  }
}

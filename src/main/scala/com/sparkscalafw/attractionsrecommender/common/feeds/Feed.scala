package com.sparkscalafw.attractionsrecommender.common.feeds

import java.io.File

import com.sparkscalafw.attractionsrecommender.common.config.EnvironmentConfiguration
import com.sparkscalafw.attractionsrecommender.common.feeds.io.{CsvFeedIO, FeedIO, ParquetFeedIO}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Definition of a Feed that can put updates in a given path and get an update form that same path.
  * Delegates IO to a given FeedIO object.
  *
  * @param feedIO the object to be used for writing to and reading from the file system.
  * @param path the path in which to put and look for the updates
  * @tparam T the type of the updates.
  */
class Feed[T](feedIO: FeedIO[T], path: String, updateHandler: FeedUpdateHandler) {
  
  // Writes an update to the Feed path.
  def put(update: T): Unit = {
    feedIO.write(update, updateHandler.newUpdate(path))
  }

  // Reads an update from the Feed path.
  def get(): T =
    feedIO.read(updateHandler.latestUpdate(path))
}

object Feed {

  // Definition of a Parquet Feed.
  def parquet(spark: SparkSession, path: String): Feed[DataFrame] =
    apply(ParquetFeedIO(spark), path)

  // Definition of a CSV Feed.
  def csv(spark: SparkSession, path: String): Feed[DataFrame] =
    apply(CsvFeedIO(spark), path)

  // TODO: Definition of a Delta Feed.

  // Instantiate a new Feed.
  def apply[T](feedIO: FeedIO[T], path: String): Feed[T] = {
    val feedPath = EnvironmentConfiguration().FeedsRootPath + path
    new Feed(feedIO, feedPath, FeedUpdateHandler(feedPath))
  }
}

/**
  * Definition of Update Operations supported on a Feed.
  */
trait FeedUpdateHandler {

  // Commits new Update to path location.
  def newUpdate(path: String): String

  // Overwrites with latest Update to path location.
  def latestUpdate(path: String): String
}

/**
  * Defines cases for supporting different storage layers.
  * Currently structured to support Local Filesystem, Azure Data Lake Storage Gen2, and AWS S3 Buckets.
  */
object FeedUpdateHandler {

  def apply(feedPath: String): FeedUpdateHandler = feedPath.split("://")(0) match {
    case "file" => new LocalFileSystemFeedUpdateHandler
    case "abfss" => new ADLSFeedUpdateHandler
    case "s3" => new S3FeedUpdateHandler
    case other: String => throw new RuntimeException(s"Unsupported feeds scheme: $other")
  }
}

/**
  * Define actions on a Local Filesystem.
  */
class LocalFileSystemFeedUpdateHandler extends FeedUpdateHandler {

  def newUpdate(path: String): String = path + System.currentTimeMillis() + "/"

  override def latestUpdate(path: String): String =
    path + new File(path.replace("file://", "")).listFiles
      .filter(_.isDirectory)
      .map(_.getName)
      .max + "/"
}

/**
  * Define actions on an Azure Data Lake Storage Gen2 Filesystem.
  */
class ADLSFeedUpdateHandler extends FeedUpdateHandler {

  // TODO: Replace Stub with code for ADLS new dataset creation.
  override def newUpdate(path: String): String = ???

  // TODO: Replace Stub with Code for ADLS overwrite.
  override def latestUpdate(path: String): String = ???
}

/**
  * Define actions on a S3 Bucket Filesystem.
  */
class S3FeedUpdateHandler extends FeedUpdateHandler {
  
  // TODO: Replace Stub with Code for S3 new dataset creation.
  override def newUpdate(path: String): String = ???

  // TODO: Replace Stub with Code for S3 overwrite.
  override def latestUpdate(path: String): String = ???
}

/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.di

import com.di.logic.SparkLogic
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object Main extends App {

  val logger: Logger = LogManager.getRootLogger
  logger.setLevel(Level.WARN)

  val parser = new OptionParser[Arguments]("Marketing Analytics") {
    opt[String]('t', "task")
      .required().valueName("").action((value, arguments) => arguments.copy(task = value))
  }

  def run(args: Arguments): Unit = {
    val config: Config = ConfigFactory
      .load("application.conf")
      .getConfig("main")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val io = new S3(config, logger)
    val sparkLogic = new SparkLogic(spark, config, logger)

    val requiresDownloading = config.getBoolean("s3.requires-download")

    io.downloadData(requiresDownloading)

    args.task match {
      case "convert-csv-parquet" =>
        logger.info("Converting csv to parquet...")
        sparkLogic.csvToParquet

      case "session-projection" =>
        logger.info("Creating session projection...")
        sparkLogic.createProjection.show()

      case "top-10-campaigns" =>
        logger.info("Acquiring top-10 campaigns...")
        val projection = sparkLogic.createProjection
        sparkLogic.topCampaigns(projection).show()

      case "top-channels" =>
        logger.info("Acquiring top channels...")
        val projection = sparkLogic.createProjection
        sparkLogic.channelsEngagements(projection).show()

      case taskName =>
        logger.error(s"Invalid task argument provided: $taskName")
    }
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) => run(arguments)
    case None => logger.error("No task name provided")
  }
}

case class Arguments(task: String = "")
package com.di.logic

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.di.domain.MobileAppClickStreamRow
import com.di.domain.UserPurchasesRow
import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

class SparkLogic(spark: SparkSession, config: Config, logger: Logger) extends ServiceMethods {
  private val mobileAppPathToParquet: String = config.getString("data.mobile-app-clickstream-output")
  private val userPurchasesPathToParquet: String = config.getString("data.user-purchases-output")

  private def saveAsParquet(dataset: Dataset[Row], prefix: String): Unit =
    Try(dataset.write.mode(SaveMode.Overwrite).parquet(s"data/parquet/${prefix}.parquet")) match { //todo: partitionBy
      case Success(_) => ()
      case Failure(ex) => logger.warn(s"Failed to save as parquet due to ${ex.getLocalizedMessage}")
    }

  private def readParquetData(path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def csvToParquet: Unit = {
    val clickStreamDataset   = clearMobileAppClickStreamData()
    val userPurchasesDataset = clearUserPurchasesData()

    saveAsParquet(clickStreamDataset, MobileAppClickStreamRow.prefix)
    saveAsParquet(userPurchasesDataset, UserPurchasesRow.prefix)
  }

  /**
   * Task 2
   */
  def createProjection: DataFrame = {
    val mobileAppDf =   readParquetData(mobileAppPathToParquet)
    val userPurchases = readParquetData(userPurchasesPathToParquet)

    val sessionized = sessionize(mobileAppDf, spark)
    userPurchases.join(sessionized, usingColumn = "purchaseId")
  }

  /**
   * Task 3.1
   */
  def topCampaigns(projection: DataFrame): DataFrame = {
    val query =
      s"SELECT SUM(billingCost) as totalBilling, campaignId FROM purchases_projection " +
        s"WHERE isConfirmed IS true " +
        s"GROUP BY campaignId " +
        s"ORDER BY totalBilling DESC " +
        s"LIMIT 10"

    projection.createOrReplaceTempView("purchases_projection")
    spark.sql(query)
  }

  /**
   * Task 3.2
   */
  def channelsEngagements(projection: DataFrame): DataFrame = {
    val query =
      s"SELECT channelId, campaignId, COUNT(DISTINCT sessionId) as totalSessions " +
        s"FROM inner_projection " +
        s"GROUP BY channelId, campaignId"

    projection.createOrReplaceTempView("inner_projection")
    val unranked = spark.sql(query)

    val rankingQuery =
      s"SELECT dense_rank() over w as rank, channelId, campaignId, totalSessions " +
        s"FROM ranked " +
        s"WINDOW w AS (PARTITION BY campaignId ORDER BY totalSessions DESC) " +
        s"ORDER BY campaignId, totalSessions DESC"

    unranked.createOrReplaceTempView("ranked")
    val ranked = spark.sql(rankingQuery)

    val filteringQuery =
      s"SELECT channelId, campaignId " +
        s"FROM filtered " +
        s"WHERE rank = 1 AND campaignId IS NOT NULL"

    ranked.createOrReplaceTempView("filtered")
    spark.sql(filteringQuery)
  }

  private def clearUserPurchasesData(pathToData: String = "data/user_purchases/*.csv") = spark.read
    .format("csv")
    .option("header", true)
    .schema(UserPurchasesRow.schema)
    .load(pathToData)
    .filter(userPurchasesDataFilter)

  private def clearMobileAppClickStreamData(pathToData: String = "data/mobile_app_clickstream/*.csv") =
    spark.read
      .format("csv")
      .option("header", true)
      .schema(MobileAppClickStreamRow.schema)
      .load(pathToData)
      .withColumn(
        "attributes",
        from_json(
          regexp_replace(col("attributes"), "'", "\""),
          lit("map<string,string>")
        )
      )
      .filter(mobileAppClickStreamDataFilter)
}

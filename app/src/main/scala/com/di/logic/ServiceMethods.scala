package com.di.logic

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lag, last, sum, udf, when}

trait ServiceMethods {
  def userPurchasesDataFilter: Column =
    col("billingCost") >= 0

  def mobileAppClickStreamDataFilter: Column =
    (col("eventTime") > "1974-01-01 23:59:59") && (col("eventType").isNotNull)

  def sessionize(mobileAppClickStreamDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    import java.util.UUID.nameUUIDFromBytes

    val partitionedByUserIdAndOrdered = Window.partitionBy($"userId").orderBy($"eventTime")
    val generateUUID = udf ((str: String) => nameUUIDFromBytes(str.getBytes).toString)

    val query =
      s"SELECT * FROM mobile_app_clickstream " +
        s"WHERE attributes IS NOT NULL " +
        s"ORDER BY eventTime"
    mobileAppClickStreamDf.createOrReplaceTempView("mobile_app_clickstream")
    spark.sql(query)
      .withColumn("campaignId", $"attributes.campaign_id")
      .withColumn("channelId", $"attributes.channel_id")
      .withColumn("purchaseId", $"attributes.purchase_id")
      .withColumn("isNewSession",
        when($"eventType" === "app_open", 1).otherwise(0)
      )
      .withColumn(
        "sessionId",
        generateUUID(sum($"isNewSession").over(partitionedByUserIdAndOrdered)) //todo: check
      )
      .withColumn(
        "channelId",
        when($"sessionId" ===
          lag($"sessionId", 1).over(partitionedByUserIdAndOrdered),
          last($"channelId", true).over(partitionedByUserIdAndOrdered))
          .otherwise($"channelId")
      )
      .withColumn(
        "campaignId",
        when($"sessionId" ===
          lag($"sessionId", 1).over(partitionedByUserIdAndOrdered),
          last($"campaignId", true).over(partitionedByUserIdAndOrdered))
          .otherwise($"campaignId")
      )
      .filter($"eventType" === "purchase")
      .drop("isNewSession", "attributes", "userId", "eventId", "eventType", "eventTime")
  }

}

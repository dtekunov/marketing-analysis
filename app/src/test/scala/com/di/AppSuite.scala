/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
package com.di

import com.di.domain.{MobileAppClickStreamRow, UserPurchasesRow}
import com.di.logic.{ServiceMethods, SparkLogic}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, from_json, lit, regexp_replace}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class AppSuite extends AnyFunSuite with ServiceMethods {
  val config: Config = ConfigFactory
    .load("application.conf")
    .getConfig("main")

  val logger: Logger = LogManager.getRootLogger
  logger.setLevel(Level.WARN)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  val sparkLogic = new SparkLogic(spark, config, logger)

  val mobileAppClickStreamTestData = List(
    List("id1","id11","app_open", Timestamp.valueOf("2020-12-13 15:43:30"),"{'campaign_id': 1, 'channel_id': 'Facebook Ads'}"),
    List("id1", "1003f2ed-df2c-43d2-8ae3-332efe0c9a6a", "purchase", Timestamp.valueOf("2020-12-13 15:44:30"), "{'purchase_id': 'e3e70682-c209-4cac-a29f-6fbed82c07cd'}"),
    List("id2","id11","app_open", Timestamp.valueOf("1972-12-13 15:45:30"),"{'campaign_id': 1, 'channel_id': 'Facebook Ads'}"),
    List("id1", "1003f2ed-df2c-43d2-8ae3-332efe0c9a6b", "purchase", Timestamp.valueOf("2020-12-13 15:44:30"), null),
    List(null, null, null, null, null)
  )

  val mobileAppRows: List[Row] = mobileAppClickStreamTestData.map(Row.apply)
  val mobileAppRdd: RDD[Row] = spark.sparkContext.makeRDD(mobileAppRows)
  val mobileAppDf: DataFrame = spark.createDataFrame(mobileAppRdd, MobileAppClickStreamRow.schema)

  val userPurchasesTestData = List(
    List("e3e70682-c209-4cac-a29f-6fbed82c07cd",Timestamp.valueOf("2020-12-13 16:00:02"), 845.98, false),
    List("e3e70682-c209-4cac-a29f-6fbed82c07cd",Timestamp.valueOf("2020-12-13 16:00:02"), -1.0, false),
    List("e3e70682-c209-4cac-a29f-6fbed82c07cd",Timestamp.valueOf("2020-12-13 16:00:02"), 0.0, false)
  )
  val userPurchasesRows: Seq[Row] = userPurchasesTestData.map(Row.apply)
  val userPurchasesRdd: RDD[Row] = spark.sparkContext.makeRDD(userPurchasesRows)
  val userPurchasesDf: DataFrame = spark.createDataFrame(userPurchasesRdd, UserPurchasesRow.schema)

  test("Filters over data work correctly") {

    val mobileAppDfSizeAfterFilter = mobileAppDf.filter(mobileAppClickStreamDataFilter).count()
    val userPurchasesDfSizeAfterFilter = userPurchasesDf.filter(userPurchasesDataFilter).count()

    assert(mobileAppDfSizeAfterFilter == 3)
    assert(userPurchasesDfSizeAfterFilter == 2)
  }

  test("Purchases with empty attributes do not apply to sessionized dataframe") {
    val withParsedAttributes = mobileAppDf.withColumn(
      "attributes",
      from_json(
        regexp_replace(col("attributes"), "'", "\""),
        lit("map<string,string>")
      )
    )
    val res = sessionize(withParsedAttributes, spark)

    assert(res.count() == 1)
    assert(res.collect()(0)(0) == "1")
  }

  test("3.1 works correctly") {

    val projectionSchema =     StructType(
      StructField("purchaseId", StringType, true) ::
        StructField("purchaseTime", TimestampType, true) ::
        StructField("billingCost", DoubleType, true) ::
        StructField("isConfirmed", BooleanType, true) ::
        StructField("campaignId", StringType, true) ::
        StructField("channelId", StringType, true) ::
        StructField("sessionId", StringType, true) ::
        Nil)

    val price1 = 964.35
    val price2 = 100.00

    val correctId = "81"

    val projectionTestData = List(
      List("00008b09-6803-4fb1-a4ed-6301c05ac04c", Timestamp.valueOf("2020-09-24 01:09:36"), 73.04,     true,       "88",        "Google Ads",  "6f4922f4-5568-361a-8cdf-4ad2299f6d23"),
      List("0000a8a6-51e4-4150-856a-e61c6493ae86", Timestamp.valueOf("2020-11-14 07:44:07"), 342.11     ,false      ,"50"        ,"VK Ads"      ,"8b406655-4730-3dfa-a026-6346bdc1b202"),
      List("0000c689-aa6e-4154-b55a-0ef4c20bb378", Timestamp.valueOf("2020-10-16 04:26:36"), 112.63     ,false      ,"79"        ,"Google Ads"  ,"dc6a6489-640c-302b-8d42-dabeb8e46bb7"),
      List("0000f7a5-e6cd-4c76-a2fd-c9f3a47f9fb4", Timestamp.valueOf("2020-10-30 15:07:25"), 911.47     ,false      ,"96"        ,"Twitter Ads" ,"e7f8a7fb-0b77-3cb3-b283-af5be021448f"),
      List("0000f8e6-d956-4373-af92-6cc0b4e12701", Timestamp.valueOf("2020-12-25 10:49:54"), 320.88     ,true       ,"25"        ,"VK Ads"      ,"fb8feff2-53bb-3c83-8deb-61ec76baa893"),
      List("00012203-63d2-41be-aa60-c6168ff58979", Timestamp.valueOf("2020-11-17 17:09:46"), 180.41     ,true       ,"51"        ,"Facebook Ads","09c6c378-3b4a-3005-8da7-4f2538ed47c6"),
      List("00012e87-a996-4378-b05d-247e319fbdad", Timestamp.valueOf("2020-10-14 15:10:40"), 159.57     ,false      ,"76"        ,"Google Ads"  ,"8fe0093b-b30d-3f8c-b147-4bd0764e6ac0"),
      List("000145f6-622a-45a8-a23a-8441a9d1788d", Timestamp.valueOf("2020-12-05 16:08:46"), 154.27     ,true       ,"11"        ,"Twitter Ads" ,"e655c771-6a4b-3ea6-bf48-c6322fc42ed6"),
      List("00014678-b4bd-455f-8cf1-08c59af1aea8", Timestamp.valueOf("2020-10-07 07:13:31"), 932.27     ,false      ,"49"        ,"Google Ads"  ,"0e01938f-c48a-3cfb-9f22-17fbfb00722d"),
      List("00016ce8-1701-4c44-b6e3-fba2ce349488", Timestamp.valueOf("2020-11-22 21:04:49"), 912.69     ,true       ,"34"        ,"Facebook Ads","a9eb8122-38f7-3313-a652-ae09963a05e9"),
      List("0001a0b7-d6c2-4655-ad2d-1d2785970829", Timestamp.valueOf("2020-11-05 10:42:36"), 663.34     ,true       ,"49"        ,"VK Ads"      ,"19b65066-0b25-3761-af18-9682e03501dd"),
      List("0001b3c6-6149-40af-95fc-28ab5e95e30a", Timestamp.valueOf("2020-12-25 23:04:34"), 692.95     ,true       ,"63"        ,"Twitter Ads" ,"ea6b2efb-dd42-35a9-b1b3-bbc6399b58f4"),
      List("0001c2a0-cf4a-4cbc-ac13-d4a654acc53e", Timestamp.valueOf("2020-11-07 13:34:49"), 760.50     ,true       ,"71"        ,"Google Ads"  ,"c2aee861-57b4-340b-b813-2f1e71a9e6f1"),
      List("0002110d-1a37-4c9a-8108-7580959549d1", Timestamp.valueOf("2020-11-12 20:14:22"), 576.03     ,false      ,"2"         ,"Google Ads"  ,"98dce83d-a57b-3395-a163-467c9dae521b"),
      List("00023a08-5a7a-4e35-8b25-a99c98ba4d61", Timestamp.valueOf("2020-11-01 13:12:46"), 307.50     ,true       ,"13"        ,"Twitter Ads" ,"7a53928f-a4dd-31e8-ac6e-f826f341daec"),
      List("00023ed5-4871-40aa-a070-f8276a2499b1", Timestamp.valueOf("2020-11-16 16:17:53"), 629.38     ,true       ,"64"        ,"Facebook Ads","41bfd20a-38bb-3b0b-ac75-acf0845530a7"),
      List("00025cf8-80e2-4a58-9e9b-5d370f6eb3ba", Timestamp.valueOf("2020-11-27 01:44:04"), price1     ,true       ,correctId        ,"Yandex Ads"  ,"7f53f8c6-c730-3f6a-ab52-e66eb74d8507"),
      List("00025cf8-80e2-4a58-9e9b-5d370f6eb3bb", Timestamp.valueOf("2020-11-27 01:44:05"), price2     ,true       ,correctId        ,"Yandex Ads"  ,"7f53f8c6-c730-3f6a-ab52-e66eb74d8508"),
      List("00025f3e-84e3-4dff-906c-6b718d8533d1", Timestamp.valueOf("2020-12-17 16:03:48"), 87.90      ,false      ,"29"        ,"Twitter Ads" ,"e1696007-be4e-3fb8-9b1a-1d39ce48681b"),
      List("00027904-6b10-48f5-ae56-4cc3af451cd9", Timestamp.valueOf("2020-10-13 22:28:48"), 685.84     ,false      ,"16"        ,"Yandex Ads"  ,"6ecbdd6e-c859-3284-9c13-885a37ce8d81"),
      List("0002babe-d783-4562-9224-eca016c0a726", Timestamp.valueOf("2020-12-26 20:03:51"), 376.40      ,true       ,"61"        ,"Yandex Ads"  ,"af3303f8-52ab-3ccd-b930-68486a391626"),
    )

    val projectionRows: Seq[Row] = projectionTestData.map(Row.apply)
    val projectionRdd: RDD[Row] = spark.sparkContext.makeRDD(projectionRows)
    val projectionDf: DataFrame = spark.createDataFrame(projectionRdd, projectionSchema)

    val topCampaignsDf = sparkLogic.topCampaigns(projectionDf)

    assert(topCampaignsDf.count() == 10)
    assert(topCampaignsDf.collect()(0)(1) == correctId)
    assert(topCampaignsDf.collect()(0)(0) == price1 + price2)
  }

  test("3.2 works correctly") {
    val projectionSchema =     StructType(
      StructField("purchaseId", StringType, true) ::
        StructField("purchaseTime", TimestampType, true) ::
        StructField("billingCost", DoubleType, true) ::
        StructField("isConfirmed", BooleanType, true) ::
        StructField("campaignId", StringType, true) ::
        StructField("channelId", StringType, true) ::
        StructField("sessionId", StringType, true) ::
        Nil)

    val projectionTestData = List(
      List("00008b09-6803-4fb1-a4ed-6301c05ac04c", Timestamp.valueOf("2020-09-24 01:09:36"), 73.04,     true,       "88",        "Google Ads",  "6f4922f4-5568-361a-8cdf-4ad2299f6d23"),
      List("00008b09-6803-4fb1-a4ed-6301c05ac04c", Timestamp.valueOf("2020-09-24 01:09:36"), 73.04,     true,       "88",        "Google Ads",  "6f4922f4-5568-361a-8cdf-4ad2299f6d24"),
      List("00008b09-6803-4fb1-a4ed-6301c05ac04c", Timestamp.valueOf("2020-09-24 01:09:36"), 73.04,     true,       "88",        "VK Ads",      "6f4922f4-5568-361a-8cdf-4ad2299f6d25")

    )

    val projectionRows: Seq[Row] = projectionTestData.map(Row.apply)
    val projectionRdd: RDD[Row] = spark.sparkContext.makeRDD(projectionRows)
    val projectionDf: DataFrame = spark.createDataFrame(projectionRdd, projectionSchema)

    val channelsEngagementsDf = sparkLogic.channelsEngagements(projectionDf)

    assert(channelsEngagementsDf.count() == 1)
    assert(channelsEngagementsDf.collect()(0)(0) == "Google Ads")

  }
}

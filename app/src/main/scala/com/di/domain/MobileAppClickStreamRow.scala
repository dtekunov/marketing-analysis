package com.di.domain

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

//todo: remove nullable where necessary
case class MobileAppClickStreamRow(userId: String,
                                   eventId: String,
                                   eventTime: Timestamp,
                                   eventType: String,
                                   attributes: Option[Map[String, String]])

object MobileAppClickStreamRow {
  final val prefix = "mobile_app_clickstream"

  final val schema =
    StructType(
      StructField("userId", StringType, true) ::
        StructField("eventId", StringType, true) ::
        StructField("eventType", StringType, true) ::
        StructField("eventTime", TimestampType, true) ::
        StructField("attributes", StringType, true) :: Nil)
}
package com.di.domain

import org.apache.spark.sql.types._

import java.sql.Timestamp

case class UserPurchasesRow(purchaseId: String,
                            purchaseTime: Timestamp,
                            billingCost: Double,
                            isConfirmed: Boolean) {
}

object UserPurchasesRow {
  final val prefix = "user_purchases"

  final val schema =
    StructType(
      StructField("purchaseId", StringType, true) ::
        StructField("purchaseTime", TimestampType, true) ::
        StructField("billingCost", DoubleType, true) ::
        StructField("isConfirmed", BooleanType, true) :: Nil)
}

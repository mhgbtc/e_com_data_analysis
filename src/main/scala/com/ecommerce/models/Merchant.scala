package com.ecommerce.models

// Un marchand tel que lu dans merchants.csv
case class Merchant(
  merchant_id: String,
  name: Option[String],
  category: Option[String],
  region: Option[String],
  commission_rate: Option[Double],
  establishment_date: Option[String]
)

package com.ecommerce.models

// Une transaction telle que lue dans transactions.csv
// amount est Option car certains montants sont manquants ou invalides
case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String,
  location: String,
  payment_method: String,
  category: String
)

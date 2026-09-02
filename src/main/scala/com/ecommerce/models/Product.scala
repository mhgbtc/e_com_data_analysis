package com.ecommerce.models

// Un produit tel que lu dans products.parquet
case class Product(
  product_id: String,
  name: Option[String],
  category: Option[String],
  price: Option[Double],
  merchant_id: String,
  rating: Option[Double],
  stock: Option[Int]
)

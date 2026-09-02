package com.ecommerce.models

// Un utilisateur tel que lu dans users.json
// Les champs numériques et certains champs texte sont Option car
// le fichier contient des valeurs manquantes
case class User(
  user_id: String,
  age: Option[Int],
  annual_income: Option[Double],
  city: Option[String],
  customer_segment: Option[String],
  preferred_categories: Option[Seq[String]],
  registration_date: Option[String]
)

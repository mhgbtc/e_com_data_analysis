package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Regroupe les quatre datasets lus, avant validation
case class DonneesBrutes(
  transactions: Dataset[Transaction],
  users: Dataset[User],
  products: Dataset[Product],
  merchants: Dataset[Merchant]
)

// Centralise la lecture et le typage des données (Partie 2, Questions 2.1 et 2.3)
class DataIngestion(spark: SparkSession, conf: ConfigLoader) {

  import spark.implicits._

  // Les chemins proviennent de application.conf, jamais codés en dur
  private val cheminTransactions =
    conf.getString("app.data.input.transactions", "src/main/resources/data/transactions.csv")
  private val cheminUsers =
    conf.getString("app.data.input.users", "src/main/resources/data/users.json")
  private val cheminProducts =
    conf.getString("app.data.input.products", "src/main/resources/data/products.parquet")
  private val cheminMerchants =
    conf.getString("app.data.input.merchants", "src/main/resources/data/merchants.csv")

  // Schéma explicite pour les transactions
  private val schemaTransactions = StructType(Seq(
    StructField("transaction_id", StringType),
    StructField("user_id", StringType),
    StructField("product_id", StringType),
    StructField("merchant_id", StringType),
    StructField("amount", DoubleType),
    StructField("timestamp", StringType),
    StructField("location", StringType),
    StructField("payment_method", StringType),
    StructField("category", StringType)
  ))

  // Schéma explicite pour les utilisateurs (gère le champ imbriqué preferred_categories)
  private val schemaUsers = StructType(Seq(
    StructField("user_id", StringType),
    StructField("age", IntegerType),
    StructField("annual_income", DoubleType),
    StructField("city", StringType),
    StructField("customer_segment", StringType),
    StructField("preferred_categories", ArrayType(StringType)),
    StructField("registration_date", StringType)
  ))

  // transactions.csv : schéma défini explicitement
  def lireTransactions(): Dataset[Transaction] =
    spark.read
      .option("header", "true")
      .schema(schemaTransactions)
      .csv(cheminTransactions)
      .as[Transaction]

  // users.json : schéma explicite pour contrôler les types et le champ imbriqué
  def lireUsers(): Dataset[User] =
    spark.read
      .schema(schemaUsers)
      .json(cheminUsers)
      .as[User]

  // products.parquet : format optimisé, on aligne les types sur la case class
  def lireProducts(): Dataset[Product] =
    spark.read
      .parquet(cheminProducts)
      .select(
        col("product_id").cast(StringType),
        col("name").cast(StringType),
        col("category").cast(StringType),
        col("price").cast(DoubleType),
        col("merchant_id").cast(StringType),
        col("rating").cast(DoubleType),
        col("stock").cast(IntegerType)
      )
      .as[Product]

  // merchants.csv : on laisse Spark inférer, puis on aligne les types
  def lireMerchants(): Dataset[Merchant] =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(cheminMerchants)
      .select(
        col("merchant_id").cast(StringType),
        col("name").cast(StringType),
        col("category").cast(StringType),
        col("region").cast(StringType),
        col("commission_rate").cast(DoubleType),
        col("establishment_date").cast(StringType)
      )
      .as[Merchant]

  // Lit les quatre sources avec gestion d'erreurs et affiche le nombre de lignes lues
  def chargerTout(): DonneesBrutes = {
    try {
      val transactions = lireTransactions()
      val users        = lireUsers()
      val products     = lireProducts()
      val merchants    = lireMerchants()

      println("Lecture des données terminée. Nombre de lignes lues avant validation :")
      println(s"  transactions : ${transactions.count()}")
      println(s"  users        : ${users.count()}")
      println(s"  products     : ${products.count()}")
      println(s"  merchants    : ${merchants.count()}")

      DonneesBrutes(transactions, users, products, merchants)
    } catch {
      case e: Exception =>
        println(s"Erreur lors du chargement des données : ${e.getMessage}")
        throw e
    }
  }
}

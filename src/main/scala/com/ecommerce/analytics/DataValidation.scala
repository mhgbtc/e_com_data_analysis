package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Regroupe les données valides, le rapport de qualité et les lignes rejetées
case class DonneesValides(
  transactions: DataFrame,
  users: DataFrame,
  products: DataFrame,
  merchants: DataFrame,
  rapport: DataFrame,
  rejets: Map[String, DataFrame]
)

// Validation des données et rapport de qualité (Partie 2, Questions 2.2 et 2.4)
class DataValidation(spark: SparkSession, conf: ConfigLoader) {

  import spark.implicits._

  // Seuils de validation lus depuis la configuration (avec valeurs par défaut)
  private val minAmount     = conf.getDouble("app.validation.transaction.min-amount", 0.0)
  private val tsLength      = conf.getInt("app.validation.transaction.timestamp-length", 14)
  private val minAge        = conf.getInt("app.validation.user.min-age", 16)
  private val maxAge        = conf.getInt("app.validation.user.max-age", 100)
  private val minIncome     = conf.getDouble("app.validation.user.min-income", 0.0)
  private val minPrice      = conf.getDouble("app.validation.product.min-price", 0.0)
  private val minRating     = conf.getDouble("app.validation.product.min-rating", 1.0)
  private val maxRating     = conf.getDouble("app.validation.product.max-rating", 5.0)
  private val minCommission = conf.getDouble("app.validation.merchant.min-commission", 0.0)
  private val maxCommission = conf.getDouble("app.validation.merchant.max-commission", 1.0)

  // Sépare un DataFrame en (lignes valides, lignes rejetées) à partir d'une liste de règles.
  // Chaque règle est une condition de validité (vraie = valide) et le motif à afficher si elle échoue.
  private def separer(df: DataFrame, regles: Seq[(Column, String)]): (DataFrame, DataFrame) = {
    // Pour chaque règle : le motif si la condition n'est pas respectée, sinon null
    val motifs = regles.map { case (condition, motif) => when(!condition, lit(motif)) }
    // concat_ws ignore les valeurs nulles : chaîne vide => aucune règle violée => ligne valide
    val avecMotif = df.withColumn("rejection_reason", concat_ws("; ", motifs: _*))
    val valides   = avecMotif.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejetees  = avecMotif.filter(col("rejection_reason") =!= "")
    (valides, rejetees)
  }

  // Transactions : amount > 0 et timestamp de 14 caractères
  def validerTransactions(df: DataFrame): (DataFrame, DataFrame) = {
    val regles = Seq(
      (col("amount").isNotNull && col("amount") > minAmount,
        "montant invalide (<= 0 ou manquant)"),
      (col("timestamp").isNotNull && length(col("timestamp")) === tsLength,
        "timestamp mal formé")
    )
    separer(df, regles)
  }

  // Users : âge entre 16 et 100, annual_income > 0
  def validerUsers(df: DataFrame): (DataFrame, DataFrame) = {
    val regles = Seq(
      (col("age").isNotNull && col("age").between(minAge, maxAge),
        "âge hors intervalle ou manquant"),
      (col("annual_income").isNotNull && col("annual_income") > minIncome,
        "revenu invalide (<= 0 ou manquant)")
    )
    separer(df, regles)
  }

  // Products : price > 0 et rating entre 1 et 5
  def validerProducts(df: DataFrame): (DataFrame, DataFrame) = {
    val regles = Seq(
      (col("price").isNotNull && col("price") > minPrice,
        "prix invalide (<= 0 ou manquant)"),
      (col("rating").isNotNull && col("rating").between(minRating, maxRating),
        "note hors intervalle ou manquante")
    )
    separer(df, regles)
  }

  // Merchants : commission_rate entre 0 et 1
  def validerMerchants(df: DataFrame): (DataFrame, DataFrame) = {
    val regles = Seq(
      (col("commission_rate").isNotNull && col("commission_rate").between(minCommission, maxCommission),
        "taux de commission invalide")
    )
    separer(df, regles)
  }

  // Compte le nombre total de valeurs nulles sur toutes les colonnes d'un DataFrame
  private def compterNulls(df: DataFrame): Long = {
    val exprs = df.columns.map(c => sum(when(col(c).isNull, 1L).otherwise(0L)).alias(c))
    val ligne = df.agg(exprs.head, exprs.tail: _*).collect()(0)
    df.columns.indices.map { i =>
      if (ligne.isNullAt(i)) 0L else ligne.getLong(i)
    }.sum
  }

  // Arrondi à deux décimales du taux de rejet en pourcentage
  private def tauxRejet(lues: Long, rejetees: Long): Double = {
    if (lues == 0) 0.0
    else math.round((rejetees.toDouble / lues) * 100.0 * 100.0) / 100.0
  }

  // Construit une ligne du rapport de qualité pour un dataset
  private def ligneRapport(nom: String, brut: DataFrame,
                           valides: DataFrame, rejetees: DataFrame): (String, Long, Long, Long, Double, Long) = {
    val nbLues     = brut.count()
    val nbValides  = valides.count()
    val nbRejetees = rejetees.count()
    (nom, nbLues, nbValides, nbRejetees, tauxRejet(nbLues, nbRejetees), compterNulls(brut))
  }

  // Valide les quatre datasets et produit le rapport de qualité (Question 2.4)
  def validerTout(donnees: DonneesBrutes): DonneesValides = {
    val dfTransactions = donnees.transactions.toDF()
    val dfUsers        = donnees.users.toDF()
    val dfProducts     = donnees.products.toDF()
    val dfMerchants    = donnees.merchants.toDF()

    val (txOk, txKo) = validerTransactions(dfTransactions)
    val (usOk, usKo) = validerUsers(dfUsers)
    val (prOk, prKo) = validerProducts(dfProducts)
    val (meOk, meKo) = validerMerchants(dfMerchants)

    val lignes = Seq(
      ligneRapport("transactions", dfTransactions, txOk, txKo),
      ligneRapport("users",        dfUsers,        usOk, usKo),
      ligneRapport("products",     dfProducts,     prOk, prKo),
      ligneRapport("merchants",    dfMerchants,    meOk, meKo)
    )

    val rapport = lignes.toDF(
      "dataset", "nb_lignes_lues", "nb_lignes_valides",
      "nb_lignes_rejetees", "taux_rejet", "nb_valeurs_nulles"
    )

    DonneesValides(
      transactions = txOk,
      users        = usOk,
      products     = prOk,
      merchants    = meOk,
      rapport      = rapport,
      rejets       = Map("transactions" -> txKo, "users" -> usKo, "products" -> prKo, "merchants" -> meKo)
    )
  }
}

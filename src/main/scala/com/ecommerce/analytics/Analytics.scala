package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Analytique business : KPI marchands et analyse de cohortes (Partie 4)
class Analytics(spark: SparkSession, conf: ConfigLoader) {

  import spark.implicits._

  // Question 4.1 : rapport détaillé par marchand
  def rapportMarchands(enrichi: DataFrame): DataFrame = {

    // Indicateurs de base par marchand : CA, volume, clients uniques,
    // panier moyen et commission totale perçue par la plateforme.
    val base = enrichi
      .groupBy("merchant_id", "merchant_name", "merchant_category", "merchant_region")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        round(avg("amount"), 2).as("montant_moyen"),
        round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale")
      )

    // Deux fenêtres pour les classements : une par catégorie, une par région.
    // Elles s'appliquent APRÈS l'agrégation, sur le résultat par marchand :
    // chaque marchand est rangé parmi les marchands de sa catégorie,
    // puis parmi ceux de sa région, par CA décroissant.
    val fenetreCat = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val fenetreReg = Window.partitionBy("merchant_region").orderBy(col("chiffre_affaires").desc)

    val avecRangs = base
      .withColumn("rang_ca_categorie", rank().over(fenetreCat))
      .withColumn("rang_ca_region", rank().over(fenetreReg))

    // Répartition du chiffre d'affaires par tranche d'âge, avec un pivot :
    // les valeurs de la colonne age_bracket deviennent des colonnes.
    // La liste des valeurs est passée explicitement : Spark n'a pas besoin de
    // scanner les données pour découvrir les valeurs, et aucune colonne null
    // n'apparaît. Les colonnes sont ensuite renommées en ca_*.
    val parAge = enrichi
      .groupBy("merchant_id")
      .pivot("age_bracket", Seq("Jeune", "Adulte", "Âge Moyen", "Senior"))
      .agg(round(sum("amount"), 2))
      .withColumnRenamed("Jeune", "ca_jeune")
      .withColumnRenamed("Adulte", "ca_adulte")
      .withColumnRenamed("Âge Moyen", "ca_age_moyen")
      .withColumnRenamed("Senior", "ca_senior")

    // Jointure sur merchant_id (left : tout marchand du rapport est conservé,
    // même si une tranche d'âge serait vide), puis tri par CA décroissant.
    avecRangs
      .join(parAge, Seq("merchant_id"), "left")
      .orderBy(col("chiffre_affaires").desc)
  }

  // Question 4.2 : analyse de cohortes utilisateurs
  def analyseCohortes(enrichi: DataFrame): DataFrame = {

    // Date de première transaction de chaque client : elle détermine sa cohorte.
    val premiereTx = enrichi
      .groupBy("user_id")
      .agg(min("event_date").as("premiere_date"))

    // Étiquetage de chaque transaction avec sa cohorte et son indice de période.
    // trunc(date, "month") ramène la date au 1er du mois : sans ça,
    // months_between renverrait un nombre décimal (2,7 mois) au lieu d'un entier.
    val avecCohorte = enrichi
      .join(premiereTx, Seq("user_id"), "inner")
      .withColumn("cohort_month", date_format(col("premiere_date"), "yyyy-MM"))
      .withColumn("period_index",
        months_between(trunc(col("event_date"), "month"), trunc(col("premiere_date"), "month")).cast("int"))

    // Taille initiale de chaque cohorte : chaque client appartient à une seule
    // cohorte (celle de sa première transaction), donc un countDistinct des
    // user_id groupé par cohort_month suffit.
    val tailleCohorte = avecCohorte
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("nb_utilisateurs_initiaux"))

    // Matrice de rétention : pour chaque couple (cohorte, période), le nombre
    // de clients actifs rapporté à la taille initiale de la cohorte.
    val matrice = avecCohorte
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("nb_utilisateurs_actifs"))
      .join(tailleCohorte, Seq("cohort_month"), "inner")
      .withColumn("taux_retention",
        round(col("nb_utilisateurs_actifs") / col("nb_utilisateurs_initiaux") * 100, 2))
      .orderBy("cohort_month", "period_index")

    matrice
  }

  // Question 4.2, dernière puce : cohorte ayant la meilleure rétention à 3 mois.
  // On filtre la matrice sur period_index == 3 (trois mois après la première
  // transaction), on trie par taux décroissant et on garde la première ligne.
  def meilleureRetention3Mois(matrice: DataFrame): DataFrame =
    matrice
      .filter(col("period_index") === 3)
      .orderBy(col("taux_retention").desc)
      .limit(1)
}
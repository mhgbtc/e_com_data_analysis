package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Transformations avancées : enrichissement, fenêtrage, détection de comportements (Partie 3)
class DataTransformation(spark: SparkSession, conf: ConfigLoader) {

  import spark.implicits._

  // Diffusion des petites tables, pilotée par la configuration (Partie 5, Question 5.2)
  private val enableBroadcast =
    conf.getBoolean("app.optimization.enable-broadcast", true) &&
    conf.getBoolean("app.optimization.enabled", true)

  // Tranche d'âge du client à partir de la colonne user_age.
  // IMPORTANT : Senior est une condition EXPLICITE, et otherwise renvoie null.
  // Si on mettait otherwise("Senior"), un âge absent (user rejeté à la validation,
  // conservé par le left join) tomberait à tort dans "Senior". Ici il reste null,
  // ce qui est le comportement attendu par le sujet.
  // Le libellé "Âge Moyen" doit rester EXACT (accent compris) : Cheick Oumar s'en
  // sert dans son pivot par tranche d'âge.
  private def trancheAge =
    when(col("user_age") < 25, "Jeune")
      .when(col("user_age") < 45, "Adulte")
      .when(col("user_age") < 65, "Âge Moyen")
      .when(col("user_age") >= 65, "Senior")
      .otherwise(null)

  // Question 3.2 : joint les quatre tables, applique l'UDF temporelle, ajoute le rang
  // et le nombre de transactions par utilisateur, puis la tranche d'âge.
  def enrichTransactionData(donnees: DonneesValides): DataFrame = {

    // On sélectionne et on renomme AVANT de joindre : category et name existent dans
    // plusieurs tables, sans renommage Spark aurait des colonnes ambiguës.
    val users = donnees.users.select(
      col("user_id"),
      col("age").as("user_age"),
      col("annual_income").as("user_income"),
      col("city").as("user_city"),
      col("customer_segment"),
      col("registration_date").as("user_registration_date")
    )

    val products = donnees.products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price").as("product_price"),
      col("rating").as("product_rating"),
      col("stock").as("product_stock")
      // merchant_id n'est PAS gardé ici : il vient déjà de la transaction.
    )

    val merchants = donnees.merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region").as("merchant_region"),
      col("commission_rate")
    )

    // Diffusion des petites tables si l'optimisation est activée
    val usersJ     = if (enableBroadcast) broadcast(users) else users
    val productsJ  = if (enableBroadcast) broadcast(products) else products
    val merchantsJ = if (enableBroadcast) broadcast(merchants) else merchants

    // Left join depuis les transactions (table pivot). On ne perd AUCUNE transaction
    // valide, même si l'utilisateur, le produit ou le marchand référencé est absent
    // (références orphelines volontaires dans les données). Un inner join les
    // supprimerait et fausserait le chiffre d'affaires. Justification à reprendre
    // dans CONTRIBUTIONS.md.
    val joint = donnees.transactions
      .join(usersJ,     Seq("user_id"),     "left")
      .join(productsJ,  Seq("product_id"),  "left")
      .join(merchantsJ, Seq("merchant_id"), "left")

    // Application de l'UDF puis éclatement de la structure en colonnes.
    val avecTemps = joint
      .withColumn("tf", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .withColumn("hour", col("tf.hour"))
      .withColumn("day_of_week", col("tf.day_of_week"))
      .withColumn("month", col("tf.month"))
      .withColumn("is_weekend", col("tf.is_weekend"))
      .withColumn("day_period", col("tf.day_period"))
      .withColumn("is_working_hours", col("tf.is_working_hours"))
      .drop("tf")
      // Horodatage exploitable par les fenêtres
      .withColumn("event_time", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("event_date", to_date(col("event_time")))

    // Deux fenêtres par utilisateur : une ordonnée par date, une sur tout l'historique
    val fenetreUser      = Window.partitionBy("user_id").orderBy("event_time")
    val fenetreUserTotal = Window.partitionBy("user_id")

    avecTemps
      .withColumn("transaction_rank",        row_number().over(fenetreUser))
      .withColumn("user_total_transactions", count(lit(1)).over(fenetreUserTotal))
      .withColumn("age_bracket",             trancheAge)
  }

  // Question 3.3 : montant cumulé sur 7 jours, utilisateur actif, délai depuis
  // l'achat précédent.
  def analyseParFenetre(enrichi: DataFrame): DataFrame = {

    // Fenêtre glissante de 7 jours par utilisateur. rangeBetween travaille sur une
    // plage de valeurs (le temps en secondes), pas sur un nombre de lignes.
    val septJours = 7L * 24 * 3600
    val fenetre7j = Window.partitionBy("user_id")
      .orderBy(col("event_time").cast("long"))
      .rangeBetween(-septJours, 0)

    // Montant total glissant sur 7 jours.
    val avecCumul = enrichi
      .withColumn("montant_cumule_7j", sum(col("amount")).over(fenetre7j))

    // Utilisateur actif : au moins 5 jours DISTINCTS d'activité sur 7 jours glissants.
    // Spark ne sait pas faire countDistinct dans une fenêtre. Astuce : on réduit
    // d'abord aux couples (user_id, jour) distincts ; chaque ligne y vaut 1 jour
    // actif, il suffit alors de COMPTER LES LIGNES dans la fenêtre de 7 jours.
    val sixJours       = 6L * 24 * 3600
    val joursDistincts = enrichi.select("user_id", "event_date").distinct()
    val fenetreJours   = Window.partitionBy("user_id")
      .orderBy(col("event_date").cast("timestamp").cast("long"))
      .rangeBetween(-sixJours, 0)

    val actifParJour = joursDistincts
      .withColumn("nb_jours_actifs_7j", count(lit(1)).over(fenetreJours))
      .withColumn("utilisateur_actif",
        when(col("nb_jours_actifs_7j") >= 5, 1).otherwise(0))

    // Délai en jours depuis la transaction précédente du même utilisateur.
    // lag donne la date précédente ; datediff calcule l'écart ; on supprime
    // ensuite la colonne intermédiaire. La 1re transaction donne null (normal).
    val fenetreUser = Window.partitionBy("user_id").orderBy("event_time")
    val avecDelai = avecCumul
      .withColumn("date_precedente", lag(col("event_date"), 1).over(fenetreUser))
      .withColumn("delai_jours_achat_precedent",
        datediff(col("event_date"), col("date_precedente")))
      .drop("date_precedente")

    // On rattache le flag utilisateur_actif au bon niveau, par (user_id, event_date).
    avecDelai.join(
      actifParJour.select("user_id", "event_date", "utilisateur_actif"),
      Seq("user_id", "event_date"),
      "left"
    )
  }

  // Chaîne complète de transformation (Question 3.2 puis 3.3)
  def transformer(donnees: DonneesValides): DataFrame = {
    val enrichi = enrichTransactionData(donnees)
    analyseParFenetre(enrichi)
  }
}

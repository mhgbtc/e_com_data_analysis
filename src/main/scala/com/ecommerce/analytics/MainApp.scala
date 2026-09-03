package com.ecommerce.analytics

import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Application principale : orchestre tout le pipeline (Partie 6).
// Accepte un argument d'étape : ingestion | transformation | analytics | all (défaut).
object MainApp {

  // Mesure et journalise la durée d'une étape. La signature à deux listes de
  // paramètres permet d'écrire chrono("ingestion") { ...du code... }
  private def chrono[T](nom: String)(bloc: => T): T = {
    val debut = System.currentTimeMillis()
    println(s"[$nom] début")
    val resultat = bloc
    val fin = System.currentTimeMillis()
    println(s"[$nom] fin - durée : ${fin - debut} ms")
    resultat
  }

  // Écrit un DataFrame aux formats CSV et Parquet dans le répertoire de sortie.
  // Le coalesce(1) regroupe toutes les lignes dans une seule partition, donc
  // un seul fichier CSV lisible (par Excel), pas un fichier par partition.
  // Le Parquet conserve les types, est compressé et colonnaire : bien plus
  // efficace pour une relecture par Spark.
  private def ecrire(df: DataFrame, dossier: String): Unit = {
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${dossier}_csv")
    df.write.mode("overwrite").parquet(s"${dossier}_parquet")
    println(s"Résultats écrits dans : ${dossier}_csv et ${dossier}_parquet")
  }

  // Message d'aide quand l'argument n'est pas reconnu
  private def aide(): Unit = {
    println("Argument d'étape inconnu.")
    println("Valeurs acceptées : ingestion | transformation | analytics | all")
  }

  def main(args: Array[String]): Unit = {
    val etape         = if (args.nonEmpty) args(0).toLowerCase else "all"
    val etapesValides = Set("ingestion", "transformation", "analytics", "all")

    // Un argument inconnu affiche l'aide sans lever d'exception
    if (!etapesValides.contains(etape)) {
      aide()
      return
    }

    val conf       = ConfigLoader()
    val spark      = SparkSessionBuilder.build(conf)
    val outputPath = conf.getString("app.data.output.path", "output/")

    try {
      println(s"=== Pipeline EcommerceAnalytics - étape demandée : $etape ===")

      val ingestion      = new DataIngestion(spark, conf)
      val validation     = new DataValidation(spark, conf)
      val transformation = new DataTransformation(spark, conf)
      val analytics      = new Analytics(spark, conf)
      val optim          = new SparkOptimizations(conf)

      // Phase 1 : ingestion et validation
      val valides = chrono("ingestion") {
        val brutes = ingestion.chargerTout()
        val donnees = validation.validerTout(brutes)
        donnees.rapport.show(false)
        println(s"Transactions valides : ${donnees.transactions.count()}")
        ecrire(donnees.rapport, s"${outputPath}rapport_qualite")
        donnees
      }
      if (etape == "ingestion") return

      // Phase 2 : transformation
      val enrichi = chrono("transformation") {
        // Ce DataFrame sert trois fois ensuite (rapport marchands, cohortes,
        // écriture) : sans cache, Spark recalculerait les jointures et les
        // fenêtres à chaque action car il est paresseux.
        val df = optim.mettreEnCache(transformation.transformer(valides))
        println("Échantillon du DataFrame enrichi :")
        df.select(
          "transaction_id", "user_id", "merchant_id", "merchant_name",
          "amount", "age_bracket", "event_date"
        ).show(10, false)
        df
      }
      if (etape == "transformation") {
        optim.liberer(enrichi)
        return
      }

      // Phase 3 : analytique
      val (rapportMarchands, cohortes, meilleure) = chrono("analytics") {
        val rapport = analytics.rapportMarchands(enrichi)
        println("=== Rapport par marchand (top 10) ===")
        rapport.show(10, false)

        val matrice = analytics.analyseCohortes(enrichi)
        println("=== Matrice de rétention par cohorte (20 premières lignes) ===")
        matrice.show(20, false)

        val meilleure = analytics.meilleureRetention3Mois(matrice)
        println("=== Meilleure rétention à 3 mois ===")
        meilleure.show(false)

        (rapport, matrice, meilleure)
      }

      // Phase 4 : écriture des résultats finaux
      ecrire(rapportMarchands, s"${outputPath}rapport_marchands")
      ecrire(cohortes, s"${outputPath}cohortes")
      optim.liberer(enrichi)

      println("=== Pipeline complet terminé avec succès ===")

    } catch {
      // Gestion d'erreurs globale
      case e: Exception =>
        println(s"Échec du pipeline : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Arrêt propre de la SparkSession dans tous les cas, y compris en cas d'échec.
      // Sans ça, le processus Java peut rester bloqué et les ressources non libérées.
      spark.stop()
    }
  }
}
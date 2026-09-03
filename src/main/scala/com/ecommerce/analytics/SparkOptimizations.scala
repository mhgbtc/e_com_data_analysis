package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

// Optimisations de stockage Spark : cache, persist, unpersist (Partie 5, Question 5.1).
// La diffusion des petites tables (broadcast) est appliquée dans DataTransformation,
// et spark.sql.shuffle.partitions est défini dans SparkSessionBuilder à partir de la config.
class SparkOptimizations(conf: ConfigLoader) {

  // Le cache est piloté par la configuration : ça permet de comparer les temps
  // d'exécution avec et sans optimisation (Question 5.3 bonus).
  private val cacheActif =
    conf.getBoolean("app.optimization.enable-cache", true) &&
    conf.getBoolean("app.optimization.enabled", true)

  // Met en cache un DataFrame réutilisé plusieurs fois.
  // cache() est un raccourci pour persist(MEMORY_ONLY) : rapide, mais si le
  // DataFrame ne tient pas en mémoire, Spark recalculera les morceaux évincés.
  def mettreEnCache(df: DataFrame): DataFrame =
    if (cacheActif) df.cache() else df

  // Persiste en mémoire ET sur disque, en version sérialisée (MEMORY_AND_DISK_SER),
  // pour les DataFrame trop volumineux pour tenir en mémoire seule : le surplus
  // est écrit sur disque sous forme sérialisée (plus compact) au prix d'un peu
  // de CPU pour désérialiser, mais on évite le recalcul.
  def persister(df: DataFrame): DataFrame =
    if (cacheActif) df.persist(StorageLevel.MEMORY_AND_DISK_SER) else df

  // Libère explicitement le cache lorsqu'il n'est plus nécessaire,
  // pour rendre la mémoire exécuteur au plus tôt.
  def liberer(df: DataFrame): Unit =
    df.unpersist()
}
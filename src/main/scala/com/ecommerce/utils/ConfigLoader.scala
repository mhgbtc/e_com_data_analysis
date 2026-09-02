package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

// Charge application.conf et fournit un accès avec valeur par défaut
// lorsqu'une clé est absente (exigence de la Partie 7)
class ConfigLoader(val config: Config) {

  def getString(chemin: String, defaut: String): String =
    if (config.hasPath(chemin)) config.getString(chemin) else defaut

  def getInt(chemin: String, defaut: Int): Int =
    if (config.hasPath(chemin)) config.getInt(chemin) else defaut

  def getDouble(chemin: String, defaut: Double): Double =
    if (config.hasPath(chemin)) config.getDouble(chemin) else defaut

  def getBoolean(chemin: String, defaut: Boolean): Boolean =
    if (config.hasPath(chemin)) config.getBoolean(chemin) else defaut
}

object ConfigLoader {
  // Charge le fichier application.conf présent dans les ressources
  def apply(): ConfigLoader = new ConfigLoader(ConfigFactory.load())
}

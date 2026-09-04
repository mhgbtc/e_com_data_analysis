package com.ecommerce.analytics

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale

// Structure renvoyée par l'UDF extractTimeFeatures (Question 3.1).
// Les six champs sont imposés par le sujet, ne change ni les noms ni les types.
case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

object TimeFeatures {

  private val formatteur = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  // Période de la journée selon l'heure. Bornes semi-ouvertes, comme le sujet :
  //   Morning   [6h, 12h[      Afternoon [12h, 18h[
  //   Evening   [18h, 22h[     Night     tout le reste (22h -> minuit ET minuit -> 6h)
  private def periode(h: Int): String = {
    if (h >= 6 && h < 12)       "Morning"
    else if (h >= 12 && h < 18) "Afternoon"
    else if (h >= 18 && h < 22) "Evening"
    else                        "Night"
  }

  // Caractéristiques temporelles à partir d'un timestamp au format yyyyMMddHHmmss.
  // Renvoie None si la chaîne est nulle, vide ou mal formée : le sujet exige que
  // le job ne plante pas. None deviendra null côté Spark, la ligne n'est pas perdue.
  def calculer(ts: String): Option[TimeFeatures] = {
    // Garde-fou écrit en premier : on rejette proprement tout ce qui n'est pas
    // exactement 14 chiffres, sans lever d'exception.
    if (ts == null || ts.trim.length != 14 || !ts.forall(_.isDigit)) {
      None
    } else {
      try {
        val dt   = LocalDateTime.parse(ts, formatteur)
        val jour = dt.getDayOfWeek.getValue // 1 = lundi ... 6 = samedi, 7 = dimanche
        Some(TimeFeatures(
          hour             = dt.getHour,
          day_of_week      = dt.getDayOfWeek.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend       = if (jour >= 6) 1 else 0,
          day_period       = periode(dt.getHour),
          is_working_hours = if (dt.getHour >= 9 && dt.getHour <= 17) 1 else 0
        ))
      } catch {
        // Filet de sécurité : même une date impossible (mois 99...) renvoie None.
        case _: Exception => None
      }
    }
  }

  // UDF exposée à Spark. Rien à modifier : Spark déduit le schéma de la structure
  // à partir de la case class TimeFeatures.
  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => calculer(ts))
}

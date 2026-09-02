package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

// Construit la SparkSession à partir des paramètres du fichier de configuration
object SparkSessionBuilder {

  def build(conf: ConfigLoader): SparkSession = {
    val nom               = conf.getString("app.name", "EcommerceAnalytics")
    val master            = conf.getString("app.spark.master", "local[*]")
    val shufflePartitions = conf.getInt("app.spark.shuffle.partitions", 8)

    val spark = SparkSession.builder()
      .appName(nom)
      .master(master)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

name := "EcommerceAnalytics"

version := "1.0.0"

// Scala 2.12 est la version compatible avec Spark 3.3
scalaVersion := "2.12.15"

// Version de Spark retenue par le groupe
val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  // Spark Core et Spark SQL (DataFrame, Dataset, fonctions)
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  // Gestion de la configuration externalisée (application.conf)
  "com.typesafe" % "config" % "1.4.2",
  // Tests unitaires
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

// Point d'entrée de l'application
Compile / mainClass := Some("com.ecommerce.analytics.MainApp")

// Options de compilation
scalacOptions ++= Seq("-deprecation", "-feature", "-encoding", "UTF-8")

// Configuration de la génération du JAR unique (sbt-assembly)
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "EcommerceAnalytics.jar"

// Stratégie de fusion pour éviter les conflits de fichiers lors de l'assemblage
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "module-info.class"      => MergeStrategy.discard
  case x =>
    val ancienne = (assembly / assemblyMergeStrategy).value
    ancienne(x)
}

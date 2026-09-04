name := "EcommerceAnalytics"

version := "1.0.0"

// Scala 2.12 est la version compatible avec Spark 3.3
scalaVersion := "2.12.15"

// Version de Spark retenue par le groupe
val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  // Spark Core et Spark SQL sont déclarés "Provided" : ils sont fournis par le cluster
  // au moment du spark-submit. Cela les retire du JAR, ainsi que toutes leurs
  // dépendances transitives (arrow, jackson, commons-logging...).
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  // Gestion de la configuration externalisée (application.conf)
  "com.typesafe" % "config" % "1.4.2",
  // Tests unitaires
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

// Point d'entrée de l'application
Compile / mainClass := Some("com.ecommerce.analytics.MainApp")

// Les jeux de données ne sont pas embarqués dans le JAR : ils sont lus sur le disque,
// aux chemins déclarés dans application.conf.
Compile / unmanagedResources / excludeFilter := {
  val dossierDonnees = ((Compile / resourceDirectory).value / "data").getCanonicalPath
  new SimpleFileFilter(fichier => fichier.getCanonicalPath.startsWith(dossierDonnees))
}

// Comme Spark est "Provided", il est absent du classpath d'exécution par défaut.
// On redéfinit run pour qu'il utilise le classpath de compilation, qui lui contient Spark.
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// Exécution locale via SBT. Sous Windows, Spark doit trouver hadoop.dll : on transmet
// le chemin au JVM. fork est indispensable pour que javaOptions soit pris en compte.
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "-Djava.library.path=" + sys.env.getOrElse("HADOOP_HOME", "C:/hadoop") + "/bin",
  "-Dfile.encoding=UTF-8"
)

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

# EcommerceAnalytics - Système d'analyse de données e-commerce (Spark & Scala)

Pipeline distribué d'analyse de données e-commerce : ingestion multi-format, validation
et rapport de qualité, transformations avancées (UDF temporelle, jointures, fenêtrage),
analytique business (KPI marchands, cohortes) et optimisations Spark.

- **Scala** : 2.12.15
- **Spark** : 3.3.0 (Spark Core + Spark SQL)
- **Java** : 11 (requis par Spark 3.3)
- **Configuration** : Typesafe Config (`application.conf`)

---

## 1. Prérequis

| Outil | Version | Installation |
|-------|---------|--------------|
| Java (JDK) | 11 | Adoptium/Temurin 11 |
| Scala | 2.12.15 | via SBT ou scala-cli |
| SBT | 1.9.9 | https://www.scala-sbt.org |
| Spark | 3.3.0 | https://archive.apache.org/dist/spark/spark-3.3.0/ |

> **Windows uniquement** : Spark a besoin de `winutils.exe` et `hadoop.dll` (version
> Hadoop 3.3.x) placés dans `%HADOOP_HOME%\bin` (par ex. `C:\hadoop\bin`), et ce dossier
> doit être présent dans le `java.library.path` du JVM (voir la section Exécution locale).

Les quatre jeux de données se trouvent dans `src/main/resources/data/` :
`transactions.csv`, `users.json`, `products.parquet`, `merchants.csv`.

---

## 2. Compilation et génération du JAR

Depuis la racine du projet (`EcommerceAnalytics/`) :

```bash
# Compiler
sbt compile

# Générer le JAR exécutable (fat JAR avec dépendances, via sbt-assembly)
sbt assembly
# => target/scala-2.12/EcommerceAnalytics.jar
```

---

## 3. Exécution locale

### 3.a Avec SBT

```bash
# Pipeline complet
sbt "run all"

# Exécution modulaire par étape
sbt "run ingestion"
sbt "run transformation"
sbt "run analytics"
```

### 3.b Avec scala-cli (sans installer SBT)

Le projet est également exécutable directement avec **scala-cli**, qui compile
l'arborescence `src/main/scala` et charge `application.conf` depuis les ressources :

```bash
scala-cli run src/main/scala \
  -S 2.12.15 --jvm temurin:11 \
  --dep org.apache.spark::spark-sql:3.3.0 \
  --dep com.typesafe:config:1.4.2 \
  --resource-dir src/main/resources \
  --java-opt '-Djava.library.path=C:\hadoop\bin' \
  --server=false -- all
```

> Sur Windows, l'option `--java-opt '-Djava.library.path=C:\hadoop\bin'` est
> indispensable : sans elle, `hadoop.dll` n'est pas chargé et Spark échoue dès la
> lecture du répertoire Parquet ou l'écriture des résultats.

Les résultats sont écrits dans `output/` aux formats **CSV** et **Parquet** :
`rapport_qualite`, `rapport_marchands`, `cohortes`.

---

## 4. Déploiement sur un cluster (spark-submit)

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/EcommerceAnalytics.jar all
```

En local avec le JAR :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  target/scala-2.12/EcommerceAnalytics.jar all
```

L'argument final (`ingestion` | `transformation` | `analytics` | `all`) sélectionne
l'étape à exécuter ; `all` est la valeur par défaut.

---

## 5. Configuration

Tous les paramètres (chemins des données, master Spark, nombre de partitions de shuffle,
seuils de validation, activation du cache et du broadcast, répertoire de sortie) sont
externalisés dans `src/main/resources/application.conf`. Aucune valeur n'est codée en dur
dans le code Scala, et un mécanisme de valeur par défaut est prévu pour chaque clé absente.

Pour comparer les performances avec et sans optimisations (cache + broadcast), basculez :

```
app.optimization.enabled = true   # ou false
```

---

## 6. Structure du projet

```
EcommerceAnalytics/
  build.sbt, project/       Configuration SBT et plugin assembly
  README.md, EQUIPE.md, CONTRIBUTIONS.md
  src/main/scala/com/ecommerce/
    models/                 case classes (Transaction, User, Product, Merchant)
    utils/                  ConfigLoader, SparkSessionBuilder
    analytics/              DataIngestion, DataValidation, DataTransformation,
                            TimeFeatures, Analytics, SparkOptimizations, MainApp
  src/main/resources/
    application.conf
    data/                   transactions.csv, users.json, products.parquet, merchants.csv
```

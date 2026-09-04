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

Spark n'est **pas** inclus dans le JAR : il est déclaré `Provided` dans `build.sbt`, car
il est fourni par le cluster au moment du `spark-submit`. Cette portée écarte aussi toutes
ses dépendances transitives (Arrow, Jackson, commons-logging...).

### 2.a Avec SBT

```bash
sbt compile
```

```bash
sbt assembly
```

Le JAR est écrit dans `target/scala-2.12/EcommerceAnalytics.jar` et pèse environ 5,8 Mo.
Il contient le code du projet, `application.conf`, Typesafe Config et la bibliothèque
Scala.

### 2.b Avec scala-cli, sans installer SBT

C'est la méthode utilisée par le groupe.

Le JAR ne doit contenir que `application.conf`, pas les jeux de données : ceux-ci sont
lus sur le disque, aux chemins déclarés dans la configuration. On prépare donc un dossier
de ressources réduit, dans `target/` qui est ignoré par Git.

```bash
mkdir -p target/pkg-resources && cp src/main/resources/application.conf target/pkg-resources/
```

```bash
scala-cli --power package src/main/scala -S 2.12.15 --jvm temurin:11 --dep org.apache.spark::spark-sql:3.3.0 --dep com.typesafe:config:1.4.2 --resource-dir target/pkg-resources --main-class com.ecommerce.analytics.MainApp --assembly --provided org.apache.spark::spark-sql --preamble=false -o EcommerceAnalytics.jar -f --server=false
```

Le JAR obtenu, `EcommerceAnalytics.jar` à la racine du projet, pèse environ 370 Ko.

> Ce JAR n'est pas exécutable seul avec `java -jar` : la bibliothèque Scala et Spark en
> sont absents volontairement. Il se lance avec `spark-submit` (section 4).

---

## 3. Exécution locale

> **Affichage des accents sous Windows.** Les fichiers produits sont toujours corrects,
> seul l'affichage console peut abîmer les accents. Deux
> réglages sont nécessaires :
>
> - le fichier `.jvmopts` à la racine force le JVM de SBT en UTF-8. Avec `fork`, c'est
>   SBT qui relit la sortie de l'application, donc c'est lui qu'il faut régler ;
> - la console elle-même, à basculer une fois par terminal. Sous PowerShell, `chcp`
>   seul ne suffit pas, il faut aussi forcer l'encodage de sortie :
>
> ```
> chcp 65001; [Console]::OutputEncoding = [Text.Encoding]::UTF8
> ```
>
> Git Bash n'affiche pas correctement ces accents, malgré son support UTF-8 habituel.
> Pour une démonstration, utilisez PowerShell avec la commande ci-dessus.

### 3.a Avec SBT

L'option Windows `java.library.path` est déjà déclarée dans `build.sbt`, il n'y a donc
rien à ajouter sur la ligne de commande.

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
  --java-opt '-Dfile.encoding=UTF-8' \
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
  EcommerceAnalytics.jar all
```

En local avec le JAR :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  EcommerceAnalytics.jar all
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

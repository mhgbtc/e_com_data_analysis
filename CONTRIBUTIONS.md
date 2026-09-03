# Journal de contribution - Groupe 3

## 1. Tableau récapitulatif : question -> responsable -> relecteur

| Question / Partie | Responsable | Relecteur |
|-------------------|-------------|-----------|
| 1.1 Structure SBT | Membre A | Membre C |
| 1.2 `build.sbt` | Membre A | Membre C |
| 1.3 `README.md` | Membre A | Membre C |
| 2.1 Ingestion multi-format | Membre A | Membre B |
| 2.2 Validation des données | Membre A | Membre B |
| 2.3 Gestion d'erreurs et résumé | Membre A | Membre B |
| 2.4 Rapport de qualité | Membre A | Membre B |
| 3.1 UDF `extractTimeFeatures` | Membre B | Membre A |
| 3.2 `enrichTransactionData` | Membre B | Membre A |
| 3.3 Analyse par partition Window | Membre B | Membre A |
| 4.1 KPI par marchand | Membre C | Membre B |
| 4.2 Analyse de cohortes | Membre C | Membre B |
| 5.1 Optimisation du stockage | Membre C | Membres A et B |
| 5.2 Optimisation des jointures | Membre C | Membres A et B |
| 6.1 Application principale | Membre C | Validée par les 3 |
| 7.1 `application.conf` | Membre A | Membre C |

## 2. Charge de travail et difficultés par membre

### Membre A - DJIDOHOKPIN, Samuel
- **Charge estimée** : ~11 h
- **Difficultés rencontrées** :
  - Typage des quatre sources hétérogènes en `Dataset[T]`. Les fichiers contiennent des
    valeurs manquantes, or un `Double` ou un `Int` Scala ne peut pas valoir `null` :
    l'encodeur échouait à la lecture. Résolu en déclarant les champs concernés en
    `Option` dans les case classes.
  - Schéma inféré de `merchants.csv` : Spark devine `establishment_date` comme un entier
    alors que le sujet le décrit comme une chaîne `yyyyMMdd`. Il a fallu réaligner les
    types après la lecture pour que la conversion en `Dataset[Merchant]` aboutisse.
  - Sous Windows, Spark s'arrêtait sur `UnsatisfiedLinkError: NativeIO$Windows.access0`
    dès la lecture du répertoire `products.parquet`. La cause n'était pas l'absence de
    `winutils`, mais le fait que `C:\hadoop\bin` ne figurait pas dans le
    `java.library.path` du JVM : `hadoop.dll` n'était donc jamais chargé. Résolu avec les
    binaires Hadoop 3.3.6 et l'option `-Djava.library.path=C:\hadoop\bin`.
  - Conservation des lignes rejetées avec leur motif plutôt que leur suppression. Deux
    points ont demandé de l'attention : `concat_ws` ignore les valeurs nulles, ce qui
    permet de reconnaître une ligne valide à une chaîne de motifs vide ; et chaque règle
    doit commencer par `isNotNull`, sinon une comparaison portant sur une valeur nulle
    renvoie `null` et non `false`, et la ligne échappe au filtre.

### Membre B - BALDÉ, Azizatou
- **Charge estimée** :
- **Difficultés rencontrées** :
  - Rendre l'UDF robuste aux timestamps nuls, vides ou mal formés.
  - Calcul de l'"utilisateur actif" (>= 5 jours distincts sur une fenêtre glissante de
    7 jours) : passage par les couples `(user_id, jour)` distincts avant comptage.

### Membre C - DIALLO, Cheick Oumar
- **Charge estimée** : ~ 13h
- **Difficultés rencontrées** :
  - Classements par catégorie et par région avec les fonctions de fenêtrage : il a fallu définir correctement les partitions (`partitionBy` catégorie et région) et tris (`orderBy` chiffre d'affaires desc) pour la fonction `dense_rank()`, afin de gérer correctement les ex-æquo éventuels entre marchands.
  - Calcul du `period_index` des cohortes via `months_between` sur des dates tronquées au mois : l'utilisation directe de `months_between` sur les timestamps complets créait des offsets de période (un écart de 30 jours n'étant pas toujours vu comme 1 mois). Résolu en tronquant d'abord les dates de transaction au premier du mois (`date_trunc("month")`) avant de calculer l'écart.
  - Gestion du cache et du `unpersist()` : déterminer le moment optimal pour mettre en cache le DataFrame des transactions enrichies (qui est réutilisé pour les KPI marchands, les cohortes et l'écriture) sans saturer la mémoire, et s'assurer de libérer explicitement l'espace (`unpersist()`) une fois les trois usages terminés.
  - Stratégie de Broadcast : identifier précisément quelles tables (merchants, users, products) étaient suffisamment petites pour justifier un `broadcast()` et éviter ainsi le shuffle réseau lors de la jointure avec le DataFrame des transactions, tout en s'assurant que cela ne provoque pas de OOM (OutOfMemory) sur le driver.
  - Orchestration globale et arrêt propre dans `MainApp.scala` : structurer le pipeline pour exécuter les étapes séquentiellement tout en garantissant que la `SparkSession` est bien arrêtée (`spark.stop()`) dans un bloc `finally`, même en cas d'exception lors de l'ingestion ou de la transformation.
  - Écriture simultanée en CSV et Parquet : gérer les contraintes d'écriture où le format CSV nécessite de regrouper les partitions (`coalesce(1)`) pour faciliter la lecture par l'équipe métier, tandis que le Parquet conserve la distribution pour une réutilisation Spark.

## 3. Décisions techniques du groupe

1. **Spark 3.3.0 + Scala 2.12.15.** Spark 3.3 est une version stable et Scala 2.12 est la
   version binaire compatible (Spark 3.3 ne supporte pas 2.13 par défaut sur toute la
   pile). L'ensemble tourne sous Java 11, requis par Spark 3.3.

2. **Stratégie de jointure : jointures à gauche + broadcast des tables de dimension.**
   Les transactions sont la table pivot ; on utilise des `left join` pour ne perdre aucune
   transaction valide même si une référence (user/product/merchant) est absente. Les tables
   `users`, `products` et `merchants` étant petites, elles sont diffusées (`broadcast`) pour
   éviter le shuffle réseau lors des jointures avec les ~136 000 transactions.

3. **Format de sortie : CSV et Parquet.** Le CSV est directement lisible par une équipe
   métier ; le Parquet conserve les types et est compressé/optimisé pour une réutilisation
   par d'autres traitements Spark. Les deux sont produits dans `output/`.

4. **Case classes avec `Option` pour les champs nullables.** Les fichiers contiennent des
   valeurs manquantes ; typer `age`, `amount`, `rating`, etc. en `Option` permet d'obtenir
   des `Dataset[T]` typés sans faire échouer la lecture, la validation rejetant ensuite les
   lignes incohérentes.

5. **Configuration externalisée (Typesafe Config) avec valeurs par défaut.** Aucun chemin,
   seuil de validation ou paramètre Spark n'est codé en dur ; `ConfigLoader` renvoie une
   valeur par défaut si une clé est absente, ce qui rend le projet robuste et paramétrable.

6. **JAR exécutable via `sbt-assembly` (fat JAR).** Un JAR unique contenant les dépendances
   simplifie l'exécution locale (`java -jar`) et le déploiement `spark-submit`, sans gérer
   un classpath externe.

## 4. Relectures croisées (chaque module relu par un autre membre)

| Date | Module relu | Auteur | Relecteur | Remarques |
|------|-------------|--------|-----------|-----------|
| * | Ingestion + validation (Partie 2) | Membre A | Membre B | * |
| 2026-09-03 | Transformations (Partie 3) | Membre B | Membre A | UDF vérifiée sur chaîne nulle, vide et mal formée : elle renvoie `None` sans interrompre le job. Jointures à gauche cohérentes avec la conservation des transactions dont la référence est orpheline. Bon choix de `otherwise(null)` plutôt que `otherwise("Senior")` : un âge absent ne bascule pas à tort dans la tranche Senior. Libellé "Âge Moyen" identique à celui attendu par le pivot de la Partie 4, la colonne `ca_age_moyen` est bien alimentée. Pipeline complet relancé après fusion : 136 157 lignes enrichies. OK. |
| * | Analytique (Partie 4) | Membre C | Membre B | * |
| 2026-09-03 | Optimisations + MainApp (Parties 5-6) | Membre C | Membre A | `cache()` appliqué au bon endroit, sur le DataFrame enrichi réutilisé trois fois, et `unpersist()` appelé sur les deux chemins de sortie. Le `try / catch / finally` garantit bien `spark.stop()` même en cas d'échec. Deux réserves sans gravité : `persister()` (MEMORY_AND_DISK_SER) est défini mais jamais appelé, il faudra pouvoir le justifier devant le jury ; et l'étape `analytics` enchaîne directement sur l'écriture faute de branchement dédié, elle se comporte donc comme `all`. OK. |
| * | Optimisations + MainApp (Parties 5-6) | Membre C | Membre B | * |
| * | Structure, build, config (Parties 1 et 7) | Membre A | Membre C | * |

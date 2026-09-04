# Journal de contribution - Groupe 7

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
- **Charge estimée** : ~14 h
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
- **Charge estimée** : ~12 h
- **Difficultés rencontrées** :
  - Robustesse de l'UDF `extractTimeFeatures` face aux timestamps invalides. Le sujet impose
    que le job ne s'interrompe pas sur une donnée mal formée, or `LocalDateTime.parse` lève une
    exception sur une chaîne nulle, vide ou non numérique. Résolu par un garde-fou placé avant
    le parsing (`ts == null`, longueur différente de 14, présence de caractères non numériques)
    qui renvoie `None`, doublé d'un `try/catch` renvoyant `None` en dernier recours. L'UDF
    renvoie donc `Option[TimeFeatures]`, ce qui devient `null` côté Spark sans interrompre le
    traitement.
  - Calcul de l'"utilisateur actif" : au moins 5 jours distincts d'activité sur une fenêtre
    glissante de 7 jours. Un `countDistinct` appliqué sur la fenêtre ne compile pas, Spark
    n'autorisant pas les agrégats distincts dans une fonction de fenêtrage. Résolu en réduisant
    d'abord la table aux couples `(user_id, event_date)` distincts — chaque ligne y vaut alors
    exactement un jour actif — puis en comptant les lignes avec `count(lit(1))` sur une fenêtre
    `rangeBetween` de 7 jours, le flag étant ensuite rattaché aux transactions par jointure sur
    `(user_id, event_date)`.
  - Tranche d'âge et valeurs manquantes. Un `otherwise("Senior")` dans `trancheAge` faisait
    basculer à tort les âges absents (utilisateurs rejetés à la validation mais conservés par le
    `left join`) dans la tranche Senior. Résolu en passant "Senior" en condition explicite
    (`user_age >= 65`) et en utilisant `otherwise(null)`, afin qu'un âge manquant reste `null`
    au lieu d'être classé.
### Membre C - DIALLO, Cheick Oumar
- **Charge estimée** : ~ 13h
- **Difficultés rencontrées** :
  - Classements par catégorie et par région avec les fonctions de fenêtrage : il a fallu définir correctement les partitions (`partitionBy` catégorie et région) et tris (`orderBy` chiffre d'affaires desc) pour la fonction `rank()`, afin de gérer correctement les ex aequo éventuels entre marchands.
  - Calcul du `period_index` des cohortes via `months_between` sur des dates tronquées au mois : l'utilisation directe de `months_between` sur les timestamps complets créait des offsets de période (un écart de 30 jours n'étant pas toujours vu comme 1 mois). Résolu en tronquant d'abord les dates de transaction au premier du mois (`trunc(col("event_date"), "month")`) avant de calculer l'écart.
  - Gestion du cache et du `unpersist()` : déterminer le moment optimal pour conserver le DataFrame des transactions enrichies (qui est réutilisé pour les KPI marchands, les cohortes et l'écriture) sans saturer la mémoire, ce qui a conduit à le persister en mémoire et sur disque plutôt qu'à le mettre simplement en cache, et s'assurer de libérer explicitement l'espace (`unpersist()`) une fois les trois usages terminés.
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

6. **JAR d'assemblage sans Spark, exécuté via `spark-submit`.** Le sujet laisse le choix
   entre `assembly` et `package`. Nous avons retenu un JAR d'assemblage contenant le code
   du projet, `application.conf` et Typesafe Config, mais **pas** Spark, déclaré comme
   fourni. Spark est en effet apporté par le cluster au moment du `spark-submit` :
   l'embarquer porterait le livrable à environ 250 Mo et rendrait l'archive impossible à
   envoyer par courriel. Le JAR final pèse 369 Ko. Dans `build.sbt`, Spark est déclaré
   `Provided` : `sbt assembly` produit alors un JAR de 5,8 Mo, débarrassé de Spark et de
   toutes ses dépendances transitives. La tâche `run` y est redéfinie pour utiliser le
   classpath de compilation, ce qui permet à `sbt run` de fonctionner malgré cette portée.

## 4. Relectures croisées (chaque module relu par un autre membre)

| Date | Module relu | Auteur | Relecteur | Remarques |
|------|-------------|--------|-----------|-----------|
| 2026-09-04 | Ingestion + validation (Partie 2) | Membre A | Membre B | Code clair. Les schémas sont définis explicitement et les chemins viennent de la config, pas codés en dur. La validation est bien pensée : une liste de règles avec le motif de rejet, et les lignes rejetées sont conservées avec leur raison au lieu d'être supprimées. Seule remarque : merchants est lu avec inferSchema alors que les autres sources ont un schéma explicite, ce serait plus cohérent de faire pareil. Rien de bloquant, OK. |
| 2026-09-03 | Transformations (Partie 3) | Membre B | Membre A | UDF vérifiée sur chaîne nulle, vide et mal formée : elle renvoie `None` sans interrompre le job. Jointures à gauche cohérentes avec la conservation des transactions dont la référence est orpheline. Bon choix de `otherwise(null)` plutôt que `otherwise("Senior")` : un âge absent ne bascule pas à tort dans la tranche Senior. Libellé "Âge Moyen" identique à celui attendu par le pivot de la Partie 4, la colonne `ca_age_moyen` est bien alimentée. Pipeline complet relancé après fusion : 136 157 lignes enrichies. OK. |
| 2026-09-04 | Analytique (Partie 4) | Membre C | Membre B | Les KPI par marchand sont complets (CA, nombre de transactions, clients uniques, panier moyen, commission). Les classements par catégorie et par région utilisent rank(), ce qui gère bien les égalités. Bon point : le pivot par tranche d'âge reprend exactement mes libellés (Jeune, Adulte, Âge Moyen, Senior), donc ça s'emboîte bien avec ma partie. Pour les cohortes, les dates sont arrondies au mois avant de calculer l'écart, sinon le period_index serait faux. Rien de bloquant, OK. |
| 2026-09-03 | Optimisations + MainApp (Parties 5-6) | Membre C | Membre A | Le `try / catch / finally` garantit `spark.stop()` même en cas d'échec, et le code de sortie non nul a bien été ajouté. **Défaut bloquant relevé et corrigé** : `exitCode` était déclaré `val` puis réaffecté dans le `catch`, ce qui empêchait la compilation de tout le projet. Passé en `var`, `sbt compile` repasse au vert. **Écart avec la Question 5.1 relevé et comblé** : `persister()` existait mais n'était jamais appelé ; il porte désormais le DataFrame enrichi, le plus volumineux, tandis que `cache()` porte la matrice de cohortes, plus petite mais relue trois fois. `unpersist()` libère les deux en fin de pipeline. Réserve restante sans gravité : l'étape `analytics` n'a pas de branchement dédié, elle se comporte donc comme `all`. OK après corrections. |
| * | Optimisations + MainApp (Parties 5-6) | Membre C | Membre B | * |
| 2026-09-04 | Structure, build, config (Parties 1 et 7) | Membre A | Membre C | Structure SBT claire, découpée en paquets models, utils et analytics. `build.sbt` déclare toutes les dépendances nécessaires (Spark, Typesafe Config, scalatest) et le plugin `sbt-assembly` est correctement configuré pour produire un JAR d'assemblage sans Spark, celui-ci étant fourni par le cluster. Le `README.md` décrit précisément les prérequis, le lancement et les sorties, ce qui facilite la prise en main. Le fichier `application.conf` est bien externalisé, avec des valeurs par défaut cohérentes ; la classe `ConfigLoader` gère proprement les clés manquantes. Seule réserve mineure : l'absence d'exemples de configurations alternatives dans le README pourrait être améliorée, mais le code reste robuste. OK. |

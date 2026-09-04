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
- **Charge estimée** : ~13 h
- **Difficultés rencontrées** :
  - Typage des sources hétérogènes en `Dataset[T]` : usage d'`Option` dans les case
    classes pour absorber les valeurs manquantes sans faire échouer l'encodeur.
  - Sous Windows, chargement de `hadoop.dll` (Hadoop 3.3.x) pour lire le répertoire
    Parquet et écrire les résultats - résolu via `-Djava.library.path`.
  - Conservation des lignes rejetées avec un motif (`rejection_reason`) plutôt que de
    les supprimer.

### Membre B - BALDÉ, Azizatou
- **Charge estimée** : ~13 h
- **Difficultés rencontrées** :
  - Rendre l'UDF robuste aux timestamps nuls, vides ou mal formés.
  - Calcul de l'"utilisateur actif" (>= 5 jours distincts sur une fenêtre glissante de
    7 jours) : passage par les couples `(user_id, jour)` distincts avant comptage.

### Membre C - DIALLO, Cheick Oumar
- **Charge estimée** : ~13 h
- **Difficultés rencontrées** :
  - Classements par catégorie et par région avec les fonctions de fenêtrage.
  - Calcul du `period_index` des cohortes via `months_between` sur des dates tronquées
    au mois.

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
| 2026-08-26 | Ingestion + validation (Partie 2) | Membre A | Membre B | Schémas explicites conformes ; motifs de rejet clairs. OK. |
| 2026-08-27 | Transformations (Partie 3) | Membre B | Membre A | UDF robuste aux valeurs mal formées ; fenêtres correctes. OK. |
| 2026-08-27 | Analytique (Partie 4) | Membre C | Membre B | KPI et cohortes cohérents avec les données. OK. |
| 2026-08-28 | Optimisations + MainApp (Parties 5-6) | Membre C | Membres A et B | Cache/broadcast pilotés par la config ; arrêt propre de la session. OK. |
| 2026-09-04 | Structure, build, config (Parties 1 et 7) | Membre A | Membre C | Structure SBT claire avec multi-module bien découpé. `build.sbt` déclare toutes les dépendances nécessaires (Spark, Typesafe Config, scalatest) et le plugin `sbt-assembly` est correctement configuré pour produire un fat JAR. Le `README.md` décrit précisément les prérequis, le lancement et les sorties, ce qui facilite la prise en main. Le fichier `application.conf` est bien externalisé, avec des valeurs par défaut cohérentes ; la classe `ConfigLoader` gère proprement les clés manquantes. Seule réserve mineure : l'absence d'exemples de configurations alternatives dans le README pourrait être améliorée, mais le code reste robuste. OK. |

> Les champs entre ... (noms, charges, dates, remarques) sont à confirmer par le groupe.

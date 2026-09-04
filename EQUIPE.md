# Équipe - Projet Final Spark & Scala (Groupe 7)

Dépôt : https://github.com/mhgbtc/e_com_data_analysis

Projet réalisé en groupe de 3 étudiants. Chaque membre est propriétaire d'un lot de
travail ; le groupe reste responsable de l'intégration et de la cohérence de l'ensemble.

## Membres et rôles

| Membre | Nom, prénom | Adresse e-mail | Rôle | `git config user.name` |
|--------|-------------|----------------|------|------------------------|
| **A** | DJIDOHOKPIN, Samuel | msd.bytes@gmail.com | Data Ingestion & Platform Engineer | Samuel DJIDOHOKPIN |
| **B** | BALDÉ, Azizatou | baldeazizatou@gmail.com | Data Transformation Engineer | Azizatou BALDE |
| **C** | DIALLO, Cheick Oumar | cheickoumardiallo990@gmail.com | Analytics & Performance Engineer | Cheick-o-diallo |

> Rôles imposés par le sujet (Partie 0.1). Un même rôle ne peut pas être occupé par deux personnes.

## Répartition nominative des questions

### Membre A - Data Ingestion & Platform Engineer (Parties 1, 2 et 7)
- Question 1.1 - Structure de projet SBT
- Question 1.2 - Configuration `build.sbt`
- Question 1.3 - Documentation `README.md`
- Question 2.1 - Ingestion multi-format (case classes, `DataIngestion.scala`)
- Question 2.2 - Validation des données (`DataValidation.scala`)
- Question 2.3 - Gestion d'erreurs et résumé
- Question 2.4 - Rapport de qualité des données
- Partie 7 - Configuration externalisée (`application.conf`, `ConfigLoader`)

### Membre B - Data Transformation Engineer (Partie 3)
- Question 3.1 - UDF `extractTimeFeatures` (`TimeFeatures.scala`)
- Question 3.2 - Fonction `enrichTransactionData` (jointures, fenêtrage, tranche d'âge)
- Question 3.3 - Analyse par partition Window (montant cumulé, utilisateur actif, délai)

### Membre C - Analytics & Performance Engineer (Parties 4, 5 et 6)
- Question 4.1 - Rapport détaillé par marchand (`Analytics.scala`)
- Question 4.2 - Analyse de cohortes utilisateurs
- Question 5.1 - Optimisation du stockage (`SparkOptimizations.scala`)
- Question 5.2 - Optimisation des jointures (broadcast, shuffle partitions)
- Question 6.1 - Application principale `MainApp.scala`

### Collectif (Parties 8 et 9)
Tests, qualité, documentation et soutenance : chaque membre contribue pour la portion de
code dont il est propriétaire.

## Fichiers par propriétaire (limite les conflits Git)

| Fichier | Propriétaire |
|---------|--------------|
| `models/*.scala`, `DataIngestion.scala`, `DataValidation.scala`, `utils/*`, `application.conf`, `build.sbt`, `README.md` | Membre A |
| `TimeFeatures.scala`, `DataTransformation.scala` | Membre B |
| `Analytics.scala`, `SparkOptimizations.scala`, `MainApp.scala` | Membre C |

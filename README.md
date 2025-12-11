# Prédiction de Trafic Cycliste - Montpellier Méditerranée Métropole

Ce projet s'inscrit dans le cadre de la formation Développeur IA. Il vise à développer une solution complète (Data Engineering, Machine Learning, Développement Web) capable de prédire l'affluence cycliste heure par heure pour le lendemain (J+1) sur les points stratégiques de la métropole de Montpellier.

## Contexte du Projet

La Métropole de Montpellier met à disposition la Data de ses compteurs vélos via des appels API.

* **Réseau :** Un total de 54 compteurs sont proposés, placés en majorité sur des aménagements cyclables.
* **Historique :** Données disponibles depuis le 01/01/2023.
* **Fréquence :** Relevés horaires, avec publication quotidienne des chiffres de la veille.
* **Contrainte :** Chaque compteur est indépendant, avec une date de mise en service et des dates d'absence de data différentes.

## Stratégie et Méthodologie

Le défi principal réside dans la disparité de la qualité des données. Certains capteurs présentent jusqu'à 83% de données manquantes ("arrêt") sur la période totale.

**Notre approche :**
Nous avons choisi d'identifier les compteurs les plus fiables pour garantir la robustesse du modèle.

1.  **Analyse de disponibilité :** Calcul du taux de présence de données pour chaque compteur depuis janvier 2023.
2.  **Sélection :** Identification du **Top 10** des compteurs ayant un taux d'arrêt inférieur à 3,2% sur la période totale.
3.  **Objectif :** Prédire l'affluence horaire uniquement sur ce Top 10 fiable.

## Architecture des Données

Pour centraliser et structurer l'information, nous avons mis en place une architecture basée sur **Supabase** (PostgreSQL) comprenant 8 tables distinctes. Cette organisation permet de :
* Centraliser les informations nécessaires à chaque étape du processus.
* Faciliter l'analyse et la prise de décision.
* Identifier les données clés à stocker pour l'entraînement et la prédiction.

Les flux de données sont gérés par des pipelines ETL (Extract, Transform, Load) distincts pour la météo, le calendrier et l'historique des compteurs.

## Stack Technique

* **Langage :** Python 3.12
* **Gestionnaire de dépendances :** uv
* **Base de données :** Supabase (PostgreSQL)
* **Machine Learning :** XGBoost (Régression), Scikit-Learn
* **Backend / API :** FastAPI
* **Frontend :** HTML5, CSS3, JavaScript (Leaflet.js, Chart.js)
* **Environnement :** Linux, Docker, Azure

## Structure du Projet

```text
.
├── README.md
├── backend/                  # Configuration Docker Backend
│   ├── Dockerfile
│   ├── api_server.py
│   └── requirements.txt
├── frontend/                 # Application Web et API Frontend
│   ├── Dockerfile
│   ├── api_server.py         # Serveur FastAPI pour servir le front
│   ├── index.html
│   └── assets/
│       ├── css/
│       ├── js/
│       └── data/
├── data/                     # Stockage local temporaire
│   ├── dataset_final_training (1).csv
│   └── raw/
├── data_calendrier/          # ETL Données Calendaires
│   ├── api.py
│   ├── clean.py
│   ├── main.py
│   └── pipeline.py
├── data_meteo/               # ETL Données Météo
│   ├── meteo.py
│   ├── pipeline.py
│   └── supabase_client.py
├── preparation_counters_forecast/ # ETL Préparation Prédiction J+1
│   ├── config.py
│   ├── extract.py            # Récupération Météo J+1 & Calendrier
│   ├── transform.py          # Création features
│   ├── load.py               # Envoi vers Supabase
│   └── run_pipeline.py       # Orchestration
├── train_model_xgboost/      # Pipeline Machine Learning
│   ├── config.py
│   ├── loader.py             # Chargement & Feature Engineering
│   ├── trainer.py            # Entraînement XGBoost
│   ├── evaluator.py          # Calcul MAE & Graphiques
│   ├── saver.py              # Sauvegarde .joblib
│   ├── pipeline_train.py     # Script d'entraînement
│   └── artifacts/            # Modèles sauvegardés
├── src/                      # Scripts utilitaires et API interne
│   └── api/
└── requirements.txt
```

# Run project Docker
```bash
docker compose build
docker compose up
```

# Azure
```bash
## frontend: https://montpellierfrontend-kirillsst-hvemarbcb7gpc7dj.francecentral-01.azurewebsites.net/
## backend: https://montpellierbackend-kirillsst-hfd9e2adfqfxgnbk.francecentral-01.azurewebsites.net/
```

## Installation et Utilisation

Ce projet utilise `uv` pour la gestion rapide de l'environnement virtuel.

### 1. Prérequis

* Avoir `uv` installé sur votre machine.
* Disposer d'un fichier `.env` à la racine contenant les identifiants Supabase (`SUPABASE_URL`, `SUPABASE_KEY`).

### 2. Workflow Complet

Le projet fonctionne en trois étapes principales : Entraînement, Préparation, Prédiction/Visualisation.

#### Étape A : Entraînement du Modèle (Optionnel)
Si vous souhaitez ré-entraîner les modèles sur de nouvelles données historiques :

```bash
uv run python -m train_model_xgboost.pipeline_train


Étape B : Prédiction pour une date future
Pour générer les prédictions (par exemple pour le 26 novembre 2025), il faut d'abord préparer les données d'entrée (météo, calendrier) puis lancer l'inférence.

Préparation des données (ETL) :

Bash

uv run python -m preparation_counters_forecast.run_pipeline --date 2025-11-26
Exécution de la prédiction :

Bash

uv run predict_hourly.py
Étape C : Visualisation (Application Web)
L'application expose un tableau de bord interactif (Carte + Graphiques).

Lancez le serveur de développement :

Bash

uv run fastapi dev frontend/api_server.py --port 8001
Accédez ensuite à l'application via votre navigateur : https://www.google.com/search?q=http://127.0.0.1:8001

Fonctionnalités du Dashboard
Carte Interactive : Visualisation géolocalisée des 10 compteurs stratégiques.

Prévisions Horaires : Affichage des courbes de trafic prédites pour la journée cible.

Analyse Historique : Consultation des statistiques passées (KPI, impact météo, évolutions).

Indicateurs de performance : Code couleur sur la carte indiquant la charge prévue des pistes cyclables.




# Dashboard de Visualisation des Données Immobilières

Ce module fournit un dashboard interactif pour visualiser les données immobilières provenant des datamarts MySQL. Il permet d'explorer les tendances de prix, les facteurs d'impact et les prédictions de prix.

## Fonctionnalités

Le dashboard offre les visualisations suivantes :

1. **Carte des prix par région/quartier** : Visualisation géographique des prix immobiliers.
2. **Tendances de prix au fil du temps** : Graphiques d'évolution des prix et volumes de transactions.
3. **Importance des caractéristiques** : Visualisation des facteurs qui influencent le plus les prix.
4. **Prédictions de prix** : Affichage des prédictions avec intervalles de confiance.
5. **Comparaison de quartiers** : Outils de comparaison entre différents quartiers.

## Structure des fichiers

```
visualization/
├── app.py                  # Point d'entrée principal du dashboard
├── utils/
│   └── db_connector.py     # Classe de connexion à la base de données
└── components/
    ├── price_map.py        # Composant pour la carte des prix
    ├── price_trends.py     # Composant pour les tendances de prix
    ├── feature_impact.py   # Composant pour l'importance des caractéristiques
    ├── predictions.py      # Composant pour les prédictions
    └── neighborhood.py     # Composant pour la comparaison des quartiers
```

## Prérequis

- Python 3.8+
- MySQL (avec les datamarts créés via les scripts ETL)
- Les packages Python listés dans `requirements.txt`

## Installation

1. Installer les dépendances :
   ```
   pip install -r requirements.txt
   ```

2. Configurer les paramètres de connexion à la base de données :
   Modifier les paramètres dans le fichier `app.py` si nécessaire.

## Utilisation

Pour lancer le dashboard :

```bash
cd visualization
streamlit run app.py
```

Le dashboard sera accessible via votre navigateur à l'adresse http://localhost:8501

## Fonctionnalités de filtrage

Le dashboard offre plusieurs options de filtrage des données :

- Par région
- Par quartier
- Par période temporelle
- Par type de propriété

## Optimisations

- Mise en cache des requêtes fréquentes pour améliorer les performances
- Interface utilisateur responsive adaptée à différentes tailles d'écran
- Gestion des erreurs de connexion à la base de données
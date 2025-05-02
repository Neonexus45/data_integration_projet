#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de prédiction des prix immobiliers.
Ce script implémente un modèle de machine learning pour prédire les prix immobiliers
à partir des features de la table price_prediction_features.

Auteur: IA Roo
Date: 02/05/2025
Version: 1.0
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import traceback
import json
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import uuid

# Imports PySpark pour charger les données Parquet
from pyspark.sql import SparkSession

# Imports scikit-learn pour le machine learning
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, explained_variance_score
from sklearn.inspection import permutation_importance

# Configuration du logging
log_dir = "machine_learning"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "housing_price_prediction.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HousingPricePredictor:
    """
    Classe principale pour la prédiction des prix immobiliers.
    
    Cette classe est responsable de:
    - Charger les données depuis la couche Gold
    - Préparer les données pour l'entraînement
    - Entraîner et évaluer plusieurs modèles ML
    - Sauvegarder le meilleur modèle
    - Enregistrer les métadonnées et résultats dans MySQL
    """
    
    def __init__(self, gold_path='data/gold', output_dir='machine_learning/models',
                 host='localhost', user='tatane', password='tatane', 
                 database='immobilier_prediction_db'):
        """
        Initialisation du prédicteur de prix immobiliers.
        
        Args:
            gold_path (str): Chemin vers les données de la couche Gold
            output_dir (str): Répertoire pour sauvegarder les modèles
            host (str): Hôte du serveur MySQL
            user (str): Utilisateur MySQL
            password (str): Mot de passe MySQL
            database (str): Nom de la base de données MySQL
        """
        self.gold_path = gold_path
        self.output_dir = output_dir
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        # Création du répertoire de sortie s'il n'existe pas
        os.makedirs(output_dir, exist_ok=True)
        
        # Attributs initialisés plus tard
        self.spark = None
        self.df = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.best_model = None
        self.best_model_name = None
        self.feature_importance = None
        self.preprocessing_pipeline = None
        
        # Dictionnaire pour stocker les modèles et leurs performances
        self.models = {}
        self.model_metrics = {}
        
        # Métadonnées du modèle
        self.model_id = f"model-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}"
        self.model_version = "v1.0"
        
        logger.info("Prédicteur de prix immobiliers initialisé")
    
    def initialize_spark(self):
        """
        Initialise une session Spark pour lire les fichiers Parquet.
        
        Returns:
            SparkSession: Session Spark configurée
        """
        logger.info("Initialisation de la session Spark...")
        
        try:
            # Configuration de Spark avec support Parquet et optimisations
            self.spark = (SparkSession.builder
                .appName("HousingPricePredictor")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.shuffle.partitions", "3")
                .config("spark.executor.memory", "1g")
                .config("spark.driver.memory", "1g")
                .config("spark.local.dir", "./spark-temp")
                .getOrCreate())
            
            # Création du répertoire temporaire si nécessaire
            os.makedirs("./spark-temp", exist_ok=True)
            
            # Configuration des niveaux de log pour réduire le bruit
            self.spark.sparkContext.setLogLevel("WARN")
            
            # Vérifier la version de Spark
            spark_version = self.spark.version
            logger.info(f"Session Spark initialisée avec succès (version: {spark_version})")
            
            return self.spark
            
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation de Spark: {e}")
            traceback.print_exc()
            raise Exception(f"Échec de l'initialisation de Spark: {e}")
    
    def connect_to_mysql(self):
        """
        Établit une connexion à MySQL.
        
        Returns:
            tuple: (connection, cursor) si la connexion est réussie, (None, None) sinon
        """
        logger.info(f"Connexion à MySQL ({self.host}, {self.user}, {self.database})...")
        
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = conn.cursor()
            logger.info("Connexion à MySQL réussie")
            return conn, cursor
            
        except Error as e:
            logger.error(f"Erreur lors de la connexion à MySQL: {e}")
            traceback.print_exc()
            return None, None
    
    def disconnect_from_mysql(self, conn, cursor):
        """
        Ferme la connexion à MySQL.
        
        Args:
            conn: Connexion MySQL
            cursor: Curseur MySQL
        """
        if cursor:
            cursor.close()
        
        if conn:
            conn.close()
            logger.info("Connexion MySQL fermée")
    
    def load_data(self):
        """
        Charge les données de prix_prediction_features de la couche Gold.
        
        Returns:
            bool: True si les données ont été chargées avec succès, False sinon
        """
        logger.info("Chargement des données price_prediction_features...")
        
        try:
            # Initialiser Spark
            if not self.spark:
                self.initialize_spark()
            
            # Construire le chemin vers les données les plus récentes
            current_date = datetime.now()
            year = current_date.year
            month = current_date.month
            day = current_date.day
            
            # Chemin avec partitionnement pour la date actuelle
            base_path = f"{self.gold_path}/price_prediction_features"
            latest_path = f"{base_path}/year={year}/month={month:02d}/day={day:02d}"
            
            # Vérifier si le chemin existe
            if not os.path.exists(latest_path):
                logger.info(f"Aucune donnée trouvée à la date actuelle ({latest_path})")
                
                # Explorer pour trouver les données les plus récentes
                found_paths = []
                for root, dirs, files in os.walk(base_path):
                    # Vérifier si le répertoire contient des fichiers Parquet
                    parquet_files = [f for f in files if f.endswith('.parquet')]
                    if parquet_files:
                        found_paths.append(root)
                
                if not found_paths:
                    logger.error("Aucune donnée trouvée dans price_prediction_features")
                    return False
                
                # Trier les chemins par date de modification (du plus récent au plus ancien)
                found_paths.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                latest_path = found_paths[0]
            
            logger.info(f"Chargement des données depuis {latest_path}")
            
            # Lister les fichiers .parquet dans le répertoire
            parquet_files = []
            for root, dirs, files in os.walk(latest_path):
                parquet_files.extend([os.path.join(root, f) for f in files 
                                    if f.endswith('.parquet') and not f.startswith('.')])
            
            if not parquet_files:
                logger.error(f"Aucun fichier Parquet trouvé dans {latest_path}")
                return False
            
            # Lire les fichiers Parquet avec Spark
            spark_df = self.spark.read.parquet(*parquet_files)
            
            # Convertir en DataFrame pandas pour faciliter le traitement ML
            self.df = spark_df.toPandas()
            
            # Afficher les informations sur les données chargées
            num_rows = len(self.df)
            num_cols = len(self.df.columns)
            logger.info(f"Données chargées avec succès: {num_rows} lignes, {num_cols} colonnes")
            logger.info(f"Colonnes disponibles: {', '.join(self.df.columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            traceback.print_exc()
            return False
    
    def prepare_data(self, target_column='target_price', test_size=0.2, random_state=42):
        """
        Prépare les données pour l'entraînement des modèles.
        
        Args:
            target_column (str): Nom de la colonne cible (prix)
            test_size (float): Proportion des données pour le test
            random_state (int): Seed pour la reproductibilité
            
        Returns:
            bool: True si la préparation a réussi, False sinon
        """
        logger.info("Préparation des données pour l'entraînement...")
        
        try:
            if self.df is None:
                logger.error("Aucune donnée à préparer, appeler load_data() d'abord")
                return False
            
            # Vérifier que la colonne cible existe
            if target_column not in self.df.columns:
                logger.error(f"La colonne cible '{target_column}' n'existe pas dans les données")
                return False
            
            # Analyser les données manquantes
            missing_values = self.df.isnull().sum()
            missing_percent = (missing_values / len(self.df)) * 100
            
            logger.info("Analyse des valeurs manquantes:")
            for col, missing in missing_values.items():
                if missing > 0:
                    logger.info(f"  - {col}: {missing} valeurs manquantes ({missing_percent[col]:.2f}%)")
            
            # Séparer les features et la cible
            X = self.df.drop(columns=[target_column])
            y = self.df[target_column]
            
            # Identifier les types de colonnes pour le prétraitement
            numeric_features = X.select_dtypes(include=['int64', 'float64']).columns.tolist()
            categorical_features = X.select_dtypes(include=['object', 'category']).columns.tolist()
            
            logger.info(f"Features numériques ({len(numeric_features)}): {', '.join(numeric_features)}")
            logger.info(f"Features catégorielles ({len(categorical_features)}): {', '.join(categorical_features)}")
            
            # Définir les transformations pour chaque type de colonne
            numeric_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler())
            ])
            
            categorical_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(handle_unknown='ignore'))
            ])
            
            # Combinaison des transformateurs dans un ColumnTransformer
            self.preprocessing_pipeline = ColumnTransformer(
                transformers=[
                    ('num', numeric_transformer, numeric_features),
                    ('cat', categorical_transformer, categorical_features)
                ])
            
            # Division en ensembles d'entraînement et de test
            self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            
            logger.info(f"Données divisées en {len(self.X_train)} exemples d'entraînement et {len(self.X_test)} exemples de test")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la préparation des données: {e}")
            traceback.print_exc()
            return False
    
    def train_linear_regression(self):
        """
        Entraîne un modèle de régression linéaire.
        
        Returns:
            tuple: (modèle, temps d'entraînement en secondes)
        """
        logger.info("Entraînement du modèle de régression linéaire...")
        
        start_time = datetime.now()
        
        # Création du pipeline complet avec prétraitement et modèle
        pipeline = Pipeline([
            ('preprocessor', self.preprocessing_pipeline),
            ('regressor', LinearRegression())
        ])
        
        # Entraînement du modèle
        pipeline.fit(self.X_train, self.y_train)
        
        end_time = datetime.now()
        training_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Modèle de régression linéaire entraîné en {training_time:.2f} secondes")
        
        return pipeline, training_time
    
    def train_random_forest(self):
        """
        Entraîne un modèle Random Forest avec recherche d'hyperparamètres.
        
        Returns:
            tuple: (modèle, temps d'entraînement en secondes)
        """
        logger.info("Entraînement du modèle Random Forest...")
        
        start_time = datetime.now()
        
        # Création du pipeline avec prétraitement
        pipeline = Pipeline([
            ('preprocessor', self.preprocessing_pipeline),
            ('regressor', RandomForestRegressor(random_state=42))
        ])
        
        # Définition des hyperparamètres à tester
        param_grid = {
            'regressor__n_estimators': [50, 100],
            'regressor__max_depth': [None, 10, 20],
            'regressor__min_samples_split': [2, 5]
        }
        
        # Recherche d'hyperparamètres par validation croisée
        grid_search = GridSearchCV(
            pipeline, param_grid, cv=3, n_jobs=-1, scoring='neg_mean_squared_error'
        )
        
        # Entraînement du modèle
        grid_search.fit(self.X_train, self.y_train)
        
        end_time = datetime.now()
        training_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Meilleurs paramètres pour Random Forest: {grid_search.best_params_}")
        logger.info(f"Modèle Random Forest entraîné en {training_time:.2f} secondes")
        
        return grid_search.best_estimator_, training_time
    
    def train_gradient_boosting(self):
        """
        Entraîne un modèle Gradient Boosting avec recherche d'hyperparamètres.
        
        Returns:
            tuple: (modèle, temps d'entraînement en secondes)
        """
        logger.info("Entraînement du modèle Gradient Boosting...")
        
        start_time = datetime.now()
        
        # Création du pipeline avec prétraitement
        pipeline = Pipeline([
            ('preprocessor', self.preprocessing_pipeline),
            ('regressor', GradientBoostingRegressor(random_state=42))
        ])
        
        # Définition des hyperparamètres à tester
        param_grid = {
            'regressor__n_estimators': [50, 100],
            'regressor__learning_rate': [0.05, 0.1],
            'regressor__max_depth': [3, 5]
        }
        
        # Recherche d'hyperparamètres par validation croisée
        grid_search = GridSearchCV(
            pipeline, param_grid, cv=3, n_jobs=-1, scoring='neg_mean_squared_error'
        )
        
        # Entraînement du modèle
        grid_search.fit(self.X_train, self.y_train)
        
        end_time = datetime.now()
        training_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Meilleurs paramètres pour Gradient Boosting: {grid_search.best_params_}")
        logger.info(f"Modèle Gradient Boosting entraîné en {training_time:.2f} secondes")
        
        return grid_search.best_estimator_, training_time
    
    def evaluate_model(self, model, model_name):
        """
        Évalue les performances d'un modèle sur l'ensemble de test.
        
        Args:
            model: Modèle entraîné à évaluer
            model_name (str): Nom du modèle
            
        Returns:
            dict: Métriques d'évaluation
        """
        logger.info(f"Évaluation du modèle {model_name}...")
        
        # Prédictions sur l'ensemble de test
        y_pred = model.predict(self.X_test)
        
        # Calcul des métriques
        rmse = np.sqrt(mean_squared_error(self.y_test, y_pred))
        mae = mean_absolute_error(self.y_test, y_pred)
        r2 = r2_score(self.y_test, y_pred)
        explained_var = explained_variance_score(self.y_test, y_pred)
        
        # Stockage des métriques
        metrics = {
            'rmse': rmse,
            'mae': mae,
            'r2': r2,
            'explained_variance': explained_var
        }
        
        logger.info(f"Métriques pour {model_name}:")
        logger.info(f"  - RMSE: {rmse:.2f}")
        logger.info(f"  - MAE: {mae:.2f}")
        logger.info(f"  - R²: {r2:.4f}")
        logger.info(f"  - Variance expliquée: {explained_var:.4f}")
        
        return metrics
    
    def get_feature_importance(self, model, features):
        """
        Calcule l'importance des features pour un modèle donné.
        Préserve les noms originaux des features après transformation.
        
        Args:
            model: Modèle entraîné
            features (list): Liste des noms de features
            
        Returns:
            dict: Importance relative de chaque feature
        """
        logger.info("Calcul de l'importance des features...")
        
        try:
            # Extraction du modèle réel à partir du pipeline
            regressor = model.named_steps['regressor']
            preprocessor = model.named_steps['preprocessor']
            
            # Méthode 1: Utiliser permutation_importance (fonctionne pour tout type de modèle)
            # Cette méthode utilise directement le pipeline complet et les noms originaux
            result = permutation_importance(model, self.X_test, self.y_test, n_repeats=10, random_state=42)
            
            # Créer un dictionnaire d'importance avec les noms originaux des features
            importance_dict = dict(zip(self.X_test.columns, result.importances_mean))
            
            # Alternative 2: Pour les modèles avec feature_importances_ ou coef_ natifs
            # Utilisé si la première méthode produit des résultats incohérents
            if hasattr(regressor, 'feature_importances_') or hasattr(regressor, 'coef_'):
                # Récupérer la structure des transformateurs
                numeric_idx = None
                categorical_idx = None
                
                for i, (name, _, cols) in enumerate(preprocessor.transformers_):
                    if name == 'num':
                        numeric_idx = i
                    elif name == 'cat':
                        categorical_idx = i
                
                # Créer un dictionnaire pour agréger l'importance par feature originale
                original_feature_importance = {}
                
                # Traiter selon le type de modèle
                if hasattr(regressor, 'feature_importances_'):
                    importances = regressor.feature_importances_
                elif hasattr(regressor, 'coef_'):
                    # Normaliser les coefficients en valeur absolue
                    importances = np.abs(regressor.coef_)
                    importances = importances / np.sum(importances)
                
                # Récupérer les transformateurs spécifiques
                feature_names_out = []
                idx_start = 0
                
                # Traiter les features numériques (mapping direct)
                if numeric_idx is not None:
                    num_transformer = preprocessor.transformers_[numeric_idx][1]
                    num_features = preprocessor.transformers_[numeric_idx][2]
                    num_count = len(num_features)
                    
                    for i, feature in enumerate(num_features):
                        if idx_start + i < len(importances):
                            original_feature_importance[feature] = importances[idx_start + i]
                    
                    idx_start += num_count
                
                # Traiter les features catégorielles (agréger les one-hot encodées)
                if categorical_idx is not None:
                    cat_transformer = preprocessor.transformers_[categorical_idx][1]
                    cat_features = preprocessor.transformers_[categorical_idx][2]
                    
                    # Recréer les noms de features après one-hot encoding
                    # Note: cette approche est une approximation mais tente de conserver la traçabilité
                    for cat in cat_features:
                        cat_values = self.df[cat].dropna().unique()
                        feature_importance_sum = 0
                        
                        # Compter combien de versions one-hot de cette feature catégorielle existent
                        one_hot_count = len(cat_values)
                        if idx_start + one_hot_count <= len(importances):
                            # Somme des importances pour toutes les versions de cette feature
                            feature_importance_sum = sum(importances[idx_start:idx_start+one_hot_count])
                            original_feature_importance[f"{cat}"] = feature_importance_sum
                            idx_start += one_hot_count
                
                # Si nous avons pu reconstruire les importances par feature originale,
                # utiliser ces résultats plutôt que permutation_importance
                if original_feature_importance:
                    logger.info("Utilisation des importances natives du modèle (agrégées par feature originale)")
                    importance_dict = original_feature_importance
            
            # Trier par importance décroissante
            importance_dict = {k: v for k, v in sorted(importance_dict.items(),
                                                      key=lambda item: item[1],
                                                      reverse=True)}
            
            return importance_dict
                
        except Exception as e:
            logger.error(f"Erreur lors du calcul de l'importance des features: {e}")
            traceback.print_exc()
            return {}
    
    def train_and_evaluate_models(self):
        """
        Entraîne et évalue tous les modèles, puis sélectionne le meilleur.
        
        Returns:
            bool: True si le processus a réussi, False sinon
        """
        logger.info("Entraînement et évaluation des modèles...")
        
        try:
            # Vérifier que les données sont prêtes
            if self.X_train is None or self.y_train is None:
                logger.error("Les données ne sont pas prêtes. Appelez prepare_data() d'abord.")
                return False
            
            # Entraîner les modèles
            lr_model, lr_time = self.train_linear_regression()
            rf_model, rf_time = self.train_random_forest()
            gb_model, gb_time = self.train_gradient_boosting()
            
            # Stocker les modèles
            self.models = {
                'Linear Regression': lr_model,
                'Random Forest': rf_model,
                'Gradient Boosting': gb_model
            }
            
            # Évaluer les modèles
            self.model_metrics = {}
            for name, model in self.models.items():
                metrics = self.evaluate_model(model, name)
                self.model_metrics[name] = metrics
            
            # Sélectionner le meilleur modèle basé sur R²
            best_r2 = -float('inf')
            self.best_model_name = None
            
            for name, metrics in self.model_metrics.items():
                if metrics['r2'] > best_r2:
                    best_r2 = metrics['r2']
                    self.best_model_name = name
            
            if self.best_model_name:
                self.best_model = self.models[self.best_model_name]
                logger.info(f"Meilleur modèle: {self.best_model_name} avec R² = {best_r2:.4f}")
                
                # Calculer l'importance des features pour le meilleur modèle
                self.feature_importance = self.get_feature_importance(
                    self.best_model, self.X_train.columns
                )
                
                return True
            else:
                logger.error("Aucun modèle n'a pu être sélectionné comme le meilleur")
                return False
                
        except Exception as e:
            logger.error(f"Erreur lors de l'entraînement et de l'évaluation des modèles: {e}")
            traceback.print_exc()
            return False
    
    def visualize_results(self):
        """
        Génère des visualisations pour analyser les résultats des modèles.
        Sauvegarde les figures dans le répertoire de sortie.
        
        Returns:
            bool: True si les visualisations ont été générées avec succès, False sinon
        """
        logger.info("Génération des visualisations...")
        
        try:
            if not self.best_model or not self.model_metrics:
                logger.error("Aucun modèle n'est disponible pour la visualisation")
                return False
            
            # Configuration des figures
            plt.style.use('seaborn-v0_8-darkgrid')
            
            # 1. Comparaison des métriques entre les modèles
            fig1, ax1 = plt.subplots(figsize=(10, 6))
            
            models = list(self.model_metrics.keys())
            r2_scores = [metrics['r2'] for metrics in self.model_metrics.values()]
            
            bars = ax1.bar(models, r2_scores, color=['#3498db', '#2ecc71', '#e74c3c'])
            ax1.set_ylabel('Score R²')
            ax1.set_title('Comparaison des performances des modèles (R²)')
            ax1.set_ylim(0, 1)
            
            # Ajouter les valeurs sur les barres
            for bar in bars:
                height = bar.get_height()
                ax1.annotate(f'{height:.3f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom')
            
            plt.tight_layout()
            fig1.savefig(os.path.join(self.output_dir, 'model_comparison.png'))
            
            # 2. Prédictions vs. Valeurs réelles pour le meilleur modèle
            y_pred = self.best_model.predict(self.X_test)
            
            fig2, ax2 = plt.subplots(figsize=(10, 6))
            ax2.scatter(self.y_test, y_pred, alpha=0.5)
            ax2.plot([self.y_test.min(), self.y_test.max()], 
                     [self.y_test.min(), self.y_test.max()], 
                     'k--', lw=2)
            ax2.set_xlabel('Prix réel')
            ax2.set_ylabel('Prix prédit')
            ax2.set_title(f'Prédictions vs. Valeurs réelles ({self.best_model_name})')
            
            plt.tight_layout()
            fig2.savefig(os.path.join(self.output_dir, 'predictions_vs_actual.png'))
            
            # 3. Importance des features pour le meilleur modèle
            if self.feature_importance:
                # Limiter aux 10 features les plus importantes
                top_n = 10
                top_features = list(self.feature_importance.keys())[:top_n]
                top_importance = list(self.feature_importance.values())[:top_n]
                
                fig3, ax3 = plt.subplots(figsize=(10, 6))
                bars = ax3.barh(top_features, top_importance, color='#3498db')
                ax3.set_xlabel('Importance relative')
                ax3.set_title(f'Top {top_n} features les plus importantes ({self.best_model_name})')
                ax3.invert_yaxis()  # Pour que la feature la plus importante soit en haut
                
                plt.tight_layout()
                fig3.savefig(os.path.join(self.output_dir, 'feature_importance.png'))
                
                # 4. Distribution des erreurs
                errors = self.y_test - y_pred
                
                fig4, ax4 = plt.subplots(figsize=(10, 6))
                ax4.hist(errors, bins=30, alpha=0.7, color='#3498db')
                ax4.axvline(0, color='r', linestyle='--', linewidth=1)
                ax4.set_xlabel('Erreur de prédiction')
                ax4.set_ylabel('Fréquence')
                ax4.set_title(f'Distribution des erreurs de prédiction ({self.best_model_name})')
                
                plt.tight_layout()
                fig4.savefig(os.path.join(self.output_dir, 'error_distribution.png'))
            
            logger.info(f"Visualisations générées et sauvegardées dans {self.output_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération des visualisations: {e}")
            traceback.print_exc()
            return False
    
    def save_model(self):
        """
        Sauvegarde le meilleur modèle au format pickle ou joblib.
        
        Returns:
            str: Chemin vers le fichier modèle sauvegardé ou None en cas d'erreur
        """
        logger.info(f"Sauvegarde du meilleur modèle ({self.best_model_name})...")
        
        try:
            if not self.best_model:
                logger.error("Aucun modèle à sauvegarder")
                return None
            
            # Créer un nom de fichier basé sur le type de modèle et la date
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            model_filename = f"{self.best_model_name.replace(' ', '_').lower()}_{timestamp}.pkl"
            model_path = os.path.join(self.output_dir, model_filename)
            
            # Sauvegarder le modèle avec pickle
            with open(model_path, 'wb') as f:
                pickle.dump(self.best_model, f)
            
            logger.info(f"Modèle sauvegardé avec succès: {model_path}")
            return model_path
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du modèle: {e}")
            traceback.print_exc()
            return None
    
    def save_model_metadata(self, model_path):
        """
        Enregistre les métadonnées du modèle dans la table dm_prediction_models.
        
        Args:
            model_path (str): Chemin vers le fichier modèle sauvegardé
            
        Returns:
            bool: True si les métadonnées ont été enregistrées avec succès, False sinon
        """
        logger.info("Enregistrement des métadonnées du modèle dans MySQL...")
        
        if not model_path or not self.best_model_name or not self.model_metrics:
            logger.error("Impossible d'enregistrer les métadonnées: informations manquantes")
            return False
        
        # Préparer les métadonnées
        model_metrics = self.model_metrics.get(self.best_model_name, {})
        
        # Paramètres spécifiques au modèle
        model_params = {}
        regressor = self.best_model.named_steps['regressor']
        
        if self.best_model_name == 'Linear Regression':
            model_params = {'fit_intercept': regressor.fit_intercept}
        elif self.best_model_name == 'Random Forest':
            model_params = {
                'n_estimators': regressor.n_estimators,
                'max_depth': regressor.max_depth,
                'min_samples_split': regressor.min_samples_split
            }
        elif self.best_model_name == 'Gradient Boosting':
            model_params = {
                'n_estimators': regressor.n_estimators,
                'learning_rate': regressor.learning_rate,
                'max_depth': regressor.max_depth
            }
        
        # Se connecter à MySQL et enregistrer les métadonnées
        conn, cursor = self.connect_to_mysql()
        if not conn or not cursor:
            return False
        
        try:
            # Préparer la requête SQL
            query = """
            REPLACE INTO dm_prediction_models 
            (model_id, model_name, model_version, training_date, model_params, 
             model_metrics, model_path, is_active, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            training_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            params = (
                self.model_id,
                self.best_model_name,
                self.model_version,
                training_date,
                json.dumps(model_params),
                json.dumps(model_metrics),
                model_path,
                True,  # Ce modèle est actif
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            
            # Exécuter la requête
            cursor.execute(query, params)
            conn.commit()
            
            logger.info(f"Métadonnées du modèle enregistrées avec succès dans dm_prediction_models")
            return True
            
        except Error as e:
            logger.error(f"Erreur lors de l'enregistrement des métadonnées du modèle: {e}")
            traceback.print_exc()
            return False
        finally:
            self.disconnect_from_mysql(conn, cursor)
    
    def generate_predictions(self):
        """
        Génère des prédictions pour l'ensemble de test et les enregistre dans MySQL.
        
        Returns:
            bool: True si les prédictions ont été générées et enregistrées avec succès, False sinon
        """
        logger.info("Génération des prédictions pour l'ensemble de test...")
        
        if not self.best_model or self.X_test is None:
            logger.error("Impossible de générer des prédictions: modèle ou données manquants")
            return False
        
        try:
            # Générer les prédictions
            y_pred = self.best_model.predict(self.X_test)
            
            # Calculer les intervalles de confiance
            # Méthode simplifiée: utiliser l'écart-type des erreurs comme marge
            errors = self.y_test - y_pred
            error_std = np.std(errors)
            confidence = 0.8  # 80% de confiance
            
            # Se connecter à MySQL
            conn, cursor = self.connect_to_mysql()
            if not conn or not cursor:
                return False
            
            try:
                # Récupérer les IDs de propriétés si disponibles
                property_ids = []
                
                if 'property_id' in self.df.columns:
                    property_ids = self.X_test.index.map(lambda i: self.df.iloc[i]['property_id'] 
                                                     if i < len(self.df) else f"test_{i}")
                else:
                    # Utiliser des IDs séquentiels si property_id n'est pas disponible
                    property_ids = [f"property_{i+1000}" for i in range(len(self.X_test))]
                
                # Traiter les prédictions par lots
                batch_size = 100
                total_predictions = 0
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                for i in range(0, len(y_pred), batch_size):
                    batch_end = min(i + batch_size, len(y_pred))
                    batch_y_pred = y_pred[i:batch_end]
                    batch_property_ids = property_ids[i:batch_end]
                    
                    # Préparer les requêtes d'insertion
                    query = """
                    REPLACE INTO dm_prediction_results
                    (prediction_id, property_id, predicted_price, prediction_interval_low, 
                     prediction_interval_high, confidence_score, model_id, prediction_timestamp,
                     feature_contributions, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    batch_values = []
                    for j, (prop_id, pred) in enumerate(zip(batch_property_ids, batch_y_pred)):
                        prediction_id = f"pred_{datetime.now().strftime('%Y%m%d')}_{total_predictions + j + 1}"
                        
                        # Calculer les intervalles de prédiction
                        pred_interval_low = max(0, pred - error_std * 1.96)
                        pred_interval_high = pred + error_std * 1.96
                        
                        # Extraire les contributions des features si disponible
                        feature_contrib = {}
                        if self.feature_importance:
                            feature_contrib = {k: float(v) for k, v in self.feature_importance.items()}
                        
                        values = (
                            prediction_id,
                            prop_id,
                            float(pred),
                            float(pred_interval_low),
                            float(pred_interval_high),
                            confidence,
                            self.model_id,
                            timestamp,
                            json.dumps(feature_contrib),
                            timestamp
                        )
                        
                        batch_values.append(values)
                    
                    # Exécuter les insertions par lots
                    cursor.executemany(query, batch_values)
                    conn.commit()
                    
                    total_predictions += len(batch_values)
                    logger.info(f"Lot de {len(batch_values)} prédictions inséré")
                
                logger.info(f"{total_predictions} prédictions enregistrées dans dm_prediction_results")
                return True
                
            except Error as e:
                logger.error(f"Erreur lors de l'enregistrement des prédictions: {e}")
                traceback.print_exc()
                return False
            finally:
                self.disconnect_from_mysql(conn, cursor)
                
        except Exception as e:
            logger.error(f"Erreur lors de la génération des prédictions: {e}")
            traceback.print_exc()
            return False
    
    def run_prediction_pipeline(self):
        """
        Exécute l'ensemble du pipeline de prédiction de prix immobiliers.
        
        Returns:
            dict: Résultats du pipeline
        """
        results = {
            'success': False,
            'model_name': None,
            'model_metrics': None,
            'model_path': None
        }
        
        try:
            logger.info("=== Début du pipeline de prédiction des prix immobiliers ===")
            
            # 1. Charger les données
            if not self.load_data():
                logger.error("Échec du chargement des données")
                return results
            
            # 2. Préparer les données
            if not self.prepare_data():
                logger.error("Échec de la préparation des données")
                return results
            
            # 3. Entraîner et évaluer les modèles
            if not self.train_and_evaluate_models():
                logger.error("Échec de l'entraînement des modèles")
                return results
            
            # 4. Visualiser les résultats
            self.visualize_results()
            
            # 5. Sauvegarder le meilleur modèle
            model_path = self.save_model()
            if not model_path:
                logger.error("Échec de la sauvegarde du modèle")
                return results
            
            # 6. Enregistrer les métadonnées du modèle
            if not self.save_model_metadata(model_path):
                logger.warning("Échec de l'enregistrement des métadonnées")
                # On continue malgré l'erreur
            
            # 7. Générer et enregistrer les prédictions
            if not self.generate_predictions():
                logger.warning("Échec de la génération des prédictions")
                # On continue malgré l'erreur
            
            # Tout a réussi
            results['success'] = True
            results['model_name'] = self.best_model_name
            results['model_metrics'] = self.model_metrics.get(self.best_model_name, {})
            results['model_path'] = model_path
            
            logger.info("=== Pipeline de prédiction terminé avec succès ===")
            return results
            
        except Exception as e:
            logger.error(f"Erreur dans le pipeline de prédiction: {e}")
            traceback.print_exc()
            return results


def main():
    """
    Fonction principale pour exécuter le pipeline de prédiction.
    """
    try:
        logger.info("Démarrage du processus de prédiction des prix immobiliers")
        
        # Initialiser le prédicteur
        predictor = HousingPricePredictor()
        
        # Exécuter le pipeline
        results = predictor.run_prediction_pipeline()
        
        if results['success']:
            model_name = results['model_name']
            metrics = results['model_metrics']
            
            logger.info(f"Modèle {model_name} entraîné avec succès")
            logger.info(f"  - RMSE: {metrics.get('rmse', 'N/A'):.2f}")
            logger.info(f"  - R²: {metrics.get('r2', 'N/A'):.4f}")
            logger.info(f"Modèle sauvegardé: {results['model_path']}")
        else:
            logger.error("Échec du processus de prédiction")
        
        return results['success']
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du processus de prédiction: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()
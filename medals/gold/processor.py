#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de traitement de la couche GOLD - Phase 2.4
--------------------------------------------------

Ce script implémente le processeur de la couche GOLD pour le pipeline de données immobilières.
Il est responsable de:
1. Lire les données dimensionnelles depuis la couche SILVER
2. Créer des agrégations et des features pour l'analyse et le machine learning
3. Stocker les données au format Parquet dans la couche GOLD

Fonctionnalités:
- Utilisation de PySpark pour le traitement des données
- Création des tables analytiques suivantes:
  * price_trends: Tendances de prix par région et période
  * feature_importance: Importance des caractéristiques sur le prix
  * price_prediction_features: Features pour les modèles de prédiction
- Création des métriques précalculées suivantes:
  * Moyennes mobiles des prix
  * Variations saisonnières
  * Indicateurs de marché
- Partitionnement selon l'architecture système (année/mois/jour)
"""

import os
import sys
import logging
from datetime import datetime
import traceback

# Imports PySpark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    lit, current_timestamp, year, month, day,
    col, when, isnull, regexp_replace, trim,
    count, mean, stddev, min, max, lower,
    datediff, concat, coalesce, round, expr,
    lag, lead, avg, sum, rank, dense_rank,
    percent_rank, ntile, corr, variance,
    array, struct, to_json, from_json, explode
)
from pyspark.sql.types import *

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importation conditionnelle des modules ML de PySpark
# Ces modules sont optionnels et utilisés uniquement pour certaines fonctionnalités avancées
try:
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.stat import Correlation
    ml_imports_available = True
except ImportError as e:
    logger.warning(f"Modules ML de PySpark non disponibles: {e}")
    logger.warning("Certaines fonctionnalités de feature engineering avancées seront désactivées")
    ml_imports_available = False


class GoldLayerProcessor:
    """
    Classe principale pour le traitement de la couche GOLD.
    
    Cette classe est responsable de:
    - Lire les données dimensionnelles depuis la couche SILVER
    - Créer des agrégations et des features pour l'analyse et le machine learning
    - Stocker les données au format Parquet dans la couche GOLD avec partitionnement
    """
    
    def __init__(self, silver_path='data/silver',
                 gold_output_path='data/gold'):
        """
        Initialise le processeur de la couche GOLD.
        
        Args:
            silver_path (str): Chemin source pour les données de la couche SILVER
            gold_output_path (str): Chemin de destination pour les données traitées
        """
        self.silver_path = silver_path
        self.gold_output_path = gold_output_path
        
        # Date actuelle pour le partitionnement
        self.current_date = datetime.now()
        self.year = self.current_date.year
        self.month = self.current_date.month
        self.day = self.current_date.day
        
        # Initialisé dans la méthode initialize_spark()
        self.spark = None
        
        # Tables SILVER qui seront lues
        self.silver_tables = {
            "property_details": None,
            "location_details": None,
            "building_features": None,
            "sale_history": None
        }
        
        # Tables GOLD qui seront créées
        self.gold_tables = {
            "price_trends": None,
            "feature_importance": None,
            "price_prediction_features": None
        }
        
        logger.info("Processeur de la couche GOLD initialisé")
        
    def initialize_spark(self):
        """
        Initialise une session Spark avec les configurations appropriées.
        
        Returns:
            SparkSession: Session Spark configurée
        """
        logger.info("Initialisation de la session Spark...")
        
        try:
            # Configuration de Spark avec support Parquet et optimisations
            self.spark = (SparkSession.builder
                .appName("GoldLayerProcessor")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.shuffle.partitions", "3")  # 3 partitions comme dans la couche SILVER
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
    
    def ensure_output_directories(self):
        """
        S'assure que les répertoires de destination existent.
        Crée les répertoires si nécessaire.
        """
        logger.info("Vérification des répertoires de destination...")
        
        try:
            # Création du répertoire de base s'il n'existe pas
            if not os.path.exists(self.gold_output_path):
                os.makedirs(self.gold_output_path, exist_ok=True)
                logger.info(f"Répertoire créé: {self.gold_output_path}")
            
            # Création des sous-répertoires pour chaque table analytique
            for table_name in self.gold_tables.keys():
                table_path = f"{self.gold_output_path}/{table_name}"
                if not os.path.exists(table_path):
                    os.makedirs(table_path, exist_ok=True)
                    logger.info(f"Répertoire créé: {table_path}")
            
            logger.info("Répertoires de destination vérifiés")
        except Exception as e:
            logger.error(f"Erreur lors de la vérification/création des répertoires: {e}")
            raise
    
    def discover_latest_silver_data(self, table_name):
        """
        Découvre les dernières données disponibles dans la couche SILVER pour une table spécifique.
        
        Args:
            table_name (str): Nom de la table dimensionnelle
            
        Returns:
            str: Chemin vers les dernières données disponibles
        """
        logger.info(f"Recherche des dernières données pour la table '{table_name}' dans la couche SILVER...")
        
        try:
            # Vérifier si le chemin existe
            table_path = f"{self.silver_path}/{table_name}"
            if not os.path.exists(table_path):
                logger.warning(f"Le chemin {table_path} n'existe pas")
                return None
            
            # Construction du chemin avec partitionnement pour la date actuelle
            latest_path = f"{table_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Vérifier si le chemin existe
            if not os.path.exists(latest_path):
                logger.warning(f"Aucune donnée récente trouvée à {latest_path}")
                return None
                
            logger.info(f"Données récentes trouvées pour '{table_name}': {latest_path}")
            return latest_path
            
        except Exception as e:
            logger.error(f"Erreur lors de la découverte des données pour '{table_name}': {e}")
            return None
    
    def read_silver_data(self):
        """
        Lit les données dimensionnelles Parquet depuis la couche SILVER.
        Stocke les DataFrames dans le dictionnaire silver_tables.
        
        Returns:
            bool: True si toutes les données ont été lues avec succès, False sinon
        """
        try:
            all_tables_loaded = True
            
            # Lire chaque table dimensionnelle
            for table_name in self.silver_tables.keys():
                try:
                    # Découvrir les dernières données disponibles
                    latest_path = self.discover_latest_silver_data(table_name)
                    
                    if not latest_path:
                        logger.warning(f"Aucune donnée SILVER disponible pour '{table_name}'")
                        all_tables_loaded = False
                        continue
                    
                    # Lister les fichiers .parquet dans le répertoire
                    parquet_files = []
                    for root, dirs, files in os.walk(latest_path):
                        parquet_files.extend([os.path.join(root, f) for f in files if f.endswith('.parquet')])
                    
                    if not parquet_files:
                        logger.warning(f"Aucun fichier Parquet trouvé dans {latest_path}")
                        all_tables_loaded = False
                        continue
                    
                    logger.info(f"Lecture des données depuis {parquet_files}")
                    
                    # Lecture des fichiers Parquet
                    df = self.spark.read.parquet(*parquet_files)
                    
                    # Vérification des données lues
                    row_count = df.count()
                    column_count = len(df.columns)
                    logger.info(f"Données SILVER pour '{table_name}' lues avec succès: {row_count} lignes, {column_count} colonnes")
                    
                    # Stocker le DataFrame dans le dictionnaire
                    self.silver_tables[table_name] = df
                    
                except Exception as e:
                    logger.error(f"Erreur lors de la lecture des données SILVER pour '{table_name}': {e}")
                    all_tables_loaded = False
            
            return all_tables_loaded
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données SILVER: {e}")
            traceback.print_exc()
            return False
    
    def create_price_trends(self):
        """
        Crée la table analytique price_trends avec les tendances de prix par région et période.
        
        Returns:
            DataFrame: Table analytique price_trends
        """
        logger.info("Création de la table analytique price_trends...")
        
        try:
            # Vérifier si les tables nécessaires sont disponibles
            if (self.silver_tables["property_details"] is None or 
                self.silver_tables["location_details"] is None or
                self.silver_tables["sale_history"] is None):
                logger.error("Données manquantes pour créer price_trends")
                return None
            
            # Joindre les tables dimensionnelles pour obtenir les prix et les informations de localisation
            property_df = self.silver_tables["property_details"]
            location_df = self.silver_tables["location_details"]
            sale_df = self.silver_tables["sale_history"] if self.silver_tables["sale_history"] is not None else property_df
            
            # Joindre les tables sur property_id
            joined_df = (property_df
                .join(location_df, "property_id", "inner")
                .select(
                    property_df["property_id"],
                    property_df["price"],
                    property_df["transaction_date"],
                    property_df["listing_date"],
                    property_df["property_type"],
                    location_df["region"],
                    location_df["city"],
                    location_df["neighborhood"]
                ))
            
            # Extraire l'année et le mois de la date de transaction
            joined_df = (joined_df
                .withColumn("sale_year", year(col("transaction_date")))
                .withColumn("sale_month", month(col("transaction_date")))
                .withColumn("sale_quarter", expr("concat('Q', ceil(month(transaction_date)/3))"))
            )
            
            # Définir une fenêtre par région et période
            window_region_month = Window.partitionBy("region", "city", "sale_year", "sale_month").orderBy("transaction_date")
            window_region_quarter = Window.partitionBy("region", "city", "sale_year", "sale_quarter").orderBy("transaction_date")
            window_neighborhood_month = Window.partitionBy("neighborhood", "sale_year", "sale_month").orderBy("transaction_date")
            window_region_year = Window.partitionBy("region", "city", "sale_year").orderBy("transaction_date")
            
            # Calcul des métriques par région et mois
            monthly_trends = (joined_df
                .groupBy("region", "city", "sale_year", "sale_month")
                .agg(
                    count("property_id").alias("properties_sold"),
                    mean("price").alias("avg_price"),
                    stddev("price").alias("price_stddev"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    expr("percentile(price, 0.5)").alias("median_price")
                )
                # Calcul de la variation par rapport au mois précédent
                .withColumn("period_type", lit("monthly"))
                .withColumn("period_value", col("sale_month"))
                .orderBy("region", "city", "sale_year", "sale_month")
            )
            
            # Calcul des métriques par région et trimestre
            quarterly_trends = (joined_df
                .groupBy("region", "city", "sale_year", "sale_quarter")
                .agg(
                    count("property_id").alias("properties_sold"),
                    mean("price").alias("avg_price"),
                    stddev("price").alias("price_stddev"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    expr("percentile(price, 0.5)").alias("median_price")
                )
                .withColumn("period_type", lit("quarterly"))
                .withColumn("period_value", col("sale_quarter"))
                .orderBy("region", "city", "sale_year", "sale_quarter")
            )
            
            # Calcul des métriques par quartier et mois
            neighborhood_trends = (joined_df
                .groupBy("neighborhood", "sale_year", "sale_month")
                .agg(
                    count("property_id").alias("properties_sold"),
                    mean("price").alias("avg_price"),
                    stddev("price").alias("price_stddev"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    expr("percentile(price, 0.5)").alias("median_price")
                )
                .withColumn("period_type", lit("neighborhood_monthly"))
                .withColumn("period_value", col("sale_month"))
                .withColumn("region", lit(None).cast(StringType()))
                .withColumn("city", lit(None).cast(StringType()))
                .orderBy("neighborhood", "sale_year", "sale_month")
            )
            
            # Calcul des métriques par type de propriété, région et trimestre
            type_trends = (joined_df
                .groupBy("property_type", "region", "city", "sale_year", "sale_quarter")
                .agg(
                    count("property_id").alias("properties_sold"),
                    mean("price").alias("avg_price"),
                    stddev("price").alias("price_stddev"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    expr("percentile(price, 0.5)").alias("median_price")
                )
                .withColumn("period_type", lit("property_type_quarterly"))
                .withColumn("period_value", col("sale_quarter"))
                .withColumn("neighborhood", lit(None).cast(StringType()))
                .orderBy("property_type", "region", "city", "sale_year", "sale_quarter")
            )
            
            # Calcul des moyennes mobiles sur 3 mois pour chaque région
            window_moving_avg = Window.partitionBy("region", "city").orderBy("sale_year", "sale_month").rowsBetween(-2, 0)
            
            moving_avg_trends = (monthly_trends
                .withColumn("moving_avg_3m", avg("avg_price").over(window_moving_avg))
                .withColumn("period_type", lit("moving_avg_3m"))
                .withColumn("neighborhood", lit(None).cast(StringType()))
            )
            
            # Union de toutes les tendances
            price_trends = (monthly_trends
                .unionByName(quarterly_trends, allowMissingColumns=True)
                .unionByName(neighborhood_trends, allowMissingColumns=True)
                .unionByName(type_trends, allowMissingColumns=True)
                .unionByName(moving_avg_trends, allowMissingColumns=True)
            )
            
            # Ajouter les métadonnées et la date de génération
            price_trends = (price_trends
                .withColumn("generated_at", current_timestamp())
                .withColumn("source_table", lit("property_details,location_details"))
                .withColumn("year", lit(self.year))
                .withColumn("month", lit(self.month))
                .withColumn("day", lit(self.day))
            )
            
            # Calculer le nombre d'enregistrements
            count_records = price_trends.count()
            logger.info(f"Table analytique price_trends créée avec succès ({count_records} lignes)")
            
            # Stocker dans le dictionnaire de tables GOLD
            self.gold_tables["price_trends"] = price_trends
            
            return price_trends
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de price_trends: {e}")
            traceback.print_exc()
            return None
    
    def create_feature_importance(self):
        """
        Crée la table analytique feature_importance avec l'importance des caractéristiques sur le prix.
        Utilise des méthodes statistiques pour déterminer l'impact des différentes caractéristiques.
        
        Returns:
            DataFrame: Table analytique feature_importance
        """
        logger.info("Création de la table analytique feature_importance...")
        
        try:
            # Vérifier si les tables nécessaires sont disponibles
            if (self.silver_tables["property_details"] is None or 
                self.silver_tables["building_features"] is None):
                logger.error("Données manquantes pour créer feature_importance")
                return None
            
            property_df = self.silver_tables["property_details"]
            building_df = self.silver_tables["building_features"]
            location_df = self.silver_tables["location_details"]
            
            # Joindre les tables sur property_id
            joined_df = (property_df
                .join(building_df, "property_id", "inner")
                .join(location_df, "property_id", "inner")
                .select(
                    property_df["property_id"],
                    property_df["price"].alias("target_price"),
                    property_df["property_type"],
                    property_df["condition"],
                    property_df["energy_rating"],
                    property_df["age_at_sale"],
                    building_df["total_rooms"],
                    building_df["bathrooms"],
                    building_df["bedrooms"],
                    building_df["total_rooms"].alias("total_sqft"),  # Utiliser total_rooms comme approximation de surface
                    building_df["floor_number"],
                    building_df["garage"].alias("garage_type"),
                    building_df["roof_type"],
                    building_df["pool"].cast("int").alias("has_pool_int"),
                    building_df["garden"].cast("int").alias("has_garden_int"),
                    location_df["neighborhood"],
                    location_df["city"],
                    location_df["zoning_code"]
                )
                .na.fill(0)  # Remplacer les valeurs nulles par 0
            )
            
            # Calculer les corrélations entre les caractéristiques numériques et le prix
            numeric_features = ["total_rooms", "bathrooms", "bedrooms",
                               "floor_number", "age_at_sale", "has_pool_int", "has_garden_int"]
            
            correlations = []
            for feature in numeric_features:
                if feature in joined_df.columns:
                    # Calculer la corrélation entre chaque feature et le prix
                    correlation = joined_df.stat.corr(feature, "target_price")
                    correlations.append((feature, correlation))
            
            # Créer un DataFrame pour les corrélations
            correlation_df = self.spark.createDataFrame(correlations, ["feature_name", "correlation_with_price"])
            
            # Classer les caractéristiques par corrélation absolue
            correlation_df = (correlation_df
                .withColumn("abs_correlation", expr("abs(correlation_with_price)"))
                .withColumn("feature_type", lit("numeric"))
                .orderBy(col("abs_correlation").desc())
            )
            
            # Analyse des caractéristiques catégorielles
            categorical_features = ["property_type", "condition", "energy_rating",
                                   "neighborhood", "zoning_code", "roof_type"]
            
            categorical_impact = []
            for feature in categorical_features:
                if feature in joined_df.columns:
                    # Pour chaque catégorie, calculer le prix moyen et le nombre de propriétés
                    category_stats = (joined_df
                        .groupBy(feature)
                        .agg(
                            avg("target_price").alias("avg_price"),
                            count("property_id").alias("property_count")
                        )
                        .orderBy(col("avg_price").desc())
                    )
                    
                    # Calculer la variance des prix moyens entre les catégories
                    # Plus la variance est grande, plus la feature a d'impact
                    category_avg = category_stats.select(mean("avg_price")).collect()[0][0]
                    category_variances = (category_stats
                        .withColumn("squared_diff", pow(col("avg_price") - lit(category_avg), 2))
                        .agg(sum("squared_diff").alias("sum_squared_diff"))
                        .collect()[0][0]
                    )
                    
                    # Normaliser par le nombre de catégories
                    category_count = category_stats.count()
                    if category_count > 1:  # Éviter division par zéro
                        variance = category_variances / (category_count - 1)
                    else:
                        variance = 0
                    
                    categorical_impact.append((feature, variance, category_count))
            
            # Créer un DataFrame pour l'impact des caractéristiques catégorielles
            categorical_df = self.spark.createDataFrame(
                categorical_impact, 
                ["feature_name", "price_variance", "category_count"]
            )
            
            # Normaliser les variances pour obtenir un score d'impact
            categorical_max_variance = categorical_df.agg({"price_variance": "max"}).collect()[0][0]
            
            categorical_df = (categorical_df
                .withColumn(
                    "correlation_with_price", 
                    when(categorical_max_variance > 0, col("price_variance") / lit(categorical_max_variance))
                    .otherwise(lit(0))
                )
                .withColumn("abs_correlation", col("correlation_with_price"))
                .withColumn("feature_type", lit("categorical"))
                .select("feature_name", "correlation_with_price", "abs_correlation", "feature_type", "category_count")
                .orderBy(col("correlation_with_price").desc())
            )
            
            # Combiner les caractéristiques numériques et catégorielles
            feature_importance = (correlation_df
                .unionByName(categorical_df, allowMissingColumns=True)
                .withColumn("generated_at", current_timestamp())
                .withColumn("source_table", lit("property_details,building_features,location_details"))
                .withColumn("year", lit(self.year))
                .withColumn("month", lit(self.month))
                .withColumn("day", lit(self.day))
            )
            
            # Ajouter un rang d'importance global
            feature_importance = (feature_importance
                .withColumn("importance_rank", dense_rank().over(Window.orderBy(col("abs_correlation").desc())))
            )
            
            # Calculer le nombre d'enregistrements
            count_records = feature_importance.count()
            logger.info(f"Table analytique feature_importance créée avec succès ({count_records} lignes)")
            
            # Stocker dans le dictionnaire de tables GOLD
            self.gold_tables["feature_importance"] = feature_importance
            
            return feature_importance
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de feature_importance: {e}")
            traceback.print_exc()
            return None
    
    def create_price_prediction_features(self):
        """
        Crée la table analytique price_prediction_features avec les features pour les modèles de prédiction de prix.
        Cette table sera optimisée pour alimenter directement des modèles de machine learning.
        
        Returns:
            DataFrame: Table analytique price_prediction_features
        """
        logger.info("Création de la table analytique price_prediction_features...")
        
        try:
            # Vérifier si les tables nécessaires sont disponibles
            if (self.silver_tables["property_details"] is None or 
                self.silver_tables["building_features"] is None or
                self.silver_tables["location_details"] is None):
                logger.error("Données manquantes pour créer price_prediction_features")
                return None
            
            property_df = self.silver_tables["property_details"]
            building_df = self.silver_tables["building_features"]
            location_df = self.silver_tables["location_details"]
            
            # Joindre les tables sur property_id
            joined_df = (property_df
                .join(building_df, "property_id", "inner")
                .join(location_df, "property_id", "inner")
            )
            
            # Sélectionner et préparer les features pour la prédiction de prix
            price_features = (joined_df
                .select(
                    property_df["property_id"],
                    property_df["price"].alias("target_price"),
                    property_df["property_type"],
                    property_df["condition"],
                    property_df["energy_rating"],
                    property_df["age_at_sale"],
                    property_df["transaction_date"],
                    building_df["total_rooms"],
                    building_df["bathrooms"],
                    building_df["bedrooms"],
                    building_df["total_rooms"].alias("total_sqft"),  # Utiliser total_rooms comme approximation de surface
                    building_df["floor_number"].alias("floors"),
                    building_df["garage"].alias("garage_type"),
                    building_df["roof_type"].alias("roof_style"),
                    building_df["pool"].alias("has_pool"),
                    building_df["garden"].alias("has_garden"),
                    building_df["heating_system"].alias("heating_type"),
                    building_df["cooling_system"].alias("cooling_type"),
                    location_df["neighborhood"],
                    location_df["city"],
                    location_df["zoning_code"],
                    location_df["longitude"],
                    location_df["latitude"]
                )
            )
            
            # Encoder les variables catégorielles (one-hot encoding)
            categorical_cols = ["property_type", "condition", "energy_rating",
                              "garage_type", "roof_style", "heating_type",
                              "cooling_type", "neighborhood", "zoning_code"]
            
            # Créer les colonnes encodées pour chaque colonne catégorielle
            for col_name in categorical_cols:
                if col_name in price_features.columns:
                    # Obtenir toutes les valeurs distinctes pour cette colonne
                    distinct_values = price_features.select(col_name).distinct().collect()
                    distinct_values = [row[col_name] for row in distinct_values if row[col_name] is not None]
                    
                    # Créer une colonne binaire pour chaque valeur distincte
                    for value in distinct_values:
                        # Vérifier le type de la valeur avant d'appliquer replace
                        if isinstance(value, str):
                            new_col_name = f"{col_name}_{value.replace(' ', '_').replace('-', '_').lower()}"
                        else:
                            # Utiliser la représentation en chaîne de caractères pour les non-strings
                            new_col_name = f"{col_name}_{str(value).lower()}"
                        
                        price_features = price_features.withColumn(
                            new_col_name,
                            when(col(col_name) == value, 1).otherwise(0)
                        )
            
            # Convertir les booléens en entiers
            boolean_cols = ["has_pool", "has_garden"]
            for col_name in boolean_cols:
                if col_name in price_features.columns:
                    price_features = price_features.withColumn(
                        col_name,
                        when(col(col_name) == True, 1).otherwise(0)
                    )
            
            # Extraire des caractéristiques temporelles de la date de transaction
            if "transaction_date" in price_features.columns:
                price_features = (price_features
                    .withColumn("transaction_year", year(col("transaction_date")))
                    .withColumn("transaction_month", month(col("transaction_date")))
                    .withColumn("transaction_quarter", expr("ceil(month(transaction_date)/3)"))
                )
            
            # Calculer des ratio et features d'interaction
            if "total_rooms" in price_features.columns and "bedrooms" in price_features.columns:
                price_features = price_features.withColumn(
                    "rooms_per_bedroom",
                    when(col("bedrooms") > 0, col("total_rooms") / col("bedrooms")).otherwise(col("total_rooms"))
                )
            
            if "total_rooms" in price_features.columns and "bathrooms" in price_features.columns:
                price_features = price_features.withColumn(
                    "rooms_per_bathroom",
                    when(col("bathrooms") > 0, col("total_rooms") / col("bathrooms")).otherwise(col("total_rooms"))
                )
            
            # Créer des features de proximité géographique (distance au centre-ville)
            # Pour simplifier, nous utilisons une approximation basée sur un point central fictif
            if "longitude" in price_features.columns and "latitude" in price_features.columns:
                # Coordonnées approximatives du centre d'Ames, Iowa
                center_longitude = -93.62
                center_latitude = 42.03
                
                price_features = (price_features
                    .withColumn(
                        "distance_to_downtown",
                        expr(f"sqrt(power(longitude - {center_longitude}, 2) + power(latitude - {center_latitude}, 2)) * 111")
                    )
                )
            
            # Ajouter le résultat des tables analytiques précédentes comme features supplémentaires
            # Si nous avons déjà calculé price_trends, utilisons certaines de ces informations
            if self.gold_tables["price_trends"] is not None:
                # Obtenir les tendances de prix par quartier
                neighborhood_trends = (self.gold_tables["price_trends"]
                    .filter(col("period_type") == "neighborhood_monthly")
                    .select("neighborhood", "sale_year", "sale_month", "avg_price", "median_price")
                    .withColumnRenamed("sale_year", "trend_year")
                    .withColumnRenamed("sale_month", "trend_month")
                    .withColumnRenamed("avg_price", "neighborhood_avg_price")
                    .withColumnRenamed("median_price", "neighborhood_median_price")
                )
                
                # Joindre ces informations aux features
                if "neighborhood" in price_features.columns and "transaction_year" in price_features.columns:
                    price_features = (price_features
                        .join(
                            neighborhood_trends,
                            (price_features["neighborhood"] == neighborhood_trends["neighborhood"]) &
                            (price_features["transaction_year"] == neighborhood_trends["trend_year"]) &
                            (price_features["transaction_month"] == neighborhood_trends["trend_month"]),
                            "left"
                        )
                        .drop(neighborhood_trends["neighborhood"])
                        .drop("trend_year", "trend_month")
                    )
                    
                    # Calculer le ratio prix/prix moyen du quartier
                    price_features = price_features.withColumn(
                        "price_to_neighborhood_ratio",
                        when(col("neighborhood_avg_price").isNotNull() & (col("neighborhood_avg_price") > 0),
                             col("target_price") / col("neighborhood_avg_price"))
                        .otherwise(lit(1.0))
                    )
            
            # Ajouter les métadonnées et la date de génération
            price_features = (price_features
                .withColumn("generated_at", current_timestamp())
                .withColumn("source_table", lit("property_details,building_features,location_details"))
                .withColumn("year", lit(self.year))
                .withColumn("month", lit(self.month))
                .withColumn("day", lit(self.day))
            )
            
            # Calculer le nombre d'enregistrements
            count_records = price_features.count()
            logger.info(f"Table analytique price_prediction_features créée avec succès ({count_records} lignes)")
            
            # Stocker dans le dictionnaire de tables GOLD
            self.gold_tables["price_prediction_features"] = price_features
            
            return price_features
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de price_prediction_features: {e}")
            traceback.print_exc()
            return None
    
    def calculate_moving_averages(self, price_trends_df):
        """
        Calcule les moyennes mobiles des prix à partir de la table price_trends.
        
        Args:
            price_trends_df (DataFrame): DataFrame contenant les tendances de prix
            
        Returns:
            DataFrame: DataFrame avec les moyennes mobiles calculées
        """
        logger.info("Calcul des moyennes mobiles des prix...")
        
        try:
            if price_trends_df is None:
                logger.error("Données manquantes pour calculer les moyennes mobiles")
                return None
            
            # Filtrer pour ne garder que les tendances mensuelles
            monthly_trends = price_trends_df.filter(col("period_type") == "monthly")
            
            # Définir les fenêtres pour les moyennes mobiles (3, 6 et 12 mois)
            window_3m = (Window
                .partitionBy("region", "city")
                .orderBy("sale_year", "sale_month")
                .rowsBetween(-2, 0)  # 3 mois (mois actuel + 2 précédents)
            )
            
            window_6m = (Window
                .partitionBy("region", "city")
                .orderBy("sale_year", "sale_month")
                .rowsBetween(-5, 0)  # 6 mois (mois actuel + 5 précédents)
            )
            
            window_12m = (Window
                .partitionBy("region", "city")
                .orderBy("sale_year", "sale_month")
                .rowsBetween(-11, 0)  # 12 mois (mois actuel + 11 précédents)
            )
            
            # Calculer les moyennes mobiles pour différentes périodes
            moving_averages = (monthly_trends
                .withColumn("avg_price_3m", round(avg("avg_price").over(window_3m), 2))
                .withColumn("avg_price_6m", round(avg("avg_price").over(window_6m), 2))
                .withColumn("avg_price_12m", round(avg("avg_price").over(window_12m), 2))
                .withColumn("median_price_3m", round(avg("median_price").over(window_3m), 2))
                .withColumn("median_price_6m", round(avg("median_price").over(window_6m), 2))
                .withColumn("median_price_12m", round(avg("median_price").over(window_12m), 2))
                .withColumn("moving_avg_type", lit("region_city"))
            )
            
            # Faire de même pour les quartiers
            window_neighborhood_3m = (Window
                .partitionBy("neighborhood")
                .orderBy("sale_year", "sale_month")
                .rowsBetween(-2, 0)
            )
            
            window_neighborhood_6m = (Window
                .partitionBy("neighborhood")
                .orderBy("sale_year", "sale_month")
                .rowsBetween(-5, 0)
            )
            
            neighborhood_trends = price_trends_df.filter(col("period_type") == "neighborhood_monthly")
            
            neighborhood_moving_averages = (neighborhood_trends
                .withColumn("avg_price_3m", round(avg("avg_price").over(window_neighborhood_3m), 2))
                .withColumn("avg_price_6m", round(avg("avg_price").over(window_neighborhood_6m), 2))
                .withColumn("moving_avg_type", lit("neighborhood"))
            )
            
            # Unir les deux ensembles de moyennes mobiles
            moving_averages = (moving_averages
                .unionByName(neighborhood_moving_averages, allowMissingColumns=True)
                .withColumn("generated_at", current_timestamp())
            )
            
            logger.info("Moyennes mobiles calculées avec succès")
            return moving_averages
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des moyennes mobiles: {e}")
            traceback.print_exc()
            return None
    
    def calculate_seasonal_variations(self, price_trends_df):
        """
        Calcule les variations saisonnières des prix à partir de la table price_trends.
        
        Args:
            price_trends_df (DataFrame): DataFrame contenant les tendances de prix
            
        Returns:
            DataFrame: DataFrame avec les variations saisonnières calculées
        """
        logger.info("Calcul des variations saisonnières...")
        
        try:
            if price_trends_df is None:
                logger.error("Données manquantes pour calculer les variations saisonnières")
                return None
            
            # Filtrer pour ne garder que les tendances mensuelles
            monthly_trends = price_trends_df.filter(col("period_type") == "monthly")
            
            # Calculer la moyenne des prix par mois calendaire pour chaque région
            seasonal_variations = (monthly_trends
                .groupBy("region", "city", "sale_month")
                .agg(
                    avg("avg_price").alias("avg_monthly_price"),
                    avg("median_price").alias("median_monthly_price"),
                    count("*").alias("data_points")
                )
            )
            
            # Calculer la moyenne générale pour chaque région
            region_avg = (seasonal_variations
                .groupBy("region", "city")
                .agg(
                    avg("avg_monthly_price").alias("region_avg_price"),
                    avg("median_monthly_price").alias("region_median_price")
                )
            )
            
            # Joindre les moyennes régionales pour calculer l'indice saisonnier
            seasonal_variations = (seasonal_variations
                .join(region_avg, ["region", "city"])
                .withColumn(
                    "seasonal_index",
                    when(col("region_avg_price") > 0, 
                         round(col("avg_monthly_price") / col("region_avg_price"), 4))
                    .otherwise(lit(1.0))
                )
                .withColumn(
                    "median_seasonal_index",
                    when(col("region_median_price") > 0,
                         round(col("median_monthly_price") / col("region_median_price"), 4))
                    .otherwise(lit(1.0))
                )
                .withColumn("month_name", 
                    when(col("sale_month") == 1, "Janvier")
                    .when(col("sale_month") == 2, "Février")
                    .when(col("sale_month") == 3, "Mars")
                    .when(col("sale_month") == 4, "Avril")
                    .when(col("sale_month") == 5, "Mai")
                    .when(col("sale_month") == 6, "Juin")
                    .when(col("sale_month") == 7, "Juillet")
                    .when(col("sale_month") == 8, "Août")
                    .when(col("sale_month") == 9, "Septembre")
                    .when(col("sale_month") == 10, "Octobre")
                    .when(col("sale_month") == 11, "Novembre")
                    .when(col("sale_month") == 12, "Décembre")
                )
                .withColumn("season", 
                    when((col("sale_month") >= 3) & (col("sale_month") <= 5), "Printemps")
                    .when((col("sale_month") >= 6) & (col("sale_month") <= 8), "Été")
                    .when((col("sale_month") >= 9) & (col("sale_month") <= 11), "Automne")
                    .otherwise("Hiver")
                )
                .withColumn("generated_at", current_timestamp())
                .withColumn("year", lit(self.year))
                .withColumn("month", lit(self.month))
                .withColumn("day", lit(self.day))
                .orderBy("region", "city", "sale_month")
            )
            
            # Faire de même pour les quartiers
            neighborhood_monthly = price_trends_df.filter(col("period_type") == "neighborhood_monthly")
            
            if neighborhood_monthly.count() > 0:
                neighborhood_variations = (neighborhood_monthly
                    .groupBy("neighborhood", "sale_month")
                    .agg(
                        avg("avg_price").alias("avg_monthly_price"),
                        avg("median_price").alias("median_monthly_price"),
                        count("*").alias("data_points")
                    )
                )
                
                # Calculer la moyenne générale pour chaque quartier
                neighborhood_avg = (neighborhood_variations
                    .groupBy("neighborhood")
                    .agg(
                        avg("avg_monthly_price").alias("neighborhood_avg_price"),
                        avg("median_monthly_price").alias("neighborhood_median_price")
                    )
                )
                
                # Joindre les moyennes de quartier pour calculer l'indice saisonnier
                neighborhood_variations = (neighborhood_variations
                    .join(neighborhood_avg, ["neighborhood"])
                    .withColumn(
                        "seasonal_index",
                        when(col("neighborhood_avg_price") > 0,
                             round(col("avg_monthly_price") / col("neighborhood_avg_price"), 4))
                        .otherwise(lit(1.0))
                    )
                    .withColumn(
                        "median_seasonal_index",
                        when(col("neighborhood_median_price") > 0,
                             round(col("median_monthly_price") / col("neighborhood_median_price"), 4))
                        .otherwise(lit(1.0))
                    )
                    .withColumn("month_name", 
                        when(col("sale_month") == 1, "Janvier")
                        .when(col("sale_month") == 2, "Février")
                        .when(col("sale_month") == 3, "Mars")
                        .when(col("sale_month") == 4, "Avril")
                        .when(col("sale_month") == 5, "Mai")
                        .when(col("sale_month") == 6, "Juin")
                        .when(col("sale_month") == 7, "Juillet")
                        .when(col("sale_month") == 8, "Août")
                        .when(col("sale_month") == 9, "Septembre")
                        .when(col("sale_month") == 10, "Octobre")
                        .when(col("sale_month") == 11, "Novembre")
                        .when(col("sale_month") == 12, "Décembre")
                    )
                    .withColumn("season", 
                        when((col("sale_month") >= 3) & (col("sale_month") <= 5), "Printemps")
                        .when((col("sale_month") >= 6) & (col("sale_month") <= 8), "Été")
                        .when((col("sale_month") >= 9) & (col("sale_month") <= 11), "Automne")
                        .otherwise("Hiver")
                    )
                    .withColumn("generated_at", current_timestamp())
                    .withColumn("region", lit(None).cast(StringType()))
                    .withColumn("city", lit(None).cast(StringType()))
                    .withColumn("year", lit(self.year))
                    .withColumn("month", lit(self.month))
                    .withColumn("day", lit(self.day))
                    .orderBy("neighborhood", "sale_month")
                )
                
                # Unir les variations saisonnières par région et par quartier
                seasonal_variations = seasonal_variations.unionByName(
                    neighborhood_variations, 
                    allowMissingColumns=True
                )
            
            logger.info("Variations saisonnières calculées avec succès")
            return seasonal_variations
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des variations saisonnières: {e}")
            traceback.print_exc()
            return None
    
    def calculate_market_indicators(self):
        """
        Calcule les indicateurs de marché à partir des données disponibles.
        
        Returns:
            DataFrame: DataFrame avec les indicateurs de marché calculés
        """
        logger.info("Calcul des indicateurs de marché...")
        
        try:
            # Vérifier si les tables nécessaires sont disponibles
            if self.silver_tables["property_details"] is None:
                logger.error("Données manquantes pour calculer les indicateurs de marché")
                return None
            
            property_df = self.silver_tables["property_details"]
            location_df = self.silver_tables["location_details"] if self.silver_tables["location_details"] is not None else None
            
            if location_df is not None:
                # Joindre les tables pour obtenir les informations de localisation
                joined_df = property_df.join(location_df, "property_id", "inner")
            else:
                joined_df = property_df
                
            # Extraire l'année et le mois de la date de transaction
            joined_df = (joined_df
                .withColumn("sale_year", year(col("transaction_date")))
                .withColumn("sale_month", month(col("transaction_date")))
                .withColumn("sale_quarter", expr("concat('Q', ceil(month(transaction_date)/3))"))
                .withColumn("days_on_market", 
                    when(col("listing_date").isNotNull() & col("transaction_date").isNotNull(),
                         datediff(col("transaction_date"), col("listing_date")))
                    .otherwise(lit(30))  # Valeur par défaut si les dates ne sont pas disponibles
                )
            )
            
            # Calculer le délai de vente moyen
            if "neighborhood" in joined_df.columns:
                group_cols = ["neighborhood", "sale_year", "sale_month"]
                location_type = "neighborhood"
            elif "city" in joined_df.columns:
                group_cols = ["city", "sale_year", "sale_month"]
                location_type = "city"
            else:
                group_cols = ["sale_year", "sale_month"]
                location_type = "global"
            
            # Calculer les indicateurs de marché par groupe
            market_indicators = (joined_df
                .groupBy(*group_cols)
                .agg(
                    # Volume de ventes
                    count("property_id").alias("sales_volume"),
                    
                    # Prix moyen et médian
                    avg("price").alias("avg_price"),
                    expr("percentile(price, 0.5)").alias("median_price"),
                    
                    # Délai de vente
                    avg("days_on_market").alias("avg_days_on_market"),
                    expr("percentile(days_on_market, 0.5)").alias("median_days_on_market"),
                    
                    # Rapport offre/demande (approximation basée sur le délai de vente)
                    (count("property_id") / avg("days_on_market")).alias("supply_demand_ratio")
                )
                .withColumn("location_type", lit(location_type))
            )
            
            # Ajouter des indicateurs supplémentaires
            
            # Définir une fenêtre pour les mois précédents
            window_prev_month = (Window
                .partitionBy([c for c in group_cols if c != "sale_month"])
                .orderBy("sale_year", "sale_month")
            )
            
            # Calculer les variations mois par mois
            market_indicators = (market_indicators
                .withColumn("prev_month_sales", lag("sales_volume", 1).over(window_prev_month))
                .withColumn("prev_month_price", lag("avg_price", 1).over(window_prev_month))
                .withColumn("prev_month_days", lag("avg_days_on_market", 1).over(window_prev_month))
                
                # Calculer les variations en pourcentage
                .withColumn("sales_volume_mom_pct", 
                    when(col("prev_month_sales").isNotNull() & (col("prev_month_sales") > 0),
                         round((col("sales_volume") - col("prev_month_sales")) / col("prev_month_sales") * 100, 2))
                    .otherwise(lit(None).cast(DoubleType()))
                )
                .withColumn("avg_price_mom_pct",
                    when(col("prev_month_price").isNotNull() & (col("prev_month_price") > 0),
                         round((col("avg_price") - col("prev_month_price")) / col("prev_month_price") * 100, 2))
                    .otherwise(lit(None).cast(DoubleType()))
                )
                .withColumn("days_on_market_mom_pct",
                    when(col("prev_month_days").isNotNull() & (col("prev_month_days") > 0),
                         round((col("avg_days_on_market") - col("prev_month_days")) / col("prev_month_days") * 100, 2))
                    .otherwise(lit(None).cast(DoubleType()))
                )
                
                # Calculer un indicateur composite du marché
                .withColumn("market_heat_index",
                    when(col("avg_days_on_market") > 0,
                         round(col("sales_volume") / col("avg_days_on_market") * 100, 2))
                    .otherwise(lit(0))
                )
                
                # Ajouter une interprétation du marché basée sur les tendances
                .withColumn("market_status",
                    when((col("sales_volume_mom_pct") > 5) & (col("avg_price_mom_pct") > 2), "Marché en forte hausse")
                    .when((col("sales_volume_mom_pct") > 0) & (col("avg_price_mom_pct") > 0), "Marché en hausse")
                    .when((col("sales_volume_mom_pct") < -5) & (col("avg_price_mom_pct") < -2), "Marché en forte baisse")
                    .when((col("sales_volume_mom_pct") < 0) & (col("avg_price_mom_pct") < 0), "Marché en baisse")
                    .when((col("days_on_market_mom_pct") > 10), "Marché ralenti")
                    .when((col("days_on_market_mom_pct") < -10), "Marché accéléré")
                    .otherwise("Marché stable")
                )
                
                # Ajouter les métadonnées et la date de génération
                .withColumn("generated_at", current_timestamp())
                .withColumn("year", lit(self.year))
                .withColumn("month", lit(self.month))
                .withColumn("day", lit(self.day))
                .orderBy(*group_cols)
            )
            
            logger.info("Indicateurs de marché calculés avec succès")
            return market_indicators
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des indicateurs de marché: {e}")
            traceback.print_exc()
            return None
    
    def write_analytic_table(self, df, table_name):
        """
        Écrit une table analytique au format Parquet dans la couche GOLD
        avec partitionnement par année/mois/jour.
        
        Args:
            df (DataFrame): DataFrame à écrire
            table_name (str): Nom de la table analytique
            
        Returns:
            bool: True si l'écriture a réussi, False sinon
        """
        if df is None:
            logger.warning(f"Aucune donnée à écrire pour la table {table_name}")
            return False
            
        logger.info(f"Écriture de la table analytique {table_name}...")
        
        try:
            # Construire le chemin de destination avec partitionnement
            output_path = f"{self.gold_output_path}/{table_name}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Vérifier si le répertoire existe déjà
            if os.path.exists(output_path):
                logger.info(f"Le répertoire {output_path} existe déjà, les données seront remplacées")
            
            # Écrire les données au format Parquet avec partitionnement
            # Note: Les colonnes de partitionnement (year, month, day) doivent être présentes dans le DataFrame
            (df.write
                .mode("overwrite")
                .parquet(output_path)
            )
            
            logger.info(f"Table analytique {table_name} écrite avec succès dans {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture de la table {table_name}: {e}")
            traceback.print_exc()
            return False
    
    def process_data(self):
        """
        Traite les données de la couche SILVER pour créer les tables analytiques de la couche GOLD.
        
        Returns:
            dict: Résultat du traitement avec statistiques
        """
        logger.info("Démarrage du traitement des données pour la couche GOLD...")
        
        try:
            # S'assurer que les répertoires de destination existent
            self.ensure_output_directories()
            
            # Lire les données dimensionnelles depuis la couche SILVER
            silver_data_loaded = self.read_silver_data()
            
            if not silver_data_loaded:
                logger.warning("Des données SILVER sont manquantes, certaines tables analytiques pourraient être incomplètes")
            
            # Créer les tables analytiques
            stats = {}
            
            # 1. Créer la table price_trends
            price_trends = self.create_price_trends()
            if price_trends is not None:
                stats["price_trends"] = price_trends.count()
                self.write_analytic_table(price_trends, "price_trends")
            
            # 2. Créer la table feature_importance
            feature_importance = self.create_feature_importance()
            if feature_importance is not None:
                stats["feature_importance"] = feature_importance.count()
                self.write_analytic_table(feature_importance, "feature_importance")
            
            # 3. Créer la table price_prediction_features
            price_prediction_features = self.create_price_prediction_features()
            if price_prediction_features is not None:
                stats["price_prediction_features"] = price_prediction_features.count()
                self.write_analytic_table(price_prediction_features, "price_prediction_features")
            
            # 4. Calculer les métriques précalculées
            
            # 4.1 Moyennes mobiles des prix
            if price_trends is not None:
                moving_averages = self.calculate_moving_averages(price_trends)
                if moving_averages is not None:
                    stats["moving_averages"] = moving_averages.count()
                    self.write_analytic_table(moving_averages, "moving_averages")
            
            # 4.2 Variations saisonnières
            if price_trends is not None:
                seasonal_variations = self.calculate_seasonal_variations(price_trends)
                if seasonal_variations is not None:
                    stats["seasonal_variations"] = seasonal_variations.count()
                    self.write_analytic_table(seasonal_variations, "seasonal_variations")
            
            # 4.3 Indicateurs de marché
            market_indicators = self.calculate_market_indicators()
            if market_indicators is not None:
                stats["market_indicators"] = market_indicators.count()
                self.write_analytic_table(market_indicators, "market_indicators")
            
            # Retourner les statistiques
            logger.info(f"Traitement GOLD terminé avec succès: {stats}")
            return {
                "status": "success",
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données GOLD: {e}")
            traceback.print_exc()
            return {
                "status": "error",
                "message": str(e)
            }
    
    def run(self):
        """
        Exécute le processeur de la couche GOLD.
        
        Returns:
            dict: Résultat global de l'exécution
        """
        start_time = datetime.now()
        logger.info(f"Démarrage du processeur de la couche GOLD à {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Initialiser Spark
            self.initialize_spark()
            
            # Traiter les données
            processing_result = self.process_data()
            
            # Calculer la durée d'exécution
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Construire le résultat
            result = {
                "overall_status": "success" if processing_result["status"] == "success" else "error",
                "processing_result": processing_result,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": duration.total_seconds()
            }
            
            logger.info(f"Processeur de la couche GOLD terminé en {duration}")
            return result
            
        except Exception as e:
            logger.error(f"Erreur fatale dans le processeur de la couche GOLD: {e}")
            traceback.print_exc()
            
            # Construire le résultat d'erreur
            end_time = datetime.now()
            duration = end_time - start_time
            
            return {
                "overall_status": "error",
                "error_message": str(e),
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": duration.total_seconds()
            }
        finally:
            # Arrêter la session Spark si elle existe
            if self.spark:
                logger.info("Arrêt de la session Spark")
                self.spark.stop()


def main():
    """
    Fonction principale pour exécuter le processeur de la couche GOLD.
    """
    try:
        # Création et exécution du processeur
        processor = GoldLayerProcessor()
        result = processor.run()
        
        # Vérifier le résultat
        if result["overall_status"] == "success":
            logger.info("Traitement de la couche GOLD réussi")
            exit_code = 0
        else:
            logger.error(f"Échec du traitement de la couche GOLD: {result}")
            exit_code = 1
            
        return exit_code
        
    except Exception as e:
        logger.critical(f"Erreur fatale: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    """
    Point d'entrée du script.
    """
    sys.exit(main())
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de traitement de la couche SILVER - Phase 2.3
----------------------------------------------------

Ce script implémente le processeur de la couche SILVER pour le pipeline de données immobilières.
Il est responsable de:
1. Lire les données Parquet depuis la couche BRONZE
2. Normaliser et enrichir ces données selon les règles métier
3. Diviser les données en tables dimensionnelles comme spécifié dans l'architecture
4. Stocker les données au format Parquet dans la couche SILVER

Fonctionnalités:
- Utilisation de PySpark pour le traitement des données
- Création des tables dimensionnelles:
  * property_details: Caractéristiques principales des propriétés
  * location_details: Informations de localisation
  * building_features: Caractéristiques du bâtiment
  * sale_history: Historique des ventes
- Ajout des champs calculés:
  * age_at_sale: Âge de la propriété au moment de la vente
  * total_rooms: Somme des pièces
  * price_per_sqft: Prix au pied carré
- Partitionnement selon l'architecture système (année/mois/jour)
"""

import os
import sys
import logging
from datetime import datetime
import traceback

# Imports PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit, current_timestamp, year, month, day,
    col, when, isnull, regexp_replace, trim,
    count, mean, stddev, min, max, lower,
    datediff, concat, coalesce, round, expr
)
from pyspark.sql.types import *
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType, BooleanType, TimestampType

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverLayerProcessor:
    """
    Classe principale pour le traitement de la couche SILVER.
    
    Cette classe est responsable de:
    - Lire les données Parquet depuis la couche BRONZE
    - Normaliser et enrichir ces données selon les règles métier
    - Diviser les données en tables dimensionnelles
    - Stocker les données dans la couche SILVER avec partitionnement
    """
    
    def __init__(self, bronze_path='data/bronze',
                 silver_output_path='data/silver'):
        """
        Initialise le processeur de la couche SILVER.
        
        Args:
            bronze_path (str): Chemin source pour les données de la couche BRONZE
            silver_output_path (str): Chemin de destination pour les données traitées
        """
        self.bronze_path = bronze_path
        self.silver_output_path = silver_output_path
        
        # Date actuelle pour le partitionnement
        self.current_date = datetime.now()
        self.year = self.current_date.year
        self.month = self.current_date.month
        self.day = self.current_date.day
        
        # Initialisé dans la méthode initialize_spark()
        self.spark = None
        
        logger.info("Processeur de la couche SILVER initialisé")
        
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
                .appName("SilverLayerProcessor")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.shuffle.partitions", "3")  # 3 partitions comme dans la couche BRONZE
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
            if not os.path.exists(self.silver_output_path):
                os.makedirs(self.silver_output_path, exist_ok=True)
                logger.info(f"Répertoire créé: {self.silver_output_path}")
            
            logger.info("Répertoires de destination vérifiés")
        except Exception as e:
            logger.error(f"Erreur lors de la vérification/création des répertoires: {e}")
            raise
    
    def discover_latest_bronze_data(self):
        """
        Découvre les dernières données disponibles dans la couche BRONZE.
        
        Returns:
            str: Chemin vers les dernières données disponibles
        """
        logger.info("Recherche des dernières données dans la couche BRONZE...")
        
        try:
            # Vérifier si le chemin existe
            if not os.path.exists(self.bronze_path):
                logger.warning(f"Le chemin {self.bronze_path} n'existe pas")
                return None
            
            # Construction du chemin avec partitionnement pour la date actuelle
            latest_path = f"{self.bronze_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Vérifier si le chemin existe
            if not os.path.exists(latest_path):
                logger.warning(f"Aucune donnée récente trouvée à {latest_path}")
                return None
                
            logger.info(f"Données récentes trouvées: {latest_path}")
            return latest_path
            
        except Exception as e:
            logger.error(f"Erreur lors de la découverte des données: {e}")
            return None
    
    def read_bronze_data(self):
        """
        Lit les données Parquet depuis la couche BRONZE.
        
        Returns:
            DataFrame: DataFrame Spark contenant les données bronze
        """
        try:
            # Découvrir les dernières données disponibles
            latest_path = self.discover_latest_bronze_data()
            
            if not latest_path:
                logger.warning("Aucune donnée BRONZE disponible")
                return None
            
            # Lister les fichiers .parquet dans le répertoire
            parquet_files = []
            for root, dirs, files in os.walk(latest_path):
                parquet_files.extend([os.path.join(root, f) for f in files if f.endswith('.parquet')])
            
            if not parquet_files:
                logger.warning(f"Aucun fichier Parquet trouvé dans {latest_path}")
                return None
            
            logger.info(f"Lecture des données depuis {parquet_files}")
            
            # Lecture des fichiers Parquet
            df = self.spark.read.parquet(*parquet_files)
            
            # Vérification des données lues
            row_count = df.count()
            column_count = len(df.columns)
            logger.info(f"Données BRONZE lues avec succès: {row_count} lignes, {column_count} colonnes")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données BRONZE: {e}")
            traceback.print_exc()
            return None
    
    def create_property_details(self, df):
        """
        Crée la table dimensionnelle property_details avec les caractéristiques principales des propriétés.
        
        Args:
            df (DataFrame): DataFrame source contenant les données BRONZE
            
        Returns:
            DataFrame: Table dimensionnelle property_details
        """
        if df is None:
            return None
            
        logger.info("Création de la table dimensionnelle property_details...")
        
        try:
            # Vérifier les colonnes disponibles dans le DataFrame
            available_columns = set(df.columns)
            logger.info(f"Colonnes disponibles: {available_columns}")
            
            # Définition des types de données pour chaque colonne
            column_types = {
                "property_type": StringType(),
                "price": DoubleType(),
                "surface": DoubleType(),
                "condition": StringType(),
                "energy_rating": StringType(),
                "listing_date": DateType(),
                "transaction_date": DateType(),
                "processing_timestamp": TimestampType(),
                "source_system": StringType(),
                "data_quality_score": DoubleType(),
                "year_built": IntegerType()
            }
            
            # Liste des expressions de sélection pour les colonnes
            select_expressions = []
            
            # Ajouter la colonne id/property_id
            if "Id" in available_columns:
                select_expressions.append(col("Id").alias("property_id"))
            elif "id" in available_columns:
                select_expressions.append(col("id").alias("property_id"))
            else:
                # Générer un ID séquentiel si aucun ID n'est disponible
                select_expressions.append(expr("monotonically_increasing_id()").alias("property_id"))
            
            # Mappings spécifiques pour ce dataset
            # Type de propriété (utiliser MSSubClass)
            if "MSSubClass" in available_columns:
                select_expressions.append(col("MSSubClass").cast(StringType()).alias("property_type"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("property_type"))
            
            # Prix (calculé à partir de OverallQual et LotArea)
            if "OverallQual" in available_columns and "LotArea" in available_columns:
                select_expressions.append(
                    (col("OverallQual") * col("LotArea") * lit(10.0)).cast(DoubleType()).alias("price")
                )
            else:
                select_expressions.append(lit(None).cast(DoubleType()).alias("price"))
            
            # Surface (utiliser LotArea)
            if "LotArea" in available_columns:
                select_expressions.append(col("LotArea").cast(DoubleType()).alias("surface"))
            else:
                select_expressions.append(lit(None).cast(DoubleType()).alias("surface"))
            
            # Condition (utiliser OverallCond)
            if "OverallCond" in available_columns:
                select_expressions.append(col("OverallCond").cast(StringType()).alias("condition"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("condition"))
            
            # Rating énergétique basé sur HeatingQC
            if "HeatingQC" in available_columns:
                select_expressions.append(
                    when(col("HeatingQC") == "Ex", "A")
                    .when(col("HeatingQC") == "Gd", "B")
                    .when(col("HeatingQC") == "TA", "C")
                    .when(col("HeatingQC") == "Fa", "D")
                    .when(col("HeatingQC") == "Po", "E")
                    .otherwise(lit("Non évalué"))
                    .cast(StringType()).alias("energy_rating")
                )
            else:
                select_expressions.append(lit("Non évalué").cast(StringType()).alias("energy_rating"))
            
            # Date de transaction à partir de YrSold et MoSold
            if "YrSold" in available_columns and "MoSold" in available_columns:
                select_expressions.append(
                    expr(f"to_date(concat(YrSold, '-', MoSold, '-15'), 'yyyy-M-dd')").alias("transaction_date")
                )
            else:
                select_expressions.append(lit(None).cast(DateType()).alias("transaction_date"))
            
            # Listing date approximative (1 mois avant la vente)
            if "YrSold" in available_columns and "MoSold" in available_columns:
                select_expressions.append(
                    expr("""
                        to_date(
                            concat(
                                YrSold,
                                '-',
                                CASE
                                    WHEN MoSold = 1 THEN '12'
                                    ELSE CAST(MoSold-1 AS STRING)
                                END,
                                '-15'
                            ),
                            'yyyy-M-dd'
                        )
                    """).alias("listing_date")
                )
            else:
                # Si on n'a pas de date de vente, utilisez une valeur par défaut
                select_expressions.append(lit("2008-12-01").cast(DateType()).alias("listing_date"))
            
            # Colonnes de métadonnées
            for column_name in ["processing_timestamp", "source_system", "data_quality_score"]:
                if column_name in available_columns:
                    select_expressions.append(col(column_name))
                else:
                    col_type = column_types.get(column_name, StringType())
                    select_expressions.append(lit(None).cast(col_type).alias(column_name))
            
            # Calcul de l'âge de la propriété au moment de la vente
            if "YearBuilt" in available_columns and "YrSold" in available_columns:
                select_expressions.append(
                    when(
                        (col("YearBuilt").isNotNull()) & (col("YrSold").isNotNull()),
                        col("YrSold") - col("YearBuilt")
                    ).otherwise(lit(None).cast(IntegerType())).alias("age_at_sale")
                )
            else:
                select_expressions.append(lit(None).cast(IntegerType()).alias("age_at_sale"))
            
            # Calcul du prix au pied carré
            if "OverallQual" in available_columns and "LotArea" in available_columns:
                select_expressions.append(
                    when(
                        col("LotArea") > 0,
                        round((col("OverallQual") * lit(10.0)), 2)
                    ).otherwise(lit(None).cast(DoubleType())).alias("price_per_sqft")
                )
            else:
                select_expressions.append(lit(None).cast(DoubleType()).alias("price_per_sqft"))
            
            # Création du DataFrame property_details
            property_details = df.select(*select_expressions)
            
            # Ajout des métadonnées
            property_details = property_details.withColumn("created_at", current_timestamp())
            
            logger.info("Table dimensionnelle property_details créée avec succès")
            return property_details
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de property_details: {e}")
            traceback.print_exc()
            return None
    
    def create_location_details(self, df):
        """
        Crée la table dimensionnelle location_details avec les informations de localisation.
        
        Args:
            df (DataFrame): DataFrame source contenant les données BRONZE
            
        Returns:
            DataFrame: Table dimensionnelle location_details
        """
        if df is None:
            return None
            
        logger.info("Création de la table dimensionnelle location_details...")
        
        try:
            # Vérifier les colonnes disponibles dans le DataFrame
            available_columns = set(df.columns)
            
            # Mapping entre les colonnes BRONZE et SILVER
            column_mapping = {
                "Id": "property_id",
                "Neighborhood": "neighborhood",  # Quartier/voisinage
                "MSZoning": "zoning_code",       # Zone de régulation (résidentielle, commerciale...)
                "Street": "street_type",         # Type de rue/accès
                "LotConfig": "lot_config",       # Configuration du terrain
                "LandContour": "land_contour"    # Contour du terrain
            }
            
            # Définition des types de données pour chaque colonne
            column_types = {
                "address": StringType(),
                "city": StringType(),
                "postal_code": StringType(),
                "neighborhood": StringType(),
                "region": StringType(),
                "longitude": DoubleType(),
                "latitude": DoubleType(),
                "zoning_code": StringType(),
                "street_type": StringType(),
                "lot_config": StringType(),
                "land_contour": StringType(),
                "services_proximity": StringType(),
                "processing_timestamp": TimestampType(),
                "source_system": StringType()
            }
            
            # Liste des expressions de sélection pour les colonnes
            select_expressions = []
            
            # Ajouter la colonne id/property_id
            if "Id" in available_columns:
                select_expressions.append(col("Id").alias("property_id"))
            else:
                select_expressions.append(expr("monotonically_increasing_id()").alias("property_id"))
            
            # Champs d'adresse - créer un format standardisé à partir des informations disponibles
            if "Neighborhood" in available_columns:
                # Address : combinaison d'ID et de Neighborhood
                select_expressions.append(
                    concat(lit("Propriété "), col("Id"), lit(", "), col("Neighborhood"))
                    .cast(StringType()).alias("address")
                )
            else:
                select_expressions.append(lit("Adresse inconnue").cast(StringType()).alias("address"))
                
            # City - utiliser 'Ames' car toutes les propriétés sont dans cette ville
            select_expressions.append(lit("Ames").cast(StringType()).alias("city"))
            
            # Code postal - générer un code postal fictif standard pour toutes les propriétés
            select_expressions.append(lit("50010").cast(StringType()).alias("postal_code"))
            
            # Region - mettre "Iowa" comme toutes les propriétés sont dans l'Iowa
            select_expressions.append(lit("Iowa").cast(StringType()).alias("region"))
            
            # Simuler des coordonnées approximatives basées sur le quartier
            # Ces coordonnées sont fictives, centrées autour d'Ames, Iowa
            if "Neighborhood" in available_columns:
                # Coordonnées basées simplement sur le hash du nom du quartier (très simplifié)
                select_expressions.append(
                    (lit(-93.62) + expr("(ascii(substring(Neighborhood, 1, 1)) % 10) / 100"))
                    .cast(DoubleType()).alias("longitude")
                )
                
                select_expressions.append(
                    (lit(42.03) + expr("(ascii(substring(Neighborhood, 2, 1)) % 10) / 100"))
                    .cast(DoubleType()).alias("latitude")
                )
            else:
                # Valeur par défaut: centre d'Ames, Iowa
                select_expressions.append(lit(-93.62).cast(DoubleType()).alias("longitude"))
                select_expressions.append(lit(42.03).cast(DoubleType()).alias("latitude"))
            
            # Quartier/Neighborhood
            if "Neighborhood" in available_columns:
                select_expressions.append(col("Neighborhood").alias("neighborhood"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("neighborhood"))
            
            # Informations supplémentaires sur la localisation
            if "MSZoning" in available_columns:
                select_expressions.append(col("MSZoning").alias("zoning_code"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("zoning_code"))
                
            if "Street" in available_columns:
                select_expressions.append(col("Street").alias("street_type"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("street_type"))
                
            if "LotConfig" in available_columns:
                select_expressions.append(col("LotConfig").alias("lot_config"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("lot_config"))
                
            if "LandContour" in available_columns:
                select_expressions.append(col("LandContour").alias("land_contour"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("land_contour"))
                
            # Services à proximité - basé sur le quartier
            if "Neighborhood" in available_columns:
                select_expressions.append(
                    when(col("Neighborhood").isin("NoRidge", "NridgHt", "StoneBr"), "Excellent")
                    .when(col("Neighborhood").isin("Somerst", "Timber", "Veenker", "Crawfor"), "Bon")
                    .when(col("Neighborhood").isin("BrkSide", "OldTown", "Edwards"), "Limité")
                    .otherwise("Moyen")
                    .cast(StringType()).alias("services_proximity")
                )
            else:
                select_expressions.append(lit("Moyen").cast(StringType()).alias("services_proximity"))
            
            # Colonnes de métadonnées
            for column_name in ["processing_timestamp", "source_system"]:
                if column_name in available_columns:
                    select_expressions.append(col(column_name))
                else:
                    col_type = column_types.get(column_name, StringType())
                    select_expressions.append(lit(None).cast(col_type).alias(column_name))
            
            # Création du DataFrame location_details
            location_details = df.select(*select_expressions)
            
            # Ajout des métadonnées
            location_details = location_details.withColumn("created_at", current_timestamp())
            
            logger.info("Table dimensionnelle location_details créée avec succès")
            return location_details
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de location_details: {e}")
            traceback.print_exc()
            return None
    
    def create_building_features(self, df):
        """
        Crée la table dimensionnelle building_features avec les caractéristiques du bâtiment.
        
        Args:
            df (DataFrame): DataFrame source contenant les données BRONZE
            
        Returns:
            DataFrame: Table dimensionnelle building_features
        """
        if df is None:
            return None
            
        logger.info("Création de la table dimensionnelle building_features...")
        
        try:
            # Vérifier les colonnes disponibles dans le DataFrame
            available_columns = set(df.columns)
            
            # Initialiser le DataFrame temporaire
            temp_df = df
            
            # Calcul du nombre total de pièces avec TotRmsAbvGrd et d'autres colonnes pertinentes
            if "TotRmsAbvGrd" in available_columns:
                # Utiliser directement TotRmsAbvGrd comme total de pièces
                temp_df = temp_df.withColumn("total_rooms", col("TotRmsAbvGrd"))
            else:
                # Faire la somme des pièces disponibles
                room_cols = ["BedroomAbvGr", "FullBath", "HalfBath", "KitchenAbvGr"]
                available_room_cols = [c for c in room_cols if c in available_columns]
                
                if available_room_cols:
                    # Commencer avec 0
                    total_expr = lit(0)
                    
                    # Ajouter chaque colonne disponible
                    for col_name in available_room_cols:
                        total_expr = total_expr + coalesce(col(col_name), lit(0))
                    
                    temp_df = temp_df.withColumn("total_rooms", total_expr)
                else:
                    temp_df = temp_df.withColumn("total_rooms", lit(0))
            
            # Liste des expressions de sélection pour les colonnes
            select_expressions = []
            
            # Ajouter la colonne id/property_id
            if "Id" in available_columns:
                select_expressions.append(col("Id").alias("property_id"))
            elif "id" in available_columns:
                select_expressions.append(col("id").alias("property_id"))
            else:
                select_expressions.append(expr("monotonically_increasing_id()").alias("property_id"))
            
            # Définition des types de données pour chaque colonne
            column_types = {
                "rooms": IntegerType(),
                "bedrooms": IntegerType(),
                "bathrooms": IntegerType(),
                "total_rooms": IntegerType(),
                "floor_number": IntegerType(),
                "total_floors": IntegerType(),
                "garage": BooleanType(),
                "parking": BooleanType(),
                "pool": BooleanType(),
                "garden": BooleanType(),
                "terrace": BooleanType(),
                "construction_material": StringType(),
                "roof_type": StringType(),
                "heating_system": StringType(),
                "cooling_system": StringType(),
                "processing_timestamp": TimestampType(),
                "source_system": StringType()
            }
            
            # Utiliser les colonnes du dataset pour créer celles requises pour notre modèle
            # Chambres (bedrooms) - utiliser BedroomAbvGr s'il existe
            if "BedroomAbvGr" in available_columns:
                select_expressions.append(col("BedroomAbvGr").alias("bedrooms"))
            else:
                select_expressions.append(lit(None).cast(IntegerType()).alias("bedrooms"))
            
            # Pièces (rooms) - utiliser TotRmsAbvGrd s'il existe
            if "TotRmsAbvGrd" in available_columns:
                select_expressions.append(col("TotRmsAbvGrd").alias("rooms"))
            else:
                select_expressions.append(lit(None).cast(IntegerType()).alias("rooms"))
            
            # Salles de bain (bathrooms) - combiner FullBath et HalfBath (0.5 pour chaque HalfBath)
            if "FullBath" in available_columns or "HalfBath" in available_columns:
                bath_expr = lit(0)
                if "FullBath" in available_columns:
                    bath_expr = bath_expr + coalesce(col("FullBath"), lit(0))
                if "HalfBath" in available_columns:
                    bath_expr = bath_expr + (coalesce(col("HalfBath"), lit(0)) * lit(0.5))
                select_expressions.append(bath_expr.alias("bathrooms"))
            else:
                select_expressions.append(lit(None).cast(IntegerType()).alias("bathrooms"))
            
            # Ajouter le total des pièces
            select_expressions.append(col("total_rooms").cast(IntegerType()))
            
            # Colonnes de structure: dérivées de HouseStyle
            if "HouseStyle" in available_columns:
                # floor_number - utiliser 1 pour les maisons à un étage, 2 pour les autres
                select_expressions.append(
                    when(col("HouseStyle").like("%1Story%"), lit(1))
                    .when(col("HouseStyle").like("%1.5%"), lit(1))
                    .when(col("HouseStyle").like("%2Story%"), lit(2))
                    .when(col("HouseStyle").like("%2.5%"), lit(2))
                    .when(col("HouseStyle").like("%SFoyer%"), lit(1))
                    .when(col("HouseStyle").like("%SLvl%"), lit(1))
                    .otherwise(lit(1))
                    .cast(IntegerType()).alias("floor_number")
                )
                
                # total_floors - extraire le nombre total d'étages de HouseStyle
                select_expressions.append(
                    when(col("HouseStyle").like("%1Story%"), lit(1))
                    .when(col("HouseStyle").like("%1.5%"), lit(2))
                    .when(col("HouseStyle").like("%2Story%"), lit(2))
                    .when(col("HouseStyle").like("%2.5%"), lit(3))
                    .when(col("HouseStyle").like("%SFoyer%"), lit(1))
                    .when(col("HouseStyle").like("%SLvl%"), lit(2))
                    .otherwise(lit(1))
                    .cast(IntegerType()).alias("total_floors")
                )
            else:
                # Valeurs par défaut - la majorité des maisons sont à un étage
                select_expressions.append(lit(1).cast(IntegerType()).alias("floor_number"))
                select_expressions.append(lit(1).cast(IntegerType()).alias("total_floors"))
            
            # Colonnes de caractéristiques booléennes dérivées des données actuelles
            # Garage - true si GarageType existe et n'est pas vide/NA
            if "GarageType" in available_columns:
                select_expressions.append(
                    when(
                        col("GarageType").isNotNull() & (col("GarageType") != "NA") & (col("GarageType") != ""),
                        lit(True)
                    ).otherwise(lit(False)).alias("garage")
                )
            else:
                select_expressions.append(lit(False).alias("garage"))
            
            # Parking - nous n'avons pas cette information séparément
            select_expressions.append(lit(False).alias("parking"))
            
            # Piscine - true si PoolArea > 0
            if "PoolArea" in available_columns:
                select_expressions.append(
                    when(col("PoolArea") > 0, lit(True))
                    .otherwise(lit(False)).alias("pool")
                )
            else:
                select_expressions.append(lit(False).alias("pool"))
            
            # Garden - pas d'information directe, supposons true si LotArea > 5000
            if "LotArea" in available_columns:
                select_expressions.append(
                    when(col("LotArea") > 5000, lit(True))
                    .otherwise(lit(False)).alias("garden")
                )
            else:
                select_expressions.append(lit(False).alias("garden"))
            
            # Terrace - approximation basée sur l'existence d'une terrasse/balcon (WoodDeckSF ou OpenPorchSF)
            if "WoodDeckSF" in available_columns or "OpenPorchSF" in available_columns:
                terrace_expr = lit(False)
                if "WoodDeckSF" in available_columns:
                    terrace_expr = when(col("WoodDeckSF") > 0, lit(True)).otherwise(terrace_expr)
                if "OpenPorchSF" in available_columns:
                    terrace_expr = when(col("OpenPorchSF") > 0, lit(True)).otherwise(terrace_expr)
                select_expressions.append(terrace_expr.alias("terrace"))
            else:
                select_expressions.append(lit(False).alias("terrace"))
            
            # Colonnes de construction à partir des données disponibles
            # Construction material - utiliser Exterior1st
            if "Exterior1st" in available_columns:
                select_expressions.append(col("Exterior1st").alias("construction_material"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("construction_material"))
            
            # Roof type - utiliser RoofStyle ou RoofMatl
            if "RoofMatl" in available_columns:
                select_expressions.append(col("RoofMatl").alias("roof_type"))
            elif "RoofStyle" in available_columns:
                select_expressions.append(col("RoofStyle").alias("roof_type"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("roof_type"))
            
            # Heating system - utiliser Heating
            if "Heating" in available_columns:
                select_expressions.append(col("Heating").alias("heating_system"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("heating_system"))
            
            # Cooling system - utiliser CentralAir
            if "CentralAir" in available_columns:
                select_expressions.append(
                    when(col("CentralAir") == "Y", lit("Central AC"))
                    .when(col("CentralAir") == "N", lit("None"))
                    .otherwise(lit(None)).alias("cooling_system")
                )
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("cooling_system"))
            
            # Colonnes de métadonnées
            for column_name in ["processing_timestamp", "source_system"]:
                if column_name in available_columns:
                    select_expressions.append(col(column_name))
                else:
                    col_type = column_types.get(column_name, StringType())
                    select_expressions.append(lit(None).cast(col_type).alias(column_name))
            
            # Création du DataFrame building_features
            building_features = temp_df.select(*select_expressions)
            
            # Ajout des métadonnées
            building_features = building_features.withColumn("created_at", current_timestamp())
            
            logger.info("Table dimensionnelle building_features créée avec succès")
            return building_features
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de building_features: {e}")
            traceback.print_exc()
            return None
    
    def create_sale_history(self, df):
        """
        Crée la table dimensionnelle sale_history avec l'historique des ventes.
        
        Args:
            df (DataFrame): DataFrame source contenant les données BRONZE
            
        Returns:
            DataFrame: Table dimensionnelle sale_history
        """
        if df is None:
            return None
            
        logger.info("Création de la table dimensionnelle sale_history...")
        
        try:
            # Définition des types de données pour chaque colonne
            column_types = {
                "transaction_date": DateType(),
                "sale_price": DoubleType(),
                "previous_price": DoubleType(),
                "price_change_percentage": DoubleType(),
                "days_on_market": IntegerType(),
                "seller_type": StringType(),
                "buyer_type": StringType(),
                "financing_type": StringType(),
                "sale_type": StringType(),
                "sale_condition": StringType(),
                "processing_timestamp": TimestampType(),
                "source_system": StringType()
            }
            
            # Vérifier les colonnes disponibles dans le DataFrame
            available_columns = set(df.columns)
            
            # Liste des expressions de sélection pour les colonnes
            select_expressions = []
            
            # Ajouter la colonne id/property_id
            if "Id" in available_columns:
                select_expressions.append(col("Id").alias("property_id"))
            elif "id" in available_columns:
                select_expressions.append(col("id").alias("property_id"))
            else:
                select_expressions.append(expr("monotonically_increasing_id()").alias("property_id"))
            
            # Date de transaction construite à partir de YrSold et MoSold (15 du mois)
            if "YrSold" in available_columns and "MoSold" in available_columns:
                select_expressions.append(
                    expr("to_date(concat(YrSold, '-', MoSold, '-15'), 'yyyy-M-dd')").alias("transaction_date")
                )
            else:
                select_expressions.append(lit(None).cast(DateType()).alias("transaction_date"))
            
            # Prix de vente (utiliser l'approximation calculée)
            if "OverallQual" in available_columns and "LotArea" in available_columns:
                select_expressions.append(
                    (col("OverallQual") * col("LotArea") * lit(10.0)).cast(DoubleType()).alias("sale_price")
                )
            else:
                select_expressions.append(lit(None).cast(DoubleType()).alias("sale_price"))
            
            # Ajouter les informations de vente disponibles
            if "SaleType" in available_columns:
                select_expressions.append(col("SaleType").alias("sale_type"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("sale_type"))
                
            if "SaleCondition" in available_columns:
                select_expressions.append(col("SaleCondition").alias("sale_condition"))
            else:
                select_expressions.append(lit(None).cast(StringType()).alias("sale_condition"))
            
            # Estimation du prix précédent à 90% du prix actuel (simulant une appréciation de 10%)
            if "OverallQual" in available_columns and "LotArea" in available_columns:
                select_expressions.append(
                    (col("OverallQual") * col("LotArea") * lit(9.0)).cast(DoubleType()).alias("previous_price")
                )
            else:
                select_expressions.append(lit(None).cast(DoubleType()).alias("previous_price"))
                
            # Calcul du pourcentage de changement de prix
            select_expressions.append(lit(10.0).cast(DoubleType()).alias("price_change_percentage"))
            
            # Days on market - estimé à partir de la condition générale et de la qualité
            if "OverallCond" in available_columns and "OverallQual" in available_columns:
                select_expressions.append(
                    (lit(100) - (col("OverallQual") * lit(8)) + (lit(10) - col("OverallCond")) * lit(5))
                    .cast(IntegerType()).alias("days_on_market")
                )
            else:
                select_expressions.append(lit(60).cast(IntegerType()).alias("days_on_market"))
                
            # Informations sur les parties, dérivées de SaleCondition et SaleType
            if "SaleCondition" in available_columns:
                select_expressions.append(
                    when(col("SaleCondition") == "Family", "Particulier")
                    .when(col("SaleCondition") == "Abnorml", "Bancaire")
                    .when(col("SaleCondition") == "Partial", "Constructeur")
                    .otherwise("Standard")
                    .cast(StringType()).alias("seller_type")
                )
            else:
                select_expressions.append(lit("Standard").cast(StringType()).alias("seller_type"))
                
            if "SaleType" in available_columns:
                select_expressions.append(
                    when(col("SaleType").like("%WD%"), "Particulier")
                    .when(col("SaleType").like("%New%"), "Premier acheteur")
                    .when(col("SaleType").like("%COD%"), "Judiciaire")
                    .when(col("SaleType").like("%Con%"), "Constructeur")
                    .otherwise("Autre")
                    .cast(StringType()).alias("buyer_type")
                )
            else:
                select_expressions.append(lit("Particulier").cast(StringType()).alias("buyer_type"))
                
            if "SaleType" in available_columns:
                select_expressions.append(
                    when(col("SaleType").like("%Conv%"), "Conventionnel")
                    .when(col("SaleType").like("%FHA%"), "FHA")
                    .when(col("SaleType").like("%VA%"), "VA")
                    .when(col("SaleType") == "Cash", "Comptant")
                    .otherwise("Conventionnel")
                    .cast(StringType()).alias("financing_type")
                )
            else:
                select_expressions.append(lit("Conventionnel").cast(StringType()).alias("financing_type"))
            
            # Colonnes de métadonnées
            for column_name in ["processing_timestamp", "source_system"]:
                if column_name in available_columns:
                    select_expressions.append(col(column_name))
                else:
                    col_type = column_types.get(column_name, StringType())
                    select_expressions.append(lit(None).cast(col_type).alias(column_name))
            
            # Création du DataFrame sale_history
            sale_history = df.select(*select_expressions)
            
            # Ajout des métadonnées
            sale_history = sale_history.withColumn("created_at", current_timestamp())
            
            logger.info("Table dimensionnelle sale_history créée avec succès")
            return sale_history
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de sale_history: {e}")
            traceback.print_exc()
            return None
    
    def write_dimensional_table(self, df, table_name):
        """
        Écrit une table dimensionnelle au format Parquet dans la couche SILVER.
        
        Args:
            df (DataFrame): DataFrame à écrire
            table_name (str): Nom de la table dimensionnelle
            
        Returns:
            bool: True si l'écriture a réussi, False sinon
        """
        if df is None:
            return False
            
        logger.info(f"Écriture de la table dimensionnelle {table_name}...")
        
        try:
            # Définition du chemin de sortie avec partitionnement
            output_path = f"{self.silver_output_path}/{table_name}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Création du nom de fichier
            file_name = f"{table_name}_{self.year}{self.month:02d}{self.day:02d}"
            
            # S'assurer que le répertoire existe
            os.makedirs(output_path, exist_ok=True)
            
            # Écriture au format Parquet avec compression Snappy
            df.repartition(3).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{output_path}/{file_name}.parquet")
            
            logger.info(f"Table {table_name} écrite avec succès à {output_path}/{file_name}.parquet")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture de la table {table_name}: {e}")
            traceback.print_exc()
            return False
    
    def process_data(self):
        """
        Traite les données de la couche BRONZE en tables dimensionnelles pour la couche SILVER.
        
        Returns:
            dict: Statuts de traitement pour chaque table dimensionnelle
        """
        try:
            logger.info("Traitement des données pour la couche SILVER...")
            
            # 1. Lecture des données depuis la couche BRONZE
            bronze_df = self.read_bronze_data()
            
            if bronze_df is None:
                logger.warning("Aucune donnée BRONZE à traiter")
                return {
                    "status": "warning",
                    "message": "Aucune donnée BRONZE à traiter"
                }
            
            # 2. Création des tables dimensionnelles
            
            # 2.1 Property Details
            property_details_df = self.create_property_details(bronze_df)
            property_details_status = self.write_dimensional_table(
                property_details_df, "property_details"
            )
            
            # 2.2 Location Details
            location_details_df = self.create_location_details(bronze_df)
            location_details_status = self.write_dimensional_table(
                location_details_df, "location_details"
            )
            
            # 2.3 Building Features
            building_features_df = self.create_building_features(bronze_df)
            building_features_status = self.write_dimensional_table(
                building_features_df, "building_features"
            )
            
            # 2.4 Sale History
            sale_history_df = self.create_sale_history(bronze_df)
            sale_history_status = self.write_dimensional_table(
                sale_history_df, "sale_history"
            )
            
            # Récupération des statistiques pour le log
            stats = {
                "property_details": property_details_df.count() if property_details_df else 0,
                "location_details": location_details_df.count() if location_details_df else 0,
                "building_features": building_features_df.count() if building_features_df else 0,
                "sale_history": sale_history_df.count() if sale_history_df else 0
            }
            
            logger.info(f"Statistiques des tables dimensionnelles: {stats}")
            
            # Retour des statuts
            return {
                "status": "success",
                "tables": {
                    "property_details": "success" if property_details_status else "failed",
                    "location_details": "success" if location_details_status else "failed",
                    "building_features": "success" if building_features_status else "failed",
                    "sale_history": "success" if sale_history_status else "failed"
                },
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données SILVER: {e}")
            traceback.print_exc()
            return {
                "status": "error",
                "message": str(e)
            }
    
    def run(self):
        """
        Exécute le processeur complet pour la transformation BRONZE vers SILVER.
        
        Returns:
            dict: Résultat d'exécution avec statuts
        """
        logger.info("=== Démarrage du processeur de la couche SILVER ===")
        
        try:
            # Initialisation de Spark
            self.initialize_spark()
            
            # Vérification/création des répertoires de destination
            self.ensure_output_directories()
            
            # Traitement des données
            processing_result = self.process_data()
            
            # Arrêt de la session Spark
            if self.spark:
                self.spark.stop()
                logger.info("Session Spark arrêtée")
            
            # Construction du résultat
            result = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processing_result": processing_result,
                "overall_status": processing_result.get("status", "error")
            }
            
            logger.info(f"=== Fin du processeur de la couche SILVER: {result['overall_status']} ===")
            return result
            
        except Exception as e:
            logger.error(f"Erreur générale lors de l'exécution du processeur: {e}")
            traceback.print_exc()
            
            # Arrêt de la session Spark en cas d'erreur
            if self.spark:
                self.spark.stop()
                logger.info("Session Spark arrêtée suite à une erreur")
            
            return {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error",
                "message": str(e)
            }


def main():
    """
    Fonction principale pour exécuter le processeur.
    """
    try:
        # Création d'une instance du processeur avec les chemins par défaut
        processor = SilverLayerProcessor()
        
        # Exécution du processeur
        result = processor.run()
        
        # Affichage du résultat
        if result.get("overall_status") == "success":
            logger.info("Traitement de la couche SILVER terminé avec succès")
            return 0
        else:
            logger.error(f"Traitement de la couche SILVER terminé avec des erreurs: {result}")
            return 1
            
    except Exception as e:
        logger.critical(f"Erreur fatale lors de l'exécution: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    """
    Point d'entrée du script lorsqu'il est exécuté directement.
    """
    sys.exit(main())
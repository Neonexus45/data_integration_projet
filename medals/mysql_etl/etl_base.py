#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Classe de base pour les ETL des datamarts MySQL
Fournit les fonctionnalités communes à tous les datamarts
"""

import os
import sys
import logging
import traceback
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# Imports PySpark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    lit, current_timestamp, year, month, day, col, when, 
    isnull, count, mean, min, max, round, expr, coalesce,
    row_number
)
from pyspark.sql.types import *

# Configuration du logging
log_dir = "medals/mysql_etl"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "etl_gold_to_mysql.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BaseETL:
    """
    Classe de base pour l'ETL Gold vers MySQL.
    
    Cette classe est responsable de:
    - Initialiser Spark et la connexion MySQL
    - Découvrir et lire les données Gold
    - Fournir les méthodes communes d'insertion dans MySQL
    """
    
    def __init__(self, gold_path='data/gold', 
                host='localhost', user='tatane', password='tatane', 
                database='immobilier_prediction_db'):
        """
        Initialise l'ETL Gold vers MySQL.
        
        Args:
            gold_path (str): Chemin vers les données de la couche GOLD
            host (str): Hôte du serveur MySQL
            user (str): Utilisateur MySQL
            password (str): Mot de passe MySQL
            database (str): Nom de la base de données MySQL
        """
        self.gold_path = gold_path
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        # Date actuelle pour trouver les données les plus récentes
        self.current_date = datetime.now()
        self.year = self.current_date.year
        self.month = self.current_date.month
        self.day = self.current_date.day
        
        # Initialisé dans la méthode initialize_spark()
        self.spark = None
        
        # Connexion MySQL
        self.conn = None
        self.cursor = None
        
        # Dictionnaire pour stocker les DataFrames GOLD
        self.gold_tables = {}
        
        logger.info("ETL Base initialisé")
    
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
                .appName("GoldToMySqlETL")
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
            bool: True si la connexion est réussie, False sinon
        """
        logger.info(f"Connexion à MySQL ({self.host}, {self.user}, {self.database})...")
        
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            logger.info("Connexion à MySQL réussie")
            return True
            
        except Error as e:
            logger.error(f"Erreur lors de la connexion à MySQL: {e}")
            traceback.print_exc()
            return False
    
    def disconnect_from_mysql(self):
        """
        Ferme la connexion à MySQL.
        """
        logger.info("Fermeture de la connexion MySQL...")
        
        if self.cursor:
            self.cursor.close()
        
        if self.conn:
            self.conn.close()
            logger.info("Connexion MySQL fermée")
    
    def test_mysql_connection(self):
        """
        Teste la connexion à MySQL et vérifie que les tables requises existent.
        
        Returns:
            bool: True si la connexion est réussie et les tables existent, False sinon
        """
        required_tables = [
            # Datamart 1
            "dm_regional_price_trends",
            "dm_neighborhood_comparison",
            "dm_seasonal_patterns",
            # Datamart 2
            "dm_feature_importance",
            "dm_regional_feature_variation",
            "dm_temporal_feature_variation",
            # Datamart 3
            "dm_prediction_models",
            "dm_prediction_results"
        ]
        
        try:
            # Connexion à MySQL
            if not self.connect_to_mysql():
                return False
            
            # Vérifier l'existence des tables
            self.cursor.execute("SHOW TABLES")
            existing_tables = [table[0] for table in self.cursor.fetchall()]
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            
            if missing_tables:
                logger.error(f"Tables manquantes dans la base de données: {missing_tables}")
                return False
            
            logger.info("Toutes les tables requises existent dans la base de données")
            return True
            
        except Error as e:
            logger.error(f"Erreur lors du test de connexion MySQL: {e}")
            return False
        finally:
            self.disconnect_from_mysql()
    
    def discover_latest_gold_data(self, table_name):
        """
        Découvre les dernières données disponibles dans la couche GOLD pour une table spécifique.
        Cherche d'abord à la date actuelle, puis explore récursivement pour trouver des données plus anciennes.
        
        Args:
            table_name (str): Nom de la table analytique
            
        Returns:
            str: Chemin vers les dernières données disponibles
        """
        logger.info(f"Recherche des dernières données pour la table '{table_name}' dans la couche GOLD...")
        
        try:
            # Vérifier si le chemin existe
            table_path = f"{self.gold_path}/{table_name}"
            if not os.path.exists(table_path):
                logger.warning(f"Le chemin {table_path} n'existe pas")
                return None
            
            # Construction du chemin avec partitionnement pour la date actuelle
            latest_path = f"{table_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Vérifier si le chemin existe
            if os.path.exists(latest_path):
                logger.info(f"Données récentes trouvées pour '{table_name}': {latest_path}")
                return latest_path
            
            # Si aucune donnée n'est trouvée à la date actuelle, rechercher récursivement
            logger.info(f"Aucune donnée trouvée à la date actuelle, recherche des données les plus récentes...")
            
            # Explorer le répertoire de manière récursive pour trouver les données les plus récentes
            found_paths = []
            for root, dirs, files in os.walk(table_path):
                # Vérifier si le répertoire contient des fichiers Parquet
                parquet_files = [f for f in files if f.endswith('.parquet')]
                if parquet_files:
                    found_paths.append(root)
            
            if not found_paths:
                logger.warning(f"Aucune donnée trouvée pour '{table_name}'")
                return None
            
            # Trier les chemins par date de modification (du plus récent au plus ancien)
            found_paths.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            latest_path = found_paths[0]
            logger.info(f"Données les plus récentes trouvées pour '{table_name}': {latest_path}")
            return latest_path
            
        except Exception as e:
            logger.error(f"Erreur lors de la découverte des données pour '{table_name}': {e}")
            return None
    
    def read_gold_data(self, table_names):
        """
        Lit les données analytiques Parquet depuis la couche GOLD.
        Stocke les DataFrames dans le dictionnaire gold_tables.
        
        Args:
            table_names (list): Liste des noms des tables à lire
            
        Returns:
            bool: True si toutes les données ont été lues avec succès, False sinon
        """
        try:
            all_tables_loaded = True
            
            # Lire chaque table analytique
            for table_name in table_names:
                try:
                    # Découvrir les dernières données disponibles
                    latest_path = self.discover_latest_gold_data(table_name)
                    
                    if not latest_path:
                        logger.warning(f"Aucune donnée GOLD disponible pour '{table_name}'")
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
                    logger.info(f"Données GOLD pour '{table_name}' lues avec succès: {row_count} lignes, {column_count} colonnes")
                    
                    # Stocker le DataFrame dans le dictionnaire
                    self.gold_tables[table_name] = df
                    
                except Exception as e:
                    logger.error(f"Erreur lors de la lecture des données GOLD pour '{table_name}': {e}")
                    all_tables_loaded = False
            
            return all_tables_loaded
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données GOLD: {e}")
            traceback.print_exc()
            return False
    
    def insert_into_mysql(self, df, table_name, replace=True):
        """
        Insère un DataFrame dans une table MySQL.
        
        Args:
            df (DataFrame): DataFrame à insérer
            table_name (str): Nom de la table MySQL
            replace (bool): Si True, utilise REPLACE INTO, sinon INSERT INTO
            
        Returns:
            int: Nombre d'enregistrements insérés
        """
        if df is None:
            logger.warning(f"Aucune donnée à insérer dans la table {table_name} (DataFrame est None)")
            return 0
        
        try:
            count = df.count()
            if count == 0:
                logger.warning(f"Aucune donnée à insérer dans la table {table_name} (DataFrame vide)")
                return 0
        except Exception as e:
            logger.warning(f"Erreur lors du comptage des lignes pour {table_name}: {e}")
            # On continue quand même, peut-être que le DataFrame est valide malgré l'erreur
            
        logger.info(f"Insertion des données dans la table MySQL {table_name}...")
        
        try:
            # Connexion à MySQL
            if not self.connect_to_mysql():
                return 0
            
            # Convertir le DataFrame en une liste de tuples
            rows = df.collect()
            
            # Préparer la requête SQL
            columns = df.columns
            placeholders = ", ".join(["%s"] * len(columns))
            columns_str = ", ".join(columns)
            
            # Utiliser REPLACE INTO ou INSERT INTO selon le paramètre
            operation = "REPLACE INTO" if replace else "INSERT INTO"
            query = f"{operation} {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Insérer les données par lots
            batch_size = 1000
            records_inserted = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                values = []
                
                for row in batch:
                    # Convertir chaque ligne en tuple de valeurs
                    row_values = []
                    for col in columns:
                        val = getattr(row, col)
                        row_values.append(val)
                    
                    values.append(tuple(row_values))
                
                # Exécuter la requête d'insertion
                self.cursor.executemany(query, values)
                self.conn.commit()
                
                records_inserted += len(batch)
                logger.info(f"Lot de {len(batch)} enregistrements inséré dans {table_name}")
            
            logger.info(f"Insertion dans {table_name} terminée: {records_inserted} enregistrements")
            return records_inserted
            
        except Error as e:
            logger.error(f"Erreur lors de l'insertion dans {table_name}: {e}")
            traceback.print_exc()
            return 0
        finally:
            self.disconnect_from_mysql()
    
    def insert_with_direct_query(self, query, params=None):
        """
        Exécute une requête SQL directement sur la base MySQL.
        
        Args:
            query (str): Requête SQL à exécuter
            params (tuple, optional): Paramètres de la requête
            
        Returns:
            int: Nombre de lignes affectées
        """
        try:
            # Connexion à MySQL
            if not self.connect_to_mysql():
                return 0
            
            # Exécuter la requête
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            # Valider la transaction
            self.conn.commit()
            
            # Récupérer le nombre de lignes affectées
            rows_affected = self.cursor.rowcount
            logger.info(f"Requête exécutée avec succès: {rows_affected} lignes affectées")
            
            return rows_affected
            
        except Error as e:
            logger.error(f"Erreur lors de l'exécution de la requête SQL: {e}")
            traceback.print_exc()
            return 0
        finally:
            self.disconnect_from_mysql()
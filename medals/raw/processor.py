#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de traitement de la couche RAW - Phase 2.1
-------------------------------------------------

Ce script implémente le processeur de la couche RAW pour le pipeline de données immobilières.
Il est responsable de:
1. Récupérer les données immobilières depuis les tables MySQL (statique et streaming)
2. Convertir ces données au format Parquet avec compression Snappy
3. Stocker les données dans une structure partitionnée par année/mois/jour

Fonctionnalités:
- Utilisation de PySpark pour le traitement des données
- Lecture depuis MySQL (tables proprietes_raw et proprietes_raw_streaming)
- Partitionnement des données selon l'architecture du système
- Gestion robuste des erreurs
- Idempotence (peut être exécuté plusieurs fois sans problème)
"""

import os
import sys
import logging
from datetime import datetime
import traceback

# Imports PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, year, month, day, hour
from pyspark.sql.types import *

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RawLayerProcessor:
    """
    Classe principale pour le traitement de la couche RAW.
    
    Cette classe est responsable de:
    - Récupérer les données brutes des différentes sources MySQL
    - Convertir ces données au format Parquet
    - Organiser les données en partitions année/mois/jour
    - Stocker le résultat dans les répertoires appropriés
    """
    
    def __init__(self, batch_output_path='data/raw/batch',
                 streaming_output_path='data/raw/streaming',
                 mysql_config=None):
        """
        Initialise le processeur de la couche RAW.
        
        Args:
            batch_output_path (str): Chemin de destination pour les données statiques traitées
            streaming_output_path (str): Chemin de destination pour les données streaming traitées
            mysql_config (dict): Configuration de connexion MySQL (par défaut: paramètres prédéfinis)
        """
        self.batch_output_path = batch_output_path
        self.streaming_output_path = streaming_output_path
        
        # Configuration par défaut pour MySQL si non fournie
        self.mysql_config = mysql_config or {
            'host': 'localhost',
            'user': 'tatane',
            'password': 'tatane',
            'database': 'immobilier_db'
        }
        
        # Date actuelle pour le partitionnement
        self.current_date = datetime.now()
        self.year = self.current_date.year
        self.month = self.current_date.month
        self.day = self.current_date.day
        self.hour = self.current_date.hour
        
        # Initialisé dans la méthode initialize_spark()
        self.spark = None
        
        logger.info("Processeur de la couche RAW initialisé")
        
    def initialize_spark(self):
        """
        Initialise une session Spark avec les configurations appropriées.
        
        Returns:
            SparkSession: Session Spark configurée
        """
        logger.info("Initialisation de la session Spark...")
        
        try:
            # Configuration de Spark avec support MySQL et Parquet
            # Ajout de configurations pour améliorer la stabilité
            self.spark = (SparkSession.builder
                .appName("RawLayerProcessor")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28")
                .config("spark.driver.extraClassPath", "./mysql-connector-j-8.0.33.jar")  # Ajouter si le JAR est disponible localement
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.shuffle.partitions", "10")  # Réduire pour les petits datasets
                .config("spark.executor.memory", "1g")  # Ajuster selon les ressources disponibles
                .config("spark.driver.memory", "1g")    # Ajuster selon les ressources disponibles
                .config("spark.local.dir", "./spark-temp")  # Répertoire temporaire explicite
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
            # Création des répertoires de base s'ils n'existent pas
            for path in [self.batch_output_path, self.streaming_output_path]:
                if not os.path.exists(path):
                    os.makedirs(path, exist_ok=True)
                    logger.info(f"Répertoire créé: {path}")
            
            logger.info("Répertoires de destination vérifiés")
        except Exception as e:
            logger.error(f"Erreur lors de la vérification/création des répertoires: {e}")
            raise
    
    def process_batch_data(self):
        """
        Traite les données statiques depuis la table MySQL proprietes_raw.
        Convertit et stocke les données au format Parquet avec partitionnement.
        
        Returns:
            bool: True si le traitement a réussi, False sinon
        """
        try:
            logger.info("Traitement des données statiques depuis MySQL (table proprietes_raw)...")
            
            # Construction de la chaîne de connexion JDBC avec options supplémentaires
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}/{self.mysql_config['database']}?useSSL=false&allowPublicKeyRetrieval=true"
            
            # Propriétés de connexion
            connection_properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # Vérifier si la table existe et contient des données
            try:
                logger.info("Vérification de la table proprietes_raw...")
                df_check = self.spark.read.jdbc(
                    url=jdbc_url,
                    table="(SELECT COUNT(*) AS count FROM proprietes_raw) AS check_table",
                    properties=connection_properties
                )
                
                row_count = df_check.first()["count"]
                logger.info(f"Table proprietes_raw contient {row_count} lignes")
                
                if row_count == 0:
                    logger.warning("Table proprietes_raw est vide")
                    return False
                    
            except Exception as e:
                logger.error(f"Erreur lors de la vérification de la table proprietes_raw: {e}")
                logger.error("Assurez-vous que la table existe et que les permissions sont correctes")
                return False
            
            # Lecture des données depuis MySQL table proprietes_raw
            logger.info("Lecture des données depuis la table proprietes_raw...")
            
            # Lecture avec des options supplémentaires
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table="proprietes_raw",
                properties=connection_properties
            )
            
            # Ajout de métadonnées
            df = df.withColumn("processing_timestamp", current_timestamp()) \
                   .withColumn("source_type", lit("batch"))
            
            # Construction du chemin de sortie avec partitionnement
            output_path = f"{self.batch_output_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Création du nom de fichier
            file_name = f"proprietes_raw_{self.year}{self.month:02d}{self.day:02d}"
            
            # S'assurer que le répertoire existe
            os.makedirs(output_path, exist_ok=True)
            
            # Écriture au format Parquet avec compression Snappy et moins de partitions
            try:
                logger.info(f"Écriture des données au format Parquet: {output_path}/{file_name}.parquet")
                
                # Utiliser coalesce pour réduire le nombre de fichiers de sortie
                df.coalesce(1).write.mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(f"{output_path}/{file_name}.parquet")
                
                logger.info("Écriture Parquet réussie")
                
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture Parquet: {e}")
                
                # Plan B: écrire au format CSV si Parquet échoue
                logger.info("Tentative de fallback vers CSV...")
                df.coalesce(1).write.mode("overwrite").csv(f"{output_path}/{file_name}.csv")
                logger.info(f"Données écrites au format CSV: {output_path}/{file_name}.csv")
            
            # Récupération de statistiques pour le log
            row_count = df.count()
            column_count = len(df.columns)
            
            logger.info(f"Données statiques traitées avec succès: {row_count} lignes, {column_count} colonnes")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données statiques: {e}")
            traceback.print_exc()
            return False
    
    def process_streaming_data(self):
        """
        Traite les données en streaming depuis la base MySQL.
        Convertit et stocke les données au format Parquet avec partitionnement.
        
        Returns:
            bool: True si le traitement a réussi, False sinon
        """
        try:
            logger.info("Traitement des données en streaming depuis MySQL...")
            
            # Construction de la chaîne de connexion JDBC avec options supplémentaires
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}/{self.mysql_config['database']}?useSSL=false&allowPublicKeyRetrieval=true"
            
            # Propriétés de connexion
            connection_properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # Vérifier si la table existe et contient des données
            try:
                logger.info("Vérification de la table proprietes_raw_streaming...")
                df_check = self.spark.read.jdbc(
                    url=jdbc_url,
                    table="(SELECT COUNT(*) AS count FROM proprietes_raw_streaming) AS check_table",
                    properties=connection_properties
                )
                
                row_count = df_check.first()["count"]
                logger.info(f"Table proprietes_raw_streaming contient {row_count} lignes")
                
                if row_count == 0:
                    logger.warning("Table proprietes_raw_streaming est vide")
                    return True  # On considère que c'est un succès même si pas de données
                    
            except Exception as e:
                logger.error(f"Erreur lors de la vérification de la table proprietes_raw_streaming: {e}")
                logger.error("Assurez-vous que la table existe et que les permissions sont correctes")
                return False
            
            # Lecture des données depuis MySQL
            logger.info("Lecture des données depuis la table proprietes_raw_streaming...")
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table="proprietes_raw_streaming",
                properties=connection_properties
            )
            
            # Ajout de métadonnées
            df = df.withColumn("processing_timestamp", current_timestamp()) \
                   .withColumn("source_type", lit("streaming"))
            
            # Construction du chemin de sortie avec partitionnement
            output_path = f"{self.streaming_output_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}/hour={self.hour:02d}"
            
            # Création du nom de fichier
            file_name = f"proprietes_raw_streaming_{self.year}{self.month:02d}{self.day:02d}{self.hour:02d}"
            
            # S'assurer que le répertoire existe
            os.makedirs(output_path, exist_ok=True)
            
            # Écriture au format Parquet avec compression Snappy et moins de partitions
            try:
                logger.info(f"Écriture des données au format Parquet: {output_path}/{file_name}.parquet")
                
                # Utiliser coalesce pour réduire le nombre de fichiers de sortie
                df.coalesce(1).write.mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(f"{output_path}/{file_name}.parquet")
                
                logger.info("Écriture Parquet réussie")
                
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture Parquet: {e}")
                
                # Plan B: écrire au format CSV si Parquet échoue
                logger.info("Tentative de fallback vers CSV...")
                df.coalesce(1).write.mode("overwrite").csv(f"{output_path}/{file_name}.csv")
                logger.info(f"Données écrites au format CSV: {output_path}/{file_name}.csv")
            
            # Récupération de statistiques pour le log
            row_count = df.count()
            column_count = len(df.columns)
            
            logger.info(f"Données streaming traitées avec succès: {row_count} lignes, {column_count} colonnes")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données streaming: {e}")
            traceback.print_exc()
            return False
    
    def run(self):
        """
        Exécute le processeur complet pour les deux sources de données.
        
        Returns:
            dict: Résultat d'exécution avec statuts
        """
        logger.info("=== Démarrage du processeur de la couche RAW ===")
        
        try:
            # Initialisation de Spark
            self.initialize_spark()
            
            # Vérification/création des répertoires de destination
            self.ensure_output_directories()
            
            # Traitement des données statiques
            batch_success = self.process_batch_data()
            
            # Traitement des données streaming
            streaming_success = self.process_streaming_data()
            
            # Arrêt de la session Spark
            if self.spark:
                self.spark.stop()
                logger.info("Session Spark arrêtée")
            
            # Construction du résultat
            result = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "batch_processing": "success" if batch_success else "failed",
                "streaming_processing": "success" if streaming_success else "failed",
                "overall_status": "success" if (batch_success and streaming_success) else "failed"
            }
            
            logger.info(f"=== Fin du processeur de la couche RAW: {result['overall_status']} ===")
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
        processor = RawLayerProcessor()
        
        # Exécution du processeur
        result = processor.run()
        
        # Affichage du résultat
        if result.get("overall_status") == "success":
            logger.info("Traitement de la couche RAW terminé avec succès")
            return 0
        else:
            logger.error(f"Traitement de la couche RAW terminé avec des erreurs: {result}")
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
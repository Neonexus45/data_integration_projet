#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de traitement de la couche BRONZE - Phase 2.2
----------------------------------------------------

Ce script implémente le processeur de la couche BRONZE pour le pipeline de données immobilières.
Il est responsable de:
1. Lire les données Parquet depuis la couche RAW
2. Nettoyer et valider ces données selon les règles métier
3. Stocker les données nettoyées au format Parquet dans la couche BRONZE
4. Implémenter le partitionnement selon l'architecture (année/mois/jour)

Fonctionnalités:
- Utilisation de PySpark pour le traitement des données
- Lecture des données depuis la couche RAW (data/raw/batch/ et data/raw/streaming/)
- Transformations:
  * Conversion des types de données
  * Standardisation des formats
  * Remplacement des valeurs nulles
  * Détection et marquage des anomalies
- Ajout de métadonnées:
  * processing_timestamp: Horodatage du traitement
  * source_system: Batch ou Streaming
  * data_quality_score: Score de qualité calculé
- Structure partitionnée dans data/bronze/ avec optimisation pour la parallélisation
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
    count, mean, stddev, min, max, lower
)
from pyspark.sql.types import *

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeLayerProcessor:
    """
    Classe principale pour le traitement de la couche BRONZE.
    
    Cette classe est responsable de:
    - Lire les données Parquet depuis la couche RAW
    - Nettoyer et valider ces données selon les règles métier
    - Ajouter des métadonnées pour le suivi et la qualité
    - Stocker les données nettoyées dans la couche BRONZE avec partitionnement
    """
    
    def __init__(self, raw_batch_path='data/raw/batch',
                 raw_streaming_path='data/raw/streaming',
                 bronze_output_path='data/bronze'):
        """
        Initialise le processeur de la couche BRONZE.
        
        Args:
            raw_batch_path (str): Chemin source pour les données batch de la couche RAW
            raw_streaming_path (str): Chemin source pour les données streaming de la couche RAW
            bronze_output_path (str): Chemin de destination pour les données traitées
        """
        self.raw_batch_path = raw_batch_path
        self.raw_streaming_path = raw_streaming_path
        self.bronze_output_path = bronze_output_path
        
        # Date actuelle pour le partitionnement
        self.current_date = datetime.now()
        self.year = self.current_date.year
        self.month = self.current_date.month
        self.day = self.current_date.day
        self.hour = self.current_date.hour
        
        # Initialisé dans la méthode initialize_spark()
        self.spark = None
        
        logger.info("Processeur de la couche BRONZE initialisé")
        
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
                .appName("BronzeLayerProcessor")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.adaptive.enabled", "true")
                # Configuration pour 3 partitions comme demandé
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
    
    def ensure_output_directories(self):
        """
        S'assure que les répertoires de destination existent.
        Crée les répertoires si nécessaire.
        """
        logger.info("Vérification des répertoires de destination...")
        
        try:
            # Création du répertoire de base s'il n'existe pas
            if not os.path.exists(self.bronze_output_path):
                os.makedirs(self.bronze_output_path, exist_ok=True)
                logger.info(f"Répertoire créé: {self.bronze_output_path}")
            
            logger.info("Répertoires de destination vérifiés")
        except Exception as e:
            logger.error(f"Erreur lors de la vérification/création des répertoires: {e}")
            raise
    
    def discover_latest_raw_data(self, source_type='batch'):
        """
        Découvre les dernières données disponibles dans la couche RAW.
        
        Args:
            source_type (str): Type de source - 'batch' ou 'streaming'
            
        Returns:
            str: Chemin vers les dernières données disponibles
        """
        logger.info(f"Recherche des dernières données {source_type} dans la couche RAW...")
        
        try:
            # Déterminer le chemin de base selon le type de source
            base_path = self.raw_batch_path if source_type == 'batch' else self.raw_streaming_path
            
            # Vérifier si le chemin existe
            if not os.path.exists(base_path):
                logger.warning(f"Le chemin {base_path} n'existe pas")
                return None
            
            # Construction du chemin avec partitionnement pour la date actuelle
            if source_type == 'batch':
                latest_path = f"{base_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            else:
                latest_path = f"{base_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}/hour={self.hour:02d}"
            
            # Vérifier si le chemin existe
            if not os.path.exists(latest_path):
                logger.warning(f"Aucune donnée récente trouvée à {latest_path}")
                return None
                
            logger.info(f"Données récentes {source_type} trouvées: {latest_path}")
            return latest_path
            
        except Exception as e:
            logger.error(f"Erreur lors de la découverte des données: {e}")
            return None
    
    def read_raw_data(self, source_type='batch'):
        """
        Lit les données Parquet depuis la couche RAW.
        
        Args:
            source_type (str): Type de source - 'batch' ou 'streaming'
            
        Returns:
            DataFrame: DataFrame Spark contenant les données brutes
        """
        try:
            # Découvrir les dernières données disponibles
            latest_path = self.discover_latest_raw_data(source_type)
            
            if not latest_path:
                logger.warning(f"Aucune donnée {source_type} disponible")
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
            logger.info(f"Données {source_type} lues avec succès: {row_count} lignes, {column_count} colonnes")
            
            # Ajout de la source comme métadonnée si elle n'existe pas déjà
            if 'source_type' not in df.columns:
                df = df.withColumn("source_type", lit(source_type))
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données {source_type}: {e}")
            traceback.print_exc()
            return None
    
    def convert_data_types(self, df):
        """
        Convertit les types de données appropriés pour standardisation.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            
        Returns:
            DataFrame: DataFrame avec types convertis
        """
        if df is None:
            return None
            
        logger.info("Conversion des types de données...")
        
        try:
            # Conversion des types pour les colonnes immobilières standard
            # Les conversions dépendent des colonnes présentes dans le DataFrame
            columns = df.columns
            
            # Colonnes numériques à convertir en Double
            numeric_cols = [
                'price', 'surface', 'rooms', 'bedrooms', 'bathrooms', 
                'price_per_sqm', 'longitude', 'latitude'
            ]
            
            # Colonnes booléennes ou catégorielles
            boolean_cols = [
                'has_garden', 'has_pool', 'has_garage', 'has_terrace', 'has_parking'
            ]
            
            # Colonnes de date
            date_cols = [
                'listing_date', 'transaction_date'
            ]
            
            # Appliquer les conversions si les colonnes existent
            for col_name in numeric_cols:
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      col(col_name).cast(DoubleType()))
            
            for col_name in boolean_cols:
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      col(col_name).cast(BooleanType()))
            
            for col_name in date_cols:
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      col(col_name).cast(DateType()))
            
            logger.info("Conversion des types de données terminée")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la conversion des types: {e}")
            return df
    
    def standardize_formats(self, df):
        """
        Standardise les formats des colonnes textuelles.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            
        Returns:
            DataFrame: DataFrame avec formats standardisés
        """
        if df is None:
            return None
            
        logger.info("Standardisation des formats...")
        
        try:
            columns = df.columns
            
            # Standardisation des textes (adresses, descriptions, etc.)
            text_cols = [
                'address', 'city', 'postal_code', 'description', 'property_type'
            ]
            
            for col_name in text_cols:
                if col_name in columns:
                    # Nettoyage des chaînes de caractères
                    df = df.withColumn(col_name, 
                                     trim(regexp_replace(col(col_name), "\\s+", " ")))
            
            # Standardisation des codes postaux (retirer les espaces, garder 5 chiffres)
            if 'postal_code' in columns:
                df = df.withColumn('postal_code', 
                                  regexp_replace(col('postal_code'), "\\s+", ""))
            
            # Standardisation des types de propriété (mettre en minuscules)
            if 'property_type' in columns:
                df = df.withColumn('property_type', 
                                  lower(col('property_type')))
            
            logger.info("Standardisation des formats terminée")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la standardisation des formats: {e}")
            return df
    
    def handle_missing_values(self, df):
        """
        Remplace les valeurs nulles par des valeurs par défaut appropriées.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            
        Returns:
            DataFrame: DataFrame avec valeurs nulles remplacées
        """
        if df is None:
            return None
            
        logger.info("Traitement des valeurs manquantes...")
        
        try:
            columns = df.columns
            
            # Remplacement des valeurs nulles pour les colonnes numériques
            numeric_defaults = {
                'price': 0.0,
                'surface': 0.0,
                'rooms': 0,
                'bedrooms': 0,
                'bathrooms': 0
            }
            
            # Remplacement des valeurs nulles pour les colonnes booléennes
            boolean_defaults = {
                'has_garden': False,
                'has_pool': False,
                'has_garage': False,
                'has_terrace': False,
                'has_parking': False
            }
            
            # Remplacement des valeurs nulles pour les colonnes textuelles
            text_defaults = {
                'address': 'Adresse inconnue',
                'city': 'Ville inconnue',
                'postal_code': '00000',
                'description': 'Pas de description',
                'property_type': 'inconnu'
            }
            
            # Appliquer les remplacements si les colonnes existent
            for col_name, default_value in numeric_defaults.items():
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      when(isnull(col(col_name)), default_value)
                                      .otherwise(col(col_name)))
            
            for col_name, default_value in boolean_defaults.items():
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      when(isnull(col(col_name)), default_value)
                                      .otherwise(col(col_name)))
            
            for col_name, default_value in text_defaults.items():
                if col_name in columns:
                    df = df.withColumn(col_name, 
                                      when(isnull(col(col_name)) | (col(col_name) == ''), default_value)
                                      .otherwise(col(col_name)))
            
            logger.info("Traitement des valeurs manquantes terminé")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des valeurs manquantes: {e}")
            return df
    
    def detect_anomalies(self, df):
        """
        Détecte et marque les anomalies dans les données.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            
        Returns:
            DataFrame: DataFrame avec anomalies marquées
        """
        if df is None:
            return None
            
        logger.info("Détection des anomalies...")
        
        try:
            # Création d'un DataFrame temporaire pour les statistiques
            df.createOrReplaceTempView("properties_data")
            
            # Récupération des statistiques pour les colonnes numériques
            numeric_cols = ['price', 'surface', 'rooms', 'bedrooms', 'bathrooms']
            stats = {}
            
            for col_name in numeric_cols:
                if col_name in df.columns:
                    stats_df = self.spark.sql(f"""
                        SELECT 
                            mean({col_name}) as mean,
                            stddev({col_name}) as stddev,
                            min({col_name}) as min,
                            max({col_name}) as max
                        FROM properties_data
                        WHERE {col_name} IS NOT NULL
                    """).collect()
                    
                    if stats_df and stats_df[0]['stddev'] is not None:
                        stats[col_name] = {
                            'mean': stats_df[0]['mean'],
                            'stddev': stats_df[0]['stddev'],
                            'min': stats_df[0]['min'],
                            'max': stats_df[0]['max']
                        }
            
            # Détection des anomalies basée sur l'écart type (valeurs à +/- 3 écarts types)
            for col_name, col_stats in stats.items():
                if col_stats['stddev'] > 0:  # Éviter division par zéro
                    lower_bound = col_stats['mean'] - 3 * col_stats['stddev']
                    upper_bound = col_stats['mean'] + 3 * col_stats['stddev']
                    
                    # Marquer les anomalies
                    df = df.withColumn(
                        f"{col_name}_anomaly",
                        when(
                            (col(col_name) < lower_bound) | (col(col_name) > upper_bound),
                            True
                        ).otherwise(False)
                    )
            
            # Création d'un indicateur global d'anomalie
            anomaly_cols = [c for c in df.columns if c.endswith('_anomaly')]
            if anomaly_cols:
                # Utilisé pour la logique OR: au moins une anomalie
                first_col = anomaly_cols[0]
                anomaly_expr = col(first_col)
                
                for anomaly_col in anomaly_cols[1:]:
                    anomaly_expr = anomaly_expr | col(anomaly_col)
                
                df = df.withColumn("has_anomaly", anomaly_expr)
            
            logger.info("Détection des anomalies terminée")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la détection des anomalies: {e}")
            traceback.print_exc()
            return df
    
    def calculate_quality_score(self, df):
        """
        Calcule un score de qualité des données.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            
        Returns:
            DataFrame: DataFrame avec score de qualité ajouté
        """
        if df is None:
            return None
            
        logger.info("Calcul du score de qualité...")
        
        try:
            # Liste des colonnes importantes pour la qualité
            key_columns = [
                'id', 'price', 'surface', 'address', 'city', 'postal_code',
                'property_type', 'rooms'
            ]
            
            # Filtrer pour ne garder que les colonnes qui existent
            existing_cols = [c for c in key_columns if c in df.columns]
            
            if not existing_cols:
                # Pas de colonnes pour calculer le score
                df = df.withColumn("data_quality_score", lit(0.0))
                return df
            
            # Créer un DataFrame temporaire pour les calculs
            df.createOrReplaceTempView("quality_data")
            
            # Calcul du score basé sur le pourcentage de colonnes non nulles
            null_checks = ", ".join([f"(CASE WHEN {col} IS NOT NULL AND {col} != '' THEN 1 ELSE 0 END)" 
                                    for col in existing_cols])
            
            quality_df = self.spark.sql(f"""
                SELECT 
                    *, 
                    ({null_checks}) / {len(existing_cols)} * 100 as quality_score
                FROM quality_data
            """)
            
            # Ajustement si des anomalies ont été détectées
            if "has_anomaly" in quality_df.columns:
                quality_df = quality_df.withColumn(
                    "data_quality_score",
                    when(col("has_anomaly"), col("quality_score") * 0.7)
                    .otherwise(col("quality_score"))
                )
            else:
                quality_df = quality_df.withColumn("data_quality_score", col("quality_score"))
            
            # Supprimer la colonne temporaire
            quality_df = quality_df.drop("quality_score")
            
            logger.info("Calcul du score de qualité terminé")
            return quality_df
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul du score de qualité: {e}")
            traceback.print_exc()
            # En cas d'erreur, ajouter un score par défaut
            return df.withColumn("data_quality_score", lit(0.0))
    
    def add_metadata(self, df, source_system):
        """
        Ajoute des métadonnées aux données traitées.
        
        Args:
            df (DataFrame): DataFrame Spark à traiter
            source_system (str): Type de source (batch ou streaming)
            
        Returns:
            DataFrame: DataFrame avec métadonnées ajoutées
        """
        if df is None:
            return None
            
        logger.info("Ajout des métadonnées...")
        
        try:
            # Ajout de l'horodatage de traitement
            df = df.withColumn("processing_timestamp", current_timestamp())
            
            # Ajout de la source si non existante
            if "source_system" not in df.columns:
                df = df.withColumn("source_system", lit(source_system))
            
            logger.info("Ajout des métadonnées terminé")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout des métadonnées: {e}")
            return df
    
    def process_data(self, source_type='batch'):
        """
        Traite les données d'une source spécifique.
        
        Args:
            source_type (str): Type de source - 'batch' ou 'streaming'
            
        Returns:
            bool: True si le traitement a réussi, False sinon
        """
        try:
            logger.info(f"Traitement des données {source_type}...")
            
            # 1. Lecture des données depuis la couche RAW
            raw_df = self.read_raw_data(source_type)
            
            if raw_df is None:
                logger.warning(f"Aucune donnée {source_type} à traiter")
                return True  # Pas d'erreur, simplement rien à traiter
            
            # 2. Application des transformations
            # 2.1 Conversion des types de données
            df = self.convert_data_types(raw_df)
            
            # 2.2 Standardisation des formats
            df = self.standardize_formats(df)
            
            # 2.3 Traitement des valeurs manquantes
            df = self.handle_missing_values(df)
            
            # 2.4 Détection des anomalies
            df = self.detect_anomalies(df)
            
            # 2.5 Calcul du score de qualité
            df = self.calculate_quality_score(df)
            
            # 2.6 Ajout des métadonnées
            df = self.add_metadata(df, source_type)
            
            # 3. Définition du chemin de sortie avec partitionnement
            output_path = f"{self.bronze_output_path}/year={self.year}/month={self.month:02d}/day={self.day:02d}"
            
            # Création du nom de fichier
            file_name = f"proprietes_bronze_{source_type}_{self.year}{self.month:02d}{self.day:02d}"
            
            # S'assurer que le répertoire existe
            os.makedirs(output_path, exist_ok=True)
            
            # 4. Écriture au format Parquet avec compression Snappy
            try:
                logger.info(f"Écriture des données au format Parquet: {output_path}/{file_name}.parquet")
                
                # Écriture avec 3 partitions comme requis
                df.repartition(3).write.mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(f"{output_path}/{file_name}.parquet")
                
                logger.info("Écriture Parquet réussie")
                
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture Parquet: {e}")
                return False
            
            # Récupération de statistiques pour le log
            row_count = df.count()
            column_count = len(df.columns)
            
            logger.info(f"Données {source_type} traitées avec succès: {row_count} lignes, {column_count} colonnes")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données {source_type}: {e}")
            traceback.print_exc()
            return False
    
    def run(self):
        """
        Exécute le processeur complet pour les deux sources de données.
        
        Returns:
            dict: Résultat d'exécution avec statuts
        """
        logger.info("=== Démarrage du processeur de la couche BRONZE ===")
        
        try:
            # Initialisation de Spark
            self.initialize_spark()
            
            # Vérification/création des répertoires de destination
            self.ensure_output_directories()
            
            # Traitement des données batch
            batch_success = self.process_data('batch')
            
            # Traitement des données streaming
            streaming_success = self.process_data('streaming')
            
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
            
            logger.info(f"=== Fin du processeur de la couche BRONZE: {result['overall_status']} ===")
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
        processor = BronzeLayerProcessor()
        
        # Exécution du processeur
        result = processor.run()
        
        # Affichage du résultat
        if result.get("overall_status") == "success":
            logger.info("Traitement de la couche BRONZE terminé avec succès")
            return 0
        else:
            logger.error(f"Traitement de la couche BRONZE terminé avec des erreurs: {result}")
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
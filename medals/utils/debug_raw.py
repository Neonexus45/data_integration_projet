#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de débogage pour le processeur de la couche RAW - Phase 2.1
------------------------------------------------------------------

Ce script teste les différentes composantes du processeur pour identifier
et résoudre les problèmes rencontrés avec Spark et MySQL.
"""

import os
import sys
import logging
import traceback
from datetime import datetime
import importlib.util
import subprocess

# Ajout du répertoire parent au path pour l'import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Configuration du logging plus détaillé
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_pyspark_installation():
    """Vérifie l'installation de PySpark et ses dépendances"""
    logger.info("=== Vérification de l'installation PySpark ===")
    
    try:
        # Vérifier si PySpark est installé
        import pyspark
        logger.info(f"PySpark est installé (version: {pyspark.__version__})")
        
        # Vérifier la version de Java
        try:
            result = subprocess.run(['java', '-version'], capture_output=True, text=True)
            if result.returncode == 0:
                # Java est généralement affiché sur stderr
                java_version = result.stderr.split('\n')[0]
                logger.info(f"Java est installé: {java_version}")
            else:
                logger.error("Java n'est pas correctement installé ou accessible")
                return False
        except Exception as e:
            logger.error(f"Erreur lors de la vérification de Java: {e}")
            return False
        
        # Vérifier si les packages JDBC sont disponibles
        jdbc_packages = [
            "mysql-connector-java",
            "mariadb-java-client"
        ]
        
        # Vérification des chemins des JARs
        java_class_path = os.environ.get('CLASSPATH', '')
        logger.info(f"CLASSPATH: {java_class_path}")
        
        # Vérifier le répertoire jars de Spark
        spark_home = pyspark._find_spark_home()
        logger.info(f"SPARK_HOME: {spark_home}")
        spark_jars = os.path.join(spark_home, "jars")
        logger.info(f"Répertoire des JARs Spark: {spark_jars}")
        
        # Liste des fichiers JAR dans le répertoire
        if os.path.exists(spark_jars):
            jdbc_jars = [f for f in os.listdir(spark_jars) if any(pkg in f.lower() for pkg in jdbc_packages)]
            if jdbc_jars:
                logger.info(f"JARs JDBC trouvés: {jdbc_jars}")
            else:
                logger.warning("Aucun JAR JDBC trouvé dans le répertoire Spark jars")
        
        return True
        
    except ImportError:
        logger.error("PySpark n'est pas installé")
        return False
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de PySpark: {e}")
        return False

def check_mysql_connection():
    """Vérifie la connexion à MySQL"""
    logger.info("=== Vérification de la connexion MySQL ===")
    
    try:
        import mysql.connector
        
        # Configuration MySQL
        config = {
            'host': 'localhost',
            'user': 'tatane',
            'password': 'tatane',
            'database': 'immobilier_db'
        }
        
        # Test de connexion basique
        logger.info(f"Tentative de connexion à MySQL: {config['host']}")
        conn = mysql.connector.connect(**config)
        
        if conn.is_connected():
            logger.info("Connexion MySQL établie avec succès")
            
            # Vérifier la base de données
            cursor = conn.cursor()
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            logger.info(f"Base de données active: {db_name}")
            
            # Vérifier les tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            logger.info(f"Tables disponibles: {[table[0] for table in tables]}")
            
            # Vérifier les données dans les tables
            for table_name in ['proprietes_raw', 'proprietes_raw_streaming']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    logger.info(f"Table {table_name}: {count} lignes")
                    
                    # Récupérer un échantillon
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                    sample = cursor.fetchone()
                    if sample:
                        logger.info(f"Exemple de ligne dans {table_name}: disponible")
                    else:
                        logger.warning(f"Table {table_name} est vide")
                        
                except Exception as e:
                    logger.error(f"Erreur lors de la vérification de la table {table_name}: {e}")
            
            cursor.close()
            conn.close()
            return True
            
        else:
            logger.error("Échec de la connexion MySQL")
            return False
            
    except ImportError:
        logger.error("Module mysql-connector-python n'est pas installé")
        return False
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à MySQL: {e}")
        return False

def test_spark_mysql_connection():
    """Teste la connexion Spark à MySQL avec une configuration simplifiée"""
    logger.info("=== Test de la connexion Spark à MySQL ===")
    
    try:
        from pyspark.sql import SparkSession
        
        # Création d'une session Spark simplifiée
        logger.info("Création d'une session Spark simplifiée...")
        spark = (SparkSession.builder
            .appName("SparkMySQLTest")
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28")
            .config("spark.driver.extraClassPath", "./mysql-connector-j-8.0.33.jar")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate())
        
        spark.sparkContext.setLogLevel("INFO")
        logger.info(f"Session Spark créée avec succès. Version: {spark.version}")
        
        # Configuration MySQL
        jdbc_url = "jdbc:mysql://localhost/immobilier_db"
        connection_properties = {
            "user": "tatane",
            "password": "tatane",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        # Test de lecture basique
        try:
            logger.info("Tentative de lecture depuis MySQL...")
            df = spark.read.jdbc(url=jdbc_url, table="proprietes_raw", properties=connection_properties)
            count = df.count()
            logger.info(f"Lecture réussie. Nombre de lignes: {count}")
            
            # Afficher la structure
            logger.info("Structure des données:")
            df.printSchema()
            
            # Afficher quelques lignes
            if count > 0:
                logger.info("Exemple de données:")
                df.show(5, truncate=False)
            
            spark.stop()
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture MySQL depuis Spark: {e}")
            traceback.print_exc()
            spark.stop()
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors du test Spark-MySQL: {e}")
        traceback.print_exc()
        return False

def test_simplified_parquet_write():
    """Teste l'écriture d'un DataFrame simple au format Parquet"""
    logger.info("=== Test d'écriture Parquet simplifié ===")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        # Création d'une session Spark simplifiée
        spark = (SparkSession.builder
            .appName("SparkParquetTest")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())
        
        # Création d'un DataFrame simple
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])
        
        data = [(1, "test1"), (2, "test2")]
        df = spark.createDataFrame(data, schema)
        
        # Chemin de test
        test_path = "data/test_parquet"
        os.makedirs(test_path, exist_ok=True)
        
        # Écriture au format Parquet
        logger.info(f"Écriture d'un DataFrame de test au format Parquet: {test_path}")
        df.write.mode("overwrite").parquet(test_path)
        
        # Vérification
        logger.info("Vérification du fichier Parquet généré")
        if os.path.exists(test_path) and any(f.endswith(".parquet") for f in os.listdir(test_path)):
            logger.info("Test d'écriture Parquet réussi")
            
            # Lecture pour confirmation
            read_df = spark.read.parquet(test_path)
            read_df.show()
            
            spark.stop()
            return True
        else:
            logger.error("Aucun fichier Parquet n'a été généré")
            spark.stop()
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors du test d'écriture Parquet: {e}")
        traceback.print_exc()
        return False

def test_raw_processor():
    """Teste le processeur avec des options de débogage activées"""
    logger.info("=== Test du processeur de couche RAW en mode débogage ===")
    
    try:
        # Importer le processeur
        from medals.raw.processor import RawLayerProcessor
        
        # Configuration MySQL
        mysql_config = {
            'host': 'localhost', 
            'user': 'tatane',
            'password': 'tatane',
            'database': 'immobilier_db'
        }
        
        # Créer une instance avec des chemins de test
        processor = RawLayerProcessor(
            batch_output_path='data/test_raw/batch',
            streaming_output_path='data/test_raw/streaming',
            mysql_config=mysql_config
        )
        
        # Exécution individuelle des étapes pour isoler les problèmes
        logger.info("Initialisation de Spark...")
        processor.initialize_spark()
        
        logger.info("Vérification des répertoires...")
        processor.ensure_output_directories()
        
        logger.info("Test du traitement des données statiques...")
        batch_result = processor.process_batch_data()
        
        logger.info("Test du traitement des données streaming...")
        streaming_result = processor.process_streaming_data()
        
        # Arrêt de Spark
        if processor.spark:
            processor.spark.stop()
        
        # Résultats
        logger.info(f"Résultats des tests: Batch: {batch_result}, Streaming: {streaming_result}")
        return batch_result or streaming_result  # Succès si au moins un a fonctionné
        
    except Exception as e:
        logger.error(f"Erreur lors du test du processeur: {e}")
        traceback.print_exc()
        return False

def main():
    """
    Fonction principale qui exécute les tests de diagnostic.
    """
    logger.info("=== DÉMARRAGE DU DIAGNOSTIC DU PROCESSEUR DE LA COUCHE RAW ===")
    
    results = {
        "pyspark_check": check_pyspark_installation(),
        "mysql_check": check_mysql_connection(),
        "spark_mysql_test": test_spark_mysql_connection(),
        "parquet_write_test": test_simplified_parquet_write(),
        "processor_test": test_raw_processor()
    }
    
    # Affichage du résumé
    logger.info("\n=== RÉSUMÉ DES DIAGNOSTICS ===")
    for test, result in results.items():
        status = "✅ SUCCÈS" if result else "❌ ÉCHEC"
        logger.info(f"{test}: {status}")
    
    # Suggestions basées sur les résultats
    logger.info("\n=== SUGGESTIONS D'AMÉLIORATION ===")
    
    if not results["pyspark_check"]:
        logger.info("1. Vérifiez l'installation de PySpark:")
        logger.info("   - Installez le connecteur MySQL: pip install mysql-connector-python")
        logger.info("   - Assurez-vous que Java est correctement installé et accessible")
        logger.info("   - Définissez JAVA_HOME dans les variables d'environnement")
    
    if not results["mysql_check"]:
        logger.info("2. Vérifiez la configuration MySQL:")
        logger.info("   - Vérifiez que le serveur MySQL est en cours d'exécution")
        logger.info("   - Confirmez les identifiants: tatane/tatane")
        logger.info("   - Vérifiez que la base de données 'immobilier_db' existe")
    
    if not results["spark_mysql_test"]:
        logger.info("3. Problèmes de connexion Spark-MySQL:")
        logger.info("   - Téléchargez et placez mysql-connector-java dans le répertoire jars de Spark")
        logger.info("   - Utilisez l'option useSSL=false dans l'URL JDBC")
        logger.info("   - Réduisez la mémoire allouée à Spark avec spark.driver.memory et spark.executor.memory")
    
    if not results["parquet_write_test"]:
        logger.info("4. Problèmes d'écriture Parquet:")
        logger.info("   - Réduisez le nombre de partitions avec coalesce(1)")
        logger.info("   - Créez explicitement les répertoires de sortie")
        logger.info("   - Utilisez un fallback CSV en cas d'échec")
    
    return 0 if all(results.values()) else 1

if __name__ == "__main__":
    sys.exit(main())
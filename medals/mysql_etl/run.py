#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal d'exécution de l'ETL Gold vers MySQL.
Ce script lance le processus de chargement des données pour tous les datamarts.
"""

import os
import sys
import argparse
import logging
import traceback
from datetime import datetime

# Import des classes ETL pour chaque datamart
from etl_base import BaseETL, logger
from datamart1_price_trends import PriceTrendsETL
from datamart2_feature_impact import FeatureImpactETL
from datamart3_predictions import PredictionModelsETL

# Configuration du logging pour ce script
log_dir = "medals/mysql_etl"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "run_etl.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    
    Returns:
        argparse.Namespace: Arguments parsés
    """
    parser = argparse.ArgumentParser(description="ETL Gold vers MySQL pour les datamarts immobiliers")
    
    # Paramètres MySQL
    parser.add_argument("--host", type=str, default="localhost",
                        help="Hôte MySQL (défaut: localhost)")
    parser.add_argument("--user", type=str, default="tatane",
                        help="Utilisateur MySQL (défaut: tatane)")
    parser.add_argument("--password", type=str, default="tatane",
                        help="Mot de passe MySQL (défaut: tatane)")
    parser.add_argument("--database", type=str, default="immobilier_prediction_db",
                        help="Base de données MySQL (défaut: immobilier_prediction_db)")
    
    # Paramètres de données
    parser.add_argument("--gold-path", type=str, default="data/gold",
                        help="Chemin vers les données Gold (défaut: data/gold)")
    
    return parser.parse_args()

def run_etl_process(args):
    """
    Exécute le processus ETL Gold vers MySQL pour tous les datamarts.
    
    Args:
        args (argparse.Namespace): Arguments parsés
        
    Returns:
        dict: Résultat du processus ETL
    """
    start_time = datetime.now()
    logger.info(f"Démarrage de l'ETL Gold vers MySQL à {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Paramètres communs pour tous les ETL
    etl_params = {
        "gold_path": args.gold_path,
        "host": args.host,
        "user": args.user,
        "password": args.password,
        "database": args.database
    }
    
    # Pour stocker les résultats globaux
    all_stats = {}
    global_status = "success"
    
    # Initialiser une session Spark partagée
    base_etl = BaseETL(**etl_params)
    spark = base_etl.initialize_spark()
    
    # Vérifier la connexion MySQL
    if not base_etl.test_mysql_connection():
        logger.error("Échec du test de connexion MySQL, arrêt de l'ETL")
        return {
            "status": "error",
            "message": "Échec du test de connexion MySQL"
        }
    
    # Datamart 1: Prix par Région
    try:
        logger.info("Chargement du Datamart 1: Analyse des Tendances de Prix par Région...")
        etl1 = PriceTrendsETL(**etl_params)
        etl1.spark = spark  # Réutiliser la session Spark existante
        
        # Charger les données et exécuter l'ETL
        etl1.load_data()
        datamart1_stats = etl1.load_datamart()
        
        all_stats["datamart1"] = datamart1_stats
        logger.info(f"Datamart 1 chargé avec succès: {datamart1_stats}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement du Datamart 1: {e}")
        global_status = "partial"
        all_stats["datamart1"] = {"error": str(e)}
        traceback.print_exc()
    
    # Datamart 2: Facteurs d'Impact
    try:
        logger.info("Chargement du Datamart 2: Évaluation des Facteurs d'Impact sur les Prix...")
        etl2 = FeatureImpactETL(**etl_params)
        etl2.spark = spark  # Réutiliser la session Spark existante
        
        # Charger les données et exécuter l'ETL
        etl2.load_data()
        datamart2_stats = etl2.load_datamart()
        
        all_stats["datamart2"] = datamart2_stats
        logger.info(f"Datamart 2 chargé avec succès: {datamart2_stats}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement du Datamart 2: {e}")
        global_status = "partial"
        all_stats["datamart2"] = {"error": str(e)}
        traceback.print_exc()
    
    # Datamart 3: Prédictions
    try:
        logger.info("Chargement du Datamart 3: Prédictions de Prix Personnalisées...")
        etl3 = PredictionModelsETL(**etl_params)
        etl3.spark = spark  # Réutiliser la session Spark existante
        
        # Charger les données et exécuter l'ETL
        etl3.load_data()
        datamart3_stats = etl3.load_datamart()
        
        all_stats["datamart3"] = datamart3_stats
        logger.info(f"Datamart 3 chargé avec succès: {datamart3_stats}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement du Datamart 3: {e}")
        global_status = "partial"
        all_stats["datamart3"] = {"error": str(e)}
        traceback.print_exc()
    
    # Arrêter la session Spark
    if spark:
        logger.info("Arrêt de la session Spark")
        spark.stop()
    
    # Calculer la durée d'exécution
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Construire le résultat final
    result = {
        "status": global_status,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration.total_seconds(),
        "stats": all_stats
    }
    
    logger.info(f"ETL Gold vers MySQL terminé en {duration}")
    return result

def main():
    """
    Fonction principale du script.
    """
    logger.info("Démarrage du script d'exécution ETL Gold vers MySQL")
    
    # Analyser les arguments
    args = parse_arguments()
    
    # Afficher les paramètres d'exécution
    logger.info("Paramètres d'exécution:")
    logger.info(f"  Gold Path: {args.gold_path}")
    logger.info(f"  MySQL Host: {args.host}")
    logger.info(f"  MySQL User: {args.user}")
    logger.info(f"  MySQL Database: {args.database}")
    logger.info(f"  Exécution pour tous les datamarts")
    
    try:
        # Exécuter le processus ETL
        result = run_etl_process(args)
        
        # Vérifier le statut
        if result["status"] == "error":
            logger.error(f"Échec de l'ETL Gold vers MySQL: {result.get('message', 'Erreur inconnue')}")
            return 1
        
        # Afficher les statistiques
        logger.info("ETL Gold vers MySQL exécuté avec succès")
        logger.info("Statistiques d'exécution:")
        for datamart, stats in result["stats"].items():
            logger.info(f"  {datamart}: {stats}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Erreur fatale dans le script d'exécution: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'exécution du processeur de la couche RAW - Phase 2.1
-------------------------------------------------------------

Ce script permet d'exécuter le processeur de la couche RAW
avec gestion des erreurs et affichage détaillé des résultats.
"""

import os
import sys
import logging
import traceback
import sys
import os

# Ajout du répertoire parent au path pour l'import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from medals.raw.processor import RawLayerProcessor

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("medals/raw/raw_processor.log")
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Fonction principale pour exécuter le processeur corrigé de la couche RAW
    avec affichage détaillé des résultats.
    """
    logger.info("=== DÉMARRAGE DU PROCESSEUR DE LA COUCHE RAW ===")
    
    # Créer le répertoire temporaire pour Spark si nécessaire
    os.makedirs("./spark-temp", exist_ok=True)
    
    try:
        # Initialisation du processeur avec les configurations MySQL
        mysql_config = {
            'host': 'localhost',
            'user': 'tatane',
            'password': 'tatane',
            'database': 'immobilier_db'
        }
        
        logger.info("Initialisation du processeur de la couche RAW...")
        processor = RawLayerProcessor(
            batch_output_path='data/raw/batch',
            streaming_output_path='data/raw/streaming',
            mysql_config=mysql_config
        )
        
        # Exécution du processeur
        logger.info("Démarrage du traitement...")
        result = processor.run()
        
        # Affichage des résultats
        logger.info("=== RÉSULTAT D'EXÉCUTION ===")
        logger.info(f"Horodatage: {result.get('timestamp')}")
        logger.info(f"Traitement des données statiques: {result.get('batch_processing', 'N/A')}")
        logger.info(f"Traitement des données streaming: {result.get('streaming_processing', 'N/A')}")
        logger.info(f"Statut global: {result.get('overall_status', result.get('status', 'N/A'))}")
        
        # Affichage du message d'erreur si présent
        if 'message' in result:
            logger.error(f"Message d'erreur: {result['message']}")
        
        # Vérification des résultats
        if result.get("overall_status") == "success" or result.get("status") == "success":
            logger.info("=== EXÉCUTION TERMINÉE AVEC SUCCÈS ===")
            
            # Afficher les chemins des fichiers générés
            if result.get("batch_processing") == "success":
                year, month, day = processor.year, processor.month, processor.day
                batch_path = f"data/raw/batch/year={year}/month={month:02d}/day={day:02d}/proprietes_raw_{year}{month:02d}{day:02d}.parquet"
                logger.info(f"Fichier Parquet batch généré: {batch_path}")
            
            if result.get("streaming_processing") == "success":
                year, month, day, hour = processor.year, processor.month, processor.day, processor.hour
                stream_path = f"data/raw/streaming/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/proprietes_raw_streaming_{year}{month:02d}{day:02d}{hour:02d}.parquet"
                logger.info(f"Fichier Parquet streaming généré: {stream_path}")
            
            return 0
        else:
            logger.error(f"=== EXÉCUTION TERMINÉE AVEC ERREURS: {result.get('message', 'Raison inconnue')} ===")
            return 1
            
    except Exception as e:
        logger.critical(f"Erreur fatale lors de l'exécution: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
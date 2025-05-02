#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'exécution du processeur de la couche BRONZE - Phase 2.2
-----------------------------------------------------------------

Ce script permet d'exécuter le processeur de la couche BRONZE
avec gestion des erreurs et affichage détaillé des résultats.
"""

import os
import sys
import logging
import traceback
import os

# Ajout du répertoire parent au path pour l'import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from medals.bronze.processor import BronzeLayerProcessor

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("medals/bronze/bronze_processor.log")
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Fonction principale pour exécuter le processeur de la couche BRONZE
    avec affichage détaillé des résultats.
    """
    logger.info("=== DÉMARRAGE DU PROCESSEUR DE LA COUCHE BRONZE ===")
    
    # Créer le répertoire temporaire pour Spark si nécessaire
    os.makedirs("./spark-temp", exist_ok=True)
    
    try:
        logger.info("Initialisation du processeur de la couche BRONZE...")
        processor = BronzeLayerProcessor(
            raw_batch_path='data/raw/batch',
            raw_streaming_path='data/raw/streaming',
            bronze_output_path='data/bronze'
        )
        
        # Exécution du processeur
        logger.info("Démarrage du traitement...")
        result = processor.run()
        
        # Affichage des résultats
        logger.info("=== RÉSULTAT D'EXÉCUTION ===")
        logger.info(f"Horodatage: {result.get('timestamp')}")
        logger.info(f"Traitement des données batch: {result.get('batch_processing', 'N/A')}")
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
                batch_path = f"data/bronze/year={year}/month={month:02d}/day={day:02d}/proprietes_bronze_batch_{year}{month:02d}{day:02d}.parquet"
                logger.info(f"Fichier Parquet batch généré: {batch_path}")
            
            if result.get("streaming_processing") == "success":
                year, month, day = processor.year, processor.month, processor.day
                stream_path = f"data/bronze/year={year}/month={month:02d}/day={day:02d}/proprietes_bronze_streaming_{year}{month:02d}{day:02d}.parquet"
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
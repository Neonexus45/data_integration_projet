#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'exécution du processeur de la couche SILVER - Phase 2.3
-----------------------------------------------------------------

Ce script sert de point d'entrée pour exécuter le processeur de la couche SILVER
qui transforme les données de la couche BRONZE en tables dimensionnelles pour la couche SILVER.
"""

import sys
import logging
import traceback
from datetime import datetime
from processor import SilverLayerProcessor

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("silver_processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """
    Fonction principale pour exécuter le processeur de la couche SILVER.
    """
    start_time = datetime.now()
    logger.info(f"Démarrage du traitement SILVER à {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Création et exécution du processeur
        processor = SilverLayerProcessor()
        result = processor.run()
        
        # Affichage et journalisation du résultat
        if result.get("overall_status") == "success":
            logger.info("Traitement de la couche SILVER terminé avec succès")
            
            # Afficher les statistiques des tables dimensionnelles si disponibles
            if "processing_result" in result and "stats" in result["processing_result"]:
                logger.info("Statistiques des tables dimensionnelles:")
                for table, count in result["processing_result"]["stats"].items():
                    logger.info(f"  - {table}: {count} enregistrements")
            
            exit_code = 0
        else:
            logger.error(f"Traitement de la couche SILVER terminé avec des erreurs: {result}")
            exit_code = 1
        
        # Calculer et afficher la durée d'exécution
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Durée totale d'exécution: {duration}")
        
        return exit_code
        
    except Exception as e:
        logger.critical(f"Erreur fatale lors de l'exécution du processeur SILVER: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    """
    Point d'entrée du script.
    """
    sys.exit(main())
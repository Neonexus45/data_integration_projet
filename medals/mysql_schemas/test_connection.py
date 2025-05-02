#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour vérifier la connexion à MySQL.
"""

import logging
import sys
from datamart_schema_creator import DatamartSchemaCreator

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('TestConnection')

def test_connection():
    """
    Teste la connexion à MySQL et vérifie les paramètres.
    """
    logger.info("Test de connexion à MySQL")
    
    try:
        # Création de l'instance du créateur de schémas
        schema_creator = DatamartSchemaCreator()
        
        # Test de connexion sans base de données
        if schema_creator.connect(use_database=False):
            logger.info("Connexion à MySQL réussie!")
            schema_creator.disconnect()
            return True
        else:
            logger.error("Échec de la connexion à MySQL")
            return False
    except Exception as e:
        logger.error(f"Exception lors du test de connexion: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
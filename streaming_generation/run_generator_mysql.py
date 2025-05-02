#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de lancement du générateur de données immobilières en streaming avec MySQL.
Ce script permet de démarrer facilement la génération de données avec différentes options
et insertion directe dans une table MySQL.
"""

import argparse
import logging
from housing_data_generator_mysql import HousingDataGenerator

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    
    Returns:
        argparse.Namespace: Objet contenant les arguments parsés
    """
    parser = argparse.ArgumentParser(
        description="Générateur de données immobilières en streaming avec insertion MySQL"
    )
    
    parser.add_argument(
        "--source", 
        type=str, 
        default="data/raw/housing_data.csv",
        help="Fichier source de données immobilières (défaut: data/raw/housing_data.csv)"
    )
    
    parser.add_argument(
        "--interval", 
        type=int, 
        default=5,
        help="Intervalle en secondes entre chaque génération (défaut: 5)"
    )
    
    parser.add_argument(
        "--count", 
        type=int, 
        default=None,
        help="Nombre de propriétés à générer. Si non spécifié, s'exécute indéfiniment."
    )
    
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Hôte MySQL (défaut: localhost)"
    )
    
    parser.add_argument(
        "--user",
        type=str,
        default="tatane",
        help="Utilisateur MySQL (défaut: tatane)"
    )
    
    parser.add_argument(
        "--password",
        type=str,
        default="tatane",
        help="Mot de passe MySQL (défaut: tatane)"
    )
    
    parser.add_argument(
        "--dbname",
        type=str,
        default="immobilier_db",
        help="Nom de la base de données (défaut: immobilier_db)"
    )
    
    parser.add_argument(
        "--table",
        type=str,
        default="proprietes_raw_streaming",
        help="Nom de la table pour les données streaming (défaut: proprietes_raw_streaming)"
    )
    
    return parser.parse_args()


def main():
    """
    Fonction principale qui initialise et exécute le générateur
    """
    # Récupérer les arguments
    args = parse_arguments()
    
    # Mise à jour des variables globales dans le module
    import housing_data_generator_mysql as hdgm
    hdgm.DB_CONFIG = {
        'host': args.host,
        'user': args.user,
        'password': args.password
    }
    hdgm.DB_NAME = args.dbname
    hdgm.TABLE_NAME = args.table
    
    logger.info(f"Configuration:")
    logger.info(f"  - Source: {args.source}")
    logger.info(f"  - Intervalle: {args.interval} secondes")
    logger.info(f"  - Nombre: {'Illimité' if args.count is None else args.count}")
    logger.info(f"  - Base de données: {args.dbname}")
    logger.info(f"  - Table: {args.table}")
    logger.info("")
    
    try:
        # Créer et démarrer le générateur avec les paramètres spécifiés
        generator = HousingDataGenerator(
            source_file_path=args.source,
            publish_interval=args.interval
        )
        
        logger.info("Démarrage de la génération de données avec insertion MySQL...")
        generator.run(num_properties=args.count)
        
    except FileNotFoundError as e:
        logger.error(f"Erreur: Fichier source introuvable - {e}")
        
    except KeyboardInterrupt:
        logger.info("\nGénération interrompue par l'utilisateur.")
        
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        
    finally:
        logger.info("Fin du programme.")


if __name__ == "__main__":
    main()
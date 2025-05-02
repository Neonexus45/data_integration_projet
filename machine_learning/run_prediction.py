#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'exécution du prédicteur de prix immobiliers.
Ce script permet de lancer le pipeline complet de prédiction de prix.

Auteur: IA Roo
Date: 02/05/2025
"""

import os
import sys
import logging
import argparse
from housing_price_predictor import HousingPricePredictor, logger

def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    
    Returns:
        argparse.Namespace: Arguments parsés
    """
    parser = argparse.ArgumentParser(description='Prédiction de prix immobiliers')
    
    parser.add_argument('--gold-path', type=str, default='data/gold',
                      help='Chemin vers les données de la couche Gold')
    
    parser.add_argument('--output-dir', type=str, default='machine_learning/models',
                      help='Répertoire de sortie pour les modèles et visualisations')
    
    parser.add_argument('--host', type=str, default='localhost',
                      help='Hôte du serveur MySQL')
    
    parser.add_argument('--user', type=str, default='tatane',
                      help='Utilisateur MySQL')
    
    parser.add_argument('--password', type=str, default='tatane',
                      help='Mot de passe MySQL')
    
    parser.add_argument('--database', type=str, default='immobilier_prediction_db',
                      help='Nom de la base de données MySQL')
    
    parser.add_argument('--test-size', type=float, default=0.2,
                      help='Proportion des données pour le test (entre 0 et 1)')
    
    return parser.parse_args()

def main():
    """
    Fonction principale pour exécuter le prédicteur de prix immobiliers.
    """
    # Analyser les arguments
    args = parse_arguments()
    
    try:
        logger.info("Démarrage du processus de prédiction des prix immobiliers")
        
        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Initialiser le prédicteur avec les arguments
        predictor = HousingPricePredictor(
            gold_path=args.gold_path,
            output_dir=args.output_dir,
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database
        )
        
        # Exécuter le pipeline complet
        results = predictor.run_prediction_pipeline()
        
        if results['success']:
            model_name = results['model_name']
            metrics = results['model_metrics']
            
            logger.info("========== RÉSULTATS DE LA PRÉDICTION ==========")
            logger.info(f"Meilleur modèle: {model_name}")
            logger.info(f"Métriques de performance:")
            logger.info(f"  - RMSE: {metrics.get('rmse', 'N/A'):.2f}")
            logger.info(f"  - MAE: {metrics.get('mae', 'N/A'):.2f}")
            logger.info(f"  - R²: {metrics.get('r2', 'N/A'):.4f}")
            logger.info(f"  - Variance expliquée: {metrics.get('explained_variance', 'N/A'):.4f}")
            logger.info(f"Modèle sauvegardé: {results['model_path']}")
            logger.info("=================================================")
            
            return 0  # Succès
        else:
            logger.error("Le processus de prédiction a échoué")
            return 1  # Échec
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du processus de prédiction: {str(e)}")
        return 1  # Échec

if __name__ == "__main__":
    sys.exit(main())
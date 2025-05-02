#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de lancement du générateur de données immobilières en streaming.
Ce script permet de démarrer facilement la génération de données avec différentes options.
"""

import argparse
from housing_data_generator import HousingDataGenerator


def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    
    Returns:
        argparse.Namespace: Objet contenant les arguments parsés
    """
    parser = argparse.ArgumentParser(
        description="Générateur de données immobilières en streaming"
    )
    
    parser.add_argument(
        "--source", 
        type=str, 
        default="data/raw/housing_data.csv",
        help="Fichier source de données immobilières (défaut: data/raw/housing_data.csv)"
    )
    
    parser.add_argument(
        "--output", 
        type=str, 
        default="data/raw/streaming/",
        help="Répertoire de sortie pour les fichiers générés (défaut: data/raw/streaming/)"
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
    
    return parser.parse_args()


def main():
    """
    Fonction principale qui initialise et exécute le générateur
    """
    # Récupérer les arguments
    args = parse_arguments()
    
    print(f"Configuration:")
    print(f"  - Source: {args.source}")
    print(f"  - Sortie: {args.output}")
    print(f"  - Intervalle: {args.interval} secondes")
    print(f"  - Nombre: {'Illimité' if args.count is None else args.count}")
    print()
    
    try:
        # Créer et démarrer le générateur avec les paramètres spécifiés
        generator = HousingDataGenerator(
            source_file_path=args.source,
            output_dir=args.output,
            publish_interval=args.interval
        )
        
        print("Démarrage de la génération de données...")
        generator.run(num_properties=args.count)
        
    except FileNotFoundError as e:
        print(f"Erreur: Fichier source introuvable - {e}")
        
    except KeyboardInterrupt:
        print("\nGénération interrompue par l'utilisateur.")
        
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        
    finally:
        print("Fin du programme.")


if __name__ == "__main__":
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Générateur de données immobilières en streaming - Phase 1.1
Ce script génère des données immobilières simulées similaires au format du fichier
housing_data.csv et les publie à intervalle régulier (5 secondes par défaut).
"""

import pandas as pd
import numpy as np
import time
import os
import random
from datetime import datetime


class HousingDataGenerator:
    """
    Classe responsable de la génération de données immobilières simulées.
    
    Cette classe charge les données sources depuis housing_data.csv, 
    génère des variations aléatoires réalistes de ces données, 
    et les publie à intervalles réguliers.
    """
    
    def __init__(self, source_file_path='data/raw/housing_data.csv', 
                 output_dir='data/raw/streaming/', 
                 publish_interval=5):
        """
        Initialise le générateur de données immobilières.
        
        Args:
            source_file_path (str): Chemin vers le fichier source de données
            output_dir (str): Répertoire de sortie pour les données générées
            publish_interval (int): Intervalle en secondes entre chaque publication
        """
        self.source_file_path = source_file_path
        self.output_dir = output_dir
        self.publish_interval = publish_interval
        
        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Charger les données sources
        self.source_data = pd.read_csv(self.source_file_path)
        
        # Extraire les informations importantes pour la génération
        self.columns = self.source_data.columns.tolist()
        
        # Ajouter la colonne source
        self.columns.append('source')
        
        # Dictionnaire des valeurs uniques par colonne catégorielle
        self.categorical_values = {}
        self._extract_categorical_values()
        
        # Statistiques numériques pour la génération de valeurs
        self.numeric_stats = {}
        self._calculate_numeric_stats()
        
        print(f"Générateur initialisé avec {len(self.source_data)} propriétés sources")
    
    def _extract_categorical_values(self):
        """
        Extrait les valeurs uniques pour chaque colonne catégorielle.
        Utilisé pour générer des valeurs aléatoires réalistes.
        """
        for column in self.source_data.columns:
            # Si le nombre de valeurs uniques est limité, considérer comme catégorielle
            if self.source_data[column].dtype == 'object' or self.source_data[column].nunique() < 30:
                self.categorical_values[column] = self.source_data[column].dropna().unique().tolist()
    
    def _calculate_numeric_stats(self):
        """
        Calcule les statistiques pour les colonnes numériques.
        Utilisé pour générer des valeurs aléatoires dans des plages réalistes.
        """
        for column in self.source_data.columns:
            if self.source_data[column].dtype in ['int64', 'float64']:
                self.numeric_stats[column] = {
                    'min': self.source_data[column].min(),
                    'max': self.source_data[column].max(),
                    'mean': self.source_data[column].mean(),
                    'std': self.source_data[column].std()
                }
    
    def generate_property(self):
        """
        Génère une propriété immobilière simulée basée sur les données sources.
        
        Returns:
            dict: Dictionnaire représentant une propriété immobilière
        """
        property_data = {}
        
        # Générer un identifiant unique (au-delà des ID existants)
        max_id = self.source_data['Id'].max()
        property_data['Id'] = max_id + random.randint(1, 1000)
        
        # Générer des valeurs pour chaque colonne
        for column in self.source_data.columns:
            if column == 'Id':
                continue  # Déjà traité
                
            # Traitement selon le type de colonne
            if column in self.categorical_values:
                # Colonne catégorielle
                property_data[column] = random.choice(self.categorical_values[column])
            elif column in self.numeric_stats:
                # Colonne numérique - générer avec variation aléatoire
                stats = self.numeric_stats[column]
                # Utiliser distribution normale pour générer des valeurs réalistes
                value = np.random.normal(stats['mean'], stats['std'])
                # Limiter aux valeurs min/max observées avec une marge de 10%
                min_val = stats['min'] * 0.9
                max_val = stats['max'] * 1.1
                value = max(min_val, min(max_val, value))
                
                # Arrondir à l'entier si la colonne source contient des entiers
                if self.source_data[column].dtype == 'int64':
                    value = int(round(value))
                
                property_data[column] = value
            else:
                # Autres types de colonnes (par défaut)
                property_data[column] = None
        
        # Ajouter la colonne source avec valeur "streaming"
        property_data['source'] = "streaming"
        
        return property_data
    
    def publish_property(self, property_data):
        """
        Publie une propriété générée dans un fichier CSV.
        Simule l'envoi vers un système comme Kafka.
        
        Args:
            property_data (dict): Données de la propriété à publier
        
        Returns:
            str: Chemin du fichier où les données ont été écrites
        """
        # Créer un DataFrame à partir des données de propriété
        df = pd.DataFrame([property_data])
        
        # Générer un nom de fichier avec horodatage
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"housing_stream_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Écrire dans un fichier CSV
        df.to_csv(filepath, index=False)
        
        print(f"Propriété publiée: ID={property_data['Id']} - {filepath}")
        return filepath
    
    def run(self, num_properties=None):
        """
        Exécute le générateur pour produire et publier des propriétés.
        
        Args:
            num_properties (int, optional): Nombre de propriétés à générer. 
                Si None, s'exécute indéfiniment.
        """
        generated_count = 0
        
        try:
            while num_properties is None or generated_count < num_properties:
                # Générer une propriété
                property_data = self.generate_property()
                
                # Publier la propriété
                self.publish_property(property_data)
                
                generated_count += 1
                
                # Attendre l'intervalle configuré
                time.sleep(self.publish_interval)
                
        except KeyboardInterrupt:
            print(f"\nGénération interrompue. {generated_count} propriétés générées.")
        
        print(f"Génération terminée. {generated_count} propriétés générées.")


if __name__ == "__main__":
    """
    Point d'entrée principal du script.
    Initialise et exécute le générateur de données immobilières.
    """
    print("Démarrage du générateur de données immobilières en streaming...")
    
    # Créer une instance du générateur
    generator = HousingDataGenerator(
        source_file_path='data/raw/housing_data.csv',
        output_dir='data/raw/streaming/',
        publish_interval=5
    )
    
    # Exécuter le générateur (s'arrête avec Ctrl+C)
    generator.run()
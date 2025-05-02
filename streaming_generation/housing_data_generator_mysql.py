#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Générateur de données immobilières en streaming avec insertion MySQL - Phase 1.1
Ce script génère des données immobilières simulées similaires au format du fichier
housing_data.csv et les insère directement dans une table MySQL.
"""

import pandas as pd
import numpy as np
import time
import os
import random
import logging
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paramètres de connexion à MySQL (identiques à ceux de import_housing_data.py)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'tatane',
    'password': 'tatane'
}

DB_NAME = 'immobilier_db'
TABLE_NAME = 'proprietes_raw_streaming'  # Table spécifique pour les données streaming


class HousingDataGenerator:
    """
    Classe responsable de la génération de données immobilières simulées.
    
    Cette classe charge les données sources depuis housing_data.csv, 
    génère des variations aléatoires réalistes de ces données, 
    et les insère directement dans une table MySQL.
    """
    
    def __init__(self, source_file_path='data/raw/housing_data.csv', 
                 publish_interval=5):
        """
        Initialise le générateur de données immobilières.
        
        Args:
            source_file_path (str): Chemin vers le fichier source de données
            publish_interval (int): Intervalle en secondes entre chaque publication
        """
        self.source_file_path = source_file_path
        self.publish_interval = publish_interval
        
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
        
        # S'assurer que la BD et la table existent
        self._setup_database()
        
        logger.info(f"Générateur initialisé avec {len(self.source_data)} propriétés sources")
    
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
    
    def _setup_database(self):
        """
        Configure la base de données et la table pour les insertions.
        Crée la base de données et la table si elles n'existent pas.
        """
        # Création de la BD si nécessaire
        if not self._create_database_if_not_exists():
            logger.error("Impossible de créer/accéder à la base de données")
            raise Exception("Erreur d'accès à la base de données")
            
        # Obtention d'une connexion
        conn = self._get_connection()
        if conn is None:
            logger.error("Impossible de se connecter à la base de données")
            raise Exception("Erreur de connexion à la base de données")
            
        # Création de la table si nécessaire
        if not self._create_table_if_not_exists(conn):
            conn.close()
            logger.error("Impossible de créer la table dans la base de données")
            raise Exception("Erreur de création de table")
            
        conn.close()
        logger.info(f"Base de données et table {TABLE_NAME} configurées avec succès")
    
    def _create_database_if_not_exists(self):
        """
        Crée la base de données si elle n'existe pas déjà.
        
        Returns:
            bool: True si la création a réussi, False sinon
        """
        try:
            # Connexion sans spécifier la base de données
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
                logger.info(f"Base de données '{DB_NAME}' créée ou déjà existante")
                cursor.close()
                conn.close()
                return True
        except Error as e:
            logger.error(f"Erreur lors de la création de la base de données: {e}")
            return False
    
    def _get_connection(self):
        """
        Établit une connexion à la base de données.
        
        Returns:
            Connection: Objet de connexion MySQL, ou None en cas d'échec
        """
        try:
            config = DB_CONFIG.copy()
            config['database'] = DB_NAME
            conn = mysql.connector.connect(**config)
            
            if conn.is_connected():
                return conn
            else:
                logger.error("Échec de connexion à la base de données")
                return None
        except Error as e:
            logger.error(f"Erreur de connexion à la base de données: {e}")
            return None
    
    def _create_table_if_not_exists(self, conn):
        """
        Crée la table pour les données streaming.
        
        Args:
            conn: Connexion à la base de données MySQL
            
        Returns:
            bool: True si la création a réussi, False sinon
        """
        try:
            cursor = conn.cursor()
            
            # Construction de la requête CREATE TABLE
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            """
            
            # On considère 'Id' comme clé primaire
            create_table_query += "    `Id` INT PRIMARY KEY,\n"
            
            # On définit les types de colonnes en se basant sur les noms et les valeurs
            for column in self.source_data.columns:
                if column == 'Id':
                    continue  # Déjà traité
                    
                # Détermine le type de colonne en fonction de son nom et contenu
                if column in ['MSSubClass', 'OverallQual', 'OverallCond', 'YearBuilt', 'YearRemodAdd', 
                             'BsmtFullBath', 'BsmtHalfBath', 'FullBath', 'HalfBath', 'BedroomAbvGr',
                             'KitchenAbvGr', 'TotRmsAbvGrd', 'Fireplaces', 'GarageCars', 'MoSold', 'YrSold']:
                    col_type = "INT"
                elif column in ['LotFrontage', 'LotArea', 'MasVnrArea', 'BsmtFinSF1', 'BsmtFinSF2', 
                               'BsmtUnfSF', 'TotalBsmtSF', '1stFlrSF', '2ndFlrSF', 'LowQualFinSF', 
                               'GrLivArea', 'GarageYrBlt', 'GarageArea', 'WoodDeckSF', 'OpenPorchSF', 
                               'EnclosedPorch', '3SsnPorch', 'ScreenPorch', 'PoolArea', 'MiscVal']:
                    col_type = "FLOAT"
                else:
                    # Pour les colonnes texte, on utilise VARCHAR
                    # On analyse la longueur maximale observée
                    max_length = self.source_data[column].astype(str).str.len().max()
                    col_type = f"VARCHAR({max(max_length * 2, 100)})"
                
                create_table_query += f"    `{column}` {col_type},\n"
            
            # Ajout de la colonne 'source'
            create_table_query += "    `source` VARCHAR(50),\n"
            # Ajout d'un timestamp pour suivre l'ordre d'insertion
            create_table_query += "    `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
            
            # Finalisation de la requête
            create_table_query += ");"
            
            cursor.execute(create_table_query)
            logger.info(f"Table '{TABLE_NAME}' créée ou déjà existante")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"Erreur lors de la création de la table: {e}")
            return False
        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
            return False
    
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
    
    def _convert_numpy_types(self, value):
        """
        Convertit les types NumPy en types Python standards.
        
        Args:
            value: Valeur à convertir
            
        Returns:
            Valeur convertie en type Python standard
        """
        # Conversion des types entiers NumPy
        if hasattr(np, 'integer') and isinstance(value, np.integer):
            return int(value)
        # Conversion des types flottants NumPy
        elif hasattr(np, 'floating') and isinstance(value, np.floating):
            if np.isnan(value):
                return None
            return float(value)
        # Autres types NumPy potentiels
        elif hasattr(np, 'bool_') and isinstance(value, np.bool_):
            return bool(value)
        else:
            return value
    
    def publish_property(self, property_data):
        """
        Publie une propriété générée dans la table MySQL.
        
        Args:
            property_data (dict): Données de la propriété à publier
        
        Returns:
            bool: True si l'insertion a réussi, False sinon
        """
        try:
            # Obtenir une connexion
            conn = self._get_connection()
            if conn is None:
                return False
                
            cursor = conn.cursor()
            
            # Préparation des colonnes et valeurs pour l'insertion
            columns = property_data.keys()
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Construction de la requête d'insertion
            column_str = ', '.join([f'`{col}`' for col in columns])
            insert_query = f"INSERT INTO {TABLE_NAME} ({column_str}) VALUES ({placeholders})"
            
            # Préparation des valeurs avec conversion des types NumPy en types Python
            values = []
            for col in columns:
                val = property_data[col]
                # Conversion des types NumPy et gestion des NaN
                values.append(self._convert_numpy_types(val))
            
            # Exécution de la requête
            cursor.execute(insert_query, tuple(values))
            conn.commit()
            
            # Récupérer l'ID inséré pour le log
            property_id = property_data['Id']
            
            cursor.close()
            conn.close()
            
            logger.info(f"Propriété insérée dans MySQL: ID={property_id}")
            return True
            
        except Error as e:
            logger.error(f"Erreur d'insertion MySQL: {e}")
            return False
        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'insertion: {e}")
            return False
    
    def run(self, num_properties=None):
        """
        Exécute le générateur pour produire et publier des propriétés.
        
        Args:
            num_properties (int, optional): Nombre de propriétés à générer. 
                Si None, s'exécute indéfiniment.
        """
        generated_count = 0
        success_count = 0
        
        try:
            while num_properties is None or generated_count < num_properties:
                # Générer une propriété
                property_data = self.generate_property()
                
                # Publier la propriété
                if self.publish_property(property_data):
                    success_count += 1
                
                generated_count += 1
                
                # Afficher un résumé périodique
                if generated_count % 10 == 0:
                    logger.info(f"Résumé: {success_count}/{generated_count} propriétés insérées avec succès")
                
                # Attendre l'intervalle configuré
                time.sleep(self.publish_interval)
                
        except KeyboardInterrupt:
            logger.info(f"\nGénération interrompue. {success_count}/{generated_count} propriétés générées.")
        
        logger.info(f"Génération terminée. {success_count}/{generated_count} propriétés insérées avec succès.")


if __name__ == "__main__":
    """
    Point d'entrée principal du script.
    Initialise et exécute le générateur de données immobilières.
    """
    logger.info("Démarrage du générateur de données immobilières en streaming (insertion MySQL)...")
    
    try:
        # Créer une instance du générateur
        generator = HousingDataGenerator(
            source_file_path='data/raw/housing_data.csv',
            publish_interval=5
        )
        
        # Exécuter le générateur (s'arrête avec Ctrl+C)
        generator.run()
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du générateur: {e}")
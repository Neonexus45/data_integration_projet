#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de création des schémas MySQL pour les datamarts d'analyse immobilière.
Phase 3.1 du plan d'implémentation.

Ce script crée la base de données 'immobilier_prediction_db' et établit 
les schémas MySQL pour les trois datamarts décrits dans l'architecture.
"""

import mysql.connector
from mysql.connector import Error
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DatamartSchemaCreator')

class DatamartSchemaCreator:
    """
    Classe pour la création des schémas de datamarts MySQL pour l'analyse immobilière.
    """
    
    def __init__(self, host='localhost', user='tatane', password='tatane', database='immobilier_prediction_db'):
        """
        Initialise le créateur de schémas avec les paramètres de connexion MySQL.
        
        Args:
            host (str): Hôte du serveur MySQL
            user (str): Utilisateur MySQL
            password (str): Mot de passe
            database (str): Nom de la base de données
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def connect(self, use_database=True):
        """
        Établit une connexion à MySQL.
        
        Args:
            use_database (bool): Si True, se connecte à la base de données spécifiée, 
                                sinon, se connecte sans sélectionner de base de données
        
        Returns:
            bool: True si la connexion est réussie, False sinon
        """
        try:
            if use_database:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            else:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
                )
            
            self.cursor = self.conn.cursor()
            logger.info(f"Connexion à MySQL réussie {'avec' if use_database else 'sans'} sélection de base de données")
            return True
        except Error as e:
            logger.error(f"Erreur lors de la connexion à MySQL: {e}")
            return False
    
    def disconnect(self):
        """Ferme la connexion à MySQL."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Déconnexion de MySQL réussie")
    
    def create_database(self):
        """
        Crée la base de données si elle n'existe pas.
        
        Returns:
            bool: True si l'opération est réussie, False sinon
        """
        try:
            # Se connecter sans sélectionner de base de données
            if not self.connect(use_database=False):
                return False
                
            # Vérifier si la base de données existe
            self.cursor.execute(f"SHOW DATABASES LIKE '{self.database}'")
            database_exists = self.cursor.fetchone() is not None
            
            if not database_exists:
                # Créer la base de données avec le jeu de caractères UTF-8
                self.cursor.execute(
                    f"CREATE DATABASE {self.database} "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                logger.info(f"Base de données '{self.database}' créée avec succès")
            else:
                logger.info(f"La base de données '{self.database}' existe déjà")
            
            # Sélectionner la base de données
            self.cursor.execute(f"USE {self.database}")
            
            return True
        except Error as e:
            logger.error(f"Erreur lors de la création de la base de données: {e}")
            return False
        finally:
            self.disconnect()
    
    def table_exists(self, table_name):
        """
        Vérifie si une table existe dans la base de données.
        
        Args:
            table_name (str): Nom de la table à vérifier
            
        Returns:
            bool: True si la table existe, False sinon
        """
        try:
            self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            return self.cursor.fetchone() is not None
        except Error as e:
            logger.error(f"Erreur lors de la vérification de l'existence de la table: {e}")
            return False
    
    def execute_query(self, query):
        """
        Exécute une requête SQL.
        
        Args:
            query (str): Requête SQL à exécuter
            
        Returns:
            bool: True si l'exécution est réussie, False sinon
        """
        try:
            self.cursor.execute(query)
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            logger.error(f"Requête: {query}")
            return False
    
    # Méthodes pour créer les tables du datamart 1
    def create_dm_regional_price_trends(self):
        """
        Crée la table dm_regional_price_trends pour l'analyse des tendances de prix par région.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_regional_price_trends'):
            logger.info("La table 'dm_regional_price_trends' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_regional_price_trends (
            region_id INT NOT NULL,
            region_name VARCHAR(100) NOT NULL,
            period_year INT NOT NULL,
            period_month INT NOT NULL,
            avg_price DECIMAL(12,2) NOT NULL,
            median_price DECIMAL(12,2) NOT NULL,
            min_price DECIMAL(12,2) NOT NULL,
            max_price DECIMAL(12,2) NOT NULL,
            price_momentum DECIMAL(5,2) NOT NULL,
            transaction_count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (region_id, period_year, period_month),
            INDEX idx_region_name (region_name),
            INDEX idx_period (period_year, period_month)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_regional_price_trends' créée avec succès")
            return True
        return False
    
    def create_dm_neighborhood_comparison(self):
        """
        Crée la table dm_neighborhood_comparison pour la comparaison des quartiers.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_neighborhood_comparison'):
            logger.info("La table 'dm_neighborhood_comparison' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_neighborhood_comparison (
            neighborhood_id INT NOT NULL,
            neighborhood_name VARCHAR(100) NOT NULL,
            city_name VARCHAR(100) NOT NULL,
            region_id INT NOT NULL,
            avg_price DECIMAL(12,2) NOT NULL,
            price_per_sqft DECIMAL(8,2) NOT NULL,
            price_change_yoy DECIMAL(5,2) NOT NULL,
            avg_days_on_market INT NOT NULL,
            walkability_score DECIMAL(4,1),
            school_rating DECIMAL(4,1),
            crime_index DECIMAL(4,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (neighborhood_id),
            INDEX idx_neighborhood_name (neighborhood_name),
            INDEX idx_city_name (city_name),
            INDEX idx_region_id (region_id)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_neighborhood_comparison' créée avec succès")
            return True
        return False
    
    def create_dm_seasonal_patterns(self):
        """
        Crée la table dm_seasonal_patterns pour l'analyse des patterns saisonniers de prix.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_seasonal_patterns'):
            logger.info("La table 'dm_seasonal_patterns' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_seasonal_patterns (
            region_id INT NOT NULL,
            month INT NOT NULL,
            price_index DECIMAL(5,2) NOT NULL,
            transaction_volume_index DECIMAL(5,2) NOT NULL,
            days_on_market_index DECIMAL(5,2) NOT NULL,
            year_of_analysis INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (region_id, month, year_of_analysis),
            INDEX idx_month (month),
            INDEX idx_year (year_of_analysis)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_seasonal_patterns' créée avec succès")
            return True
        return False
    
    # Méthodes pour créer les tables du datamart 2
    def create_dm_feature_importance(self):
        """
        Crée la table dm_feature_importance pour l'analyse de l'importance des caractéristiques.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_feature_importance'):
            logger.info("La table 'dm_feature_importance' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_feature_importance (
            feature_id INT NOT NULL AUTO_INCREMENT,
            feature_name VARCHAR(100) NOT NULL,
            feature_category VARCHAR(50) NOT NULL,
            global_importance_score DECIMAL(5,2) NOT NULL,
            correlation_with_price DECIMAL(4,3) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            model_version VARCHAR(20) NOT NULL,
            PRIMARY KEY (feature_id),
            UNIQUE INDEX idx_feature_name (feature_name),
            INDEX idx_importance (global_importance_score DESC)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_feature_importance' créée avec succès")
            return True
        return False
    
    def create_dm_regional_feature_variation(self):
        """
        Crée la table dm_regional_feature_variation pour l'analyse de la variation régionale 
        de l'importance des caractéristiques.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_regional_feature_variation'):
            logger.info("La table 'dm_regional_feature_variation' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_regional_feature_variation (
            feature_id INT NOT NULL,
            region_id INT NOT NULL,
            importance_score DECIMAL(5,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (feature_id, region_id),
            FOREIGN KEY (feature_id) REFERENCES dm_feature_importance(feature_id)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_regional_feature_variation' créée avec succès")
            return True
        return False
    
    def create_dm_temporal_feature_variation(self):
        """
        Crée la table dm_temporal_feature_variation pour l'analyse de la variation temporelle
        de l'importance des caractéristiques.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_temporal_feature_variation'):
            logger.info("La table 'dm_temporal_feature_variation' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_temporal_feature_variation (
            feature_id INT NOT NULL,
            period_year INT NOT NULL,
            period_quarter INT NOT NULL,
            importance_score DECIMAL(5,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (feature_id, period_year, period_quarter),
            FOREIGN KEY (feature_id) REFERENCES dm_feature_importance(feature_id)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_temporal_feature_variation' créée avec succès")
            return True
        return False
    
    # Méthodes pour créer les tables du datamart 3
    def create_dm_prediction_models(self):
        """
        Crée la table dm_prediction_models pour stocker les métadonnées des modèles de prédiction.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_prediction_models'):
            logger.info("La table 'dm_prediction_models' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_prediction_models (
            model_id VARCHAR(36) NOT NULL,
            model_name VARCHAR(100) NOT NULL,
            model_version VARCHAR(20) NOT NULL,
            training_date TIMESTAMP NOT NULL,
            model_params TEXT NOT NULL,
            model_metrics JSON NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (model_id),
            UNIQUE INDEX idx_model_version (model_version)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_prediction_models' créée avec succès")
            return True
        return False
    
    def create_dm_prediction_results(self):
        """
        Crée la table dm_prediction_results pour stocker les résultats des prédictions.
        
        Returns:
            bool: True si la création est réussie, False sinon
        """
        if self.table_exists('dm_prediction_results'):
            logger.info("La table 'dm_prediction_results' existe déjà")
            return True
            
        query = """
        CREATE TABLE dm_prediction_results (
            prediction_id INT NOT NULL AUTO_INCREMENT,
            property_id INT NOT NULL,
            predicted_price DECIMAL(12,2) NOT NULL,
            prediction_interval_low DECIMAL(12,2) NOT NULL,
            prediction_interval_high DECIMAL(12,2) NOT NULL,
            confidence_score DECIMAL(4,3) NOT NULL,
            model_id VARCHAR(36) NOT NULL,
            prediction_timestamp TIMESTAMP NOT NULL,
            feature_contributions JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (prediction_id),
            INDEX idx_property (property_id),
            INDEX idx_timestamp (prediction_timestamp),
            INDEX idx_confidence (confidence_score),
            FOREIGN KEY (model_id) REFERENCES dm_prediction_models(model_id)
        ) ENGINE=InnoDB;
        """
        if self.execute_query(query):
            logger.info("Table 'dm_prediction_results' créée avec succès")
            return True
        return False
    
    def create_all_tables(self):
        """
        Crée toutes les tables des datamarts dans l'ordre approprié.
        
        Returns:
            bool: True si toutes les tables sont créées avec succès, False sinon
        """
        # Se connecter à la base de données
        if not self.connect():
            logger.error("Impossible de se connecter à la base de données")
            return False
        
        try:
            # Datamart 1: Analyse des Tendances de Prix par Région
            success_dm1 = (
                self.create_dm_regional_price_trends() and
                self.create_dm_neighborhood_comparison() and
                self.create_dm_seasonal_patterns()
            )
            
            # Datamart 2: Évaluation des Facteurs d'Impact sur les Prix
            # Les tables dm_regional_feature_variation et dm_temporal_feature_variation 
            # dépendent de dm_feature_importance (clé étrangère)
            success_dm2_part1 = self.create_dm_feature_importance()
            success_dm2_part2 = (
                self.create_dm_regional_feature_variation() and
                self.create_dm_temporal_feature_variation()
            ) if success_dm2_part1 else False
            
            # Datamart 3: Prédictions de Prix Personnalisées
            # La table dm_prediction_results dépend de dm_prediction_models (clé étrangère)
            success_dm3_part1 = self.create_dm_prediction_models()
            success_dm3_part2 = self.create_dm_prediction_results() if success_dm3_part1 else False
            
            return success_dm1 and success_dm2_part1 and success_dm2_part2 and success_dm3_part1 and success_dm3_part2
        except Exception as e:
            logger.error(f"Erreur lors de la création des tables: {e}")
            return False
        finally:
            self.disconnect()


def main():
    """
    Fonction principale pour exécuter la création de schéma.
    """
    logger.info("Début de la création des schémas des datamarts MySQL")
    
    # Création de l'instance du créateur de schémas
    schema_creator = DatamartSchemaCreator()
    
    # Création de la base de données
    if not schema_creator.create_database():
        logger.error("Échec de la création de la base de données")
        return False
    
    # Création des tables
    success = schema_creator.create_all_tables()
    
    if success:
        logger.info("Tous les schémas des datamarts ont été créés avec succès")
    else:
        logger.error("Échec de la création des schémas des datamarts")
    
    return success


if __name__ == "__main__":
    main()
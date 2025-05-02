#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'importation des données immobilières statiques dans MySQL.
Ce script:
- Se connecte à une base de données MySQL
- Crée la base de données 'immobilier_db' si elle n'existe pas
- Crée une table 'proprietes_raw' avec la structure appropriée
- Importe les données de housing_data.csv dans cette table
- Ajoute une colonne 'source' avec la valeur "static"
"""

import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paramètres de connexion à MySQL
DB_CONFIG = {
    'host': 'localhost',
    'user': 'tatane',
    'password': 'tatane'
}

DB_NAME = 'immobilier_db'
TABLE_NAME = 'proprietes_raw'

CSV_PATH = os.path.join(os.getcwd(), 'data', 'raw', 'housing_data.csv')


def create_database_if_not_exists():
    """
    Crée la base de données 'immobilier_db' si elle n'existe pas déjà.
    
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

def get_connection():
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

def create_table_if_not_exists(conn):
    """
    Crée la table 'proprietes_raw' avec les colonnes appropriées.
    
    Args:
        conn: Connexion à la base de données MySQL
        
    Returns:
        bool: True si la création a réussi, False sinon
    """
    try:
        cursor = conn.cursor()
        
        # Lecture du CSV pour déterminer les colonnes
        df = pd.read_csv(CSV_PATH)
        
        # Construction de la requête CREATE TABLE
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        """
        
        # Mapping des types de colonnes
        # Basé sur l'analyse des premières lignes du CSV
        column_types = {}
        
        # On considère 'Id' comme clé primaire
        create_table_query += "    `Id` INT PRIMARY KEY,\n"
        
        # On définit les types de colonnes en se basant sur les noms et les valeurs
        for column in df.columns:
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
                max_length = df[column].astype(str).str.len().max()
                col_type = f"VARCHAR({max(max_length * 2, 100)})"
            
            create_table_query += f"    `{column}` {col_type},\n"
        
        # Ajout de la colonne 'source'
        create_table_query += "    `source` VARCHAR(50)\n"
        
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

def check_table_exists(conn, table_name):
    """
    Vérifie si une table existe dans la base de données.
    
    Args:
        conn: Connexion à la base de données MySQL
        table_name: Nom de la table à vérifier
        
    Returns:
        bool: True si la table existe, False sinon
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = %s
    """, (DB_NAME, table_name))
    result = cursor.fetchone()
    cursor.close()
    return result[0] > 0

def truncate_table(conn, table_name):
    """
    Vide une table pour permettre la réimportation (idempotence).
    
    Args:
        conn: Connexion à la base de données MySQL
        table_name: Nom de la table à vider
        
    Returns:
        bool: True si la table a été vidée, False sinon
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        cursor.close()
        logger.info(f"Table '{table_name}' vidée pour réimportation")
        return True
    except Error as e:
        logger.error(f"Erreur lors du vidage de la table: {e}")
        return False

def import_data_from_csv(conn):
    """
    Importe les données du CSV dans la table MySQL.
    
    Args:
        conn: Connexion à la base de données MySQL
        
    Returns:
        dict: Résultat de l'importation avec statistiques
    """
    try:
        # Lecture du fichier CSV
        logger.info(f"Lecture du fichier CSV: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        
        # Ajout de la colonne source
        df['source'] = 'static'
        
        # Remplacement des valeurs 'NA' par None pour MySQL
        # (Les valeurs NaN seront gérées lors de la conversion des données)
        df = df.replace('NA', None)
        
        # Vide la table si elle existe déjà (pour assurer l'idempotence)
        if check_table_exists(conn, TABLE_NAME):
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            count = cursor.fetchone()[0]
            cursor.close()
            
            if count > 0:
                logger.info(f"La table contient déjà {count} enregistrements")
                truncate_table(conn, TABLE_NAME)
        
        # Préparation pour l'insertion
        cursor = conn.cursor()
        
        # Conversion des données pour l'insertion
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construction de la requête d'insertion
        insert_query = f"""
        INSERT INTO {TABLE_NAME}
        ({', '.join([f'`{col}`' for col in columns])})
        VALUES ({placeholders})
        """
        
        # Insertion des données par lots
        batch_size = 1000
        total_rows = len(df)
        inserted_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch_df = df.iloc[i:batch_end]
            # Conversion en liste pour l'insertion, en remplaçant NaN par None
            values = []
            for _, row in batch_df.iterrows():
                # Conversion explicite des NaN en None
                processed_row = [None if pd.isna(val) else val for val in row]
                values.append(tuple(processed_row))
            
            
            cursor.executemany(insert_query, values)
            conn.commit()
            
            inserted_rows += len(values)
            logger.info(f"Insertion de {len(values)} lignes ({inserted_rows}/{total_rows})")
        
        # Validation de l'importation
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]
        
        logger.info(f"Importation terminée. Nombre total de lignes dans la table: {final_count}")
        logger.info(f"Nombre de lignes dans le fichier CSV: {total_rows}")
        
        cursor.close()
        
        return {
            "status": "success" if final_count == total_rows else "warning",
            "rows_in_csv": total_rows,
            "rows_imported": final_count,
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Error as e:
        logger.error(f"Erreur lors de l'importation des données: {e}")
        return {
            "status": "error",
            "message": str(e),
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        return {
            "status": "error",
            "message": str(e),
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

def main():
    """
    Fonction principale qui orchestre le processus d'importation.
    """
    logger.info("=== Démarrage du script d'importation des données immobilières ===")
    
    # Création de la base de données si elle n'existe pas
    if not create_database_if_not_exists():
        logger.error("Impossible de créer la base de données. Arrêt du script.")
        return
    
    # Connexion à la base de données
    conn = get_connection()
    if conn is None:
        logger.error("Impossible de se connecter à la base de données. Arrêt du script.")
        return
    
    logger.info("Connexion à la base de données établie")
    
    # Création de la table si elle n'existe pas
    if not create_table_if_not_exists(conn):
        logger.error("Impossible de créer la table. Arrêt du script.")
        conn.close()
        return
    
    # Importation des données
    result = import_data_from_csv(conn)
    
    # Fermeture de la connexion
    conn.close()
    logger.info("Connexion à la base de données fermée")
    
    # Rapport d'importation
    if result["status"] == "success":
        logger.info("=== Importation réussie ===")
        logger.info(f"Lignes dans le CSV: {result['rows_in_csv']}")
        logger.info(f"Lignes importées: {result['rows_imported']}")
    else:
        logger.error(f"=== Échec de l'importation: {result.get('message', 'Raison inconnue')} ===")
    
    logger.info("=== Fin du script d'importation ===")
    return result

if __name__ == "__main__":
    main()
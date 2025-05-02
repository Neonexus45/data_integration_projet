#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de connexion à la base de données MySQL.

Ce module fournit une classe pour gérer les connexions à la base de données MySQL
et récupérer les données nécessaires pour le dashboard.
"""

import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Tuple, Optional, Any, Union
import functools

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def cache_query(ttl_seconds=300):
    """
    Décorateur pour mettre en cache les résultats des requêtes SQL.
    
    Args:
        ttl_seconds (int): Durée de vie du cache en secondes
    """
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Créer une clé de cache basée sur les arguments
            key = str(args) + str(kwargs)
            
            # Vérifier si le résultat est dans le cache et valide
            if key in cache:
                result, timestamp = cache[key]
                if datetime.now().timestamp() - timestamp < ttl_seconds:
                    return result
            
            # Exécuter la fonction et mettre en cache le résultat
            result = func(*args, **kwargs)
            cache[key] = (result, datetime.now().timestamp())
            return result
            
        return wrapper
    return decorator


class MySQLConnector:
    """Classe pour gérer les connexions à la base de données MySQL"""
    
    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Initialise la connexion à la base de données.
        
        Args:
            host (str): Hôte du serveur MySQL
            user (str): Nom d'utilisateur MySQL
            password (str): Mot de passe MySQL
            database (str): Nom de la base de données
        """
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.connect()
    
    def connect(self) -> None:
        """Établit une connexion à la base de données MySQL"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            logger.info("Connexion à la base de données établie")
        except mysql.connector.Error as err:
            logger.error(f"Erreur de connexion à MySQL: {err}")
            raise
    
    def ensure_connection(self) -> None:
        """S'assure que la connexion est active, la rétablit si nécessaire"""
        try:
            if self.conn is None or not self.conn.is_connected():
                logger.info("Reconnexion à la base de données...")
                self.connect()
        except Exception as e:
            logger.error(f"Erreur lors de la vérification de la connexion: {e}")
            self.connect()
    
    def execute_query(self, query: str, params: Tuple = None) -> pd.DataFrame:
        """
        Exécute une requête SQL et retourne les résultats sous forme de DataFrame.
        
        Args:
            query (str): Requête SQL à exécuter
            params (tuple, optional): Paramètres pour la requête
            
        Returns:
            pd.DataFrame: Résultats de la requête
        """
        self.ensure_connection()
        
        try:
            start_time = time.time()
            
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(query, params)
            result = cursor.fetchall()
            
            df = pd.DataFrame(result) if result else pd.DataFrame()
            
            execution_time = time.time() - start_time
            logger.info(f"Requête exécutée en {execution_time:.2f} secondes")
            
            cursor.close()
            return df
            
        except mysql.connector.Error as err:
            logger.error(f"Erreur lors de l'exécution de la requête: {err}")
            logger.debug(f"Requête: {query}")
            logger.debug(f"Paramètres: {params}")
            
            # Rétablir la connexion en cas d'erreur
            self.connect()
            return pd.DataFrame()
    
    def close(self) -> None:
        """Ferme la connexion à la base de données"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("Connexion à la base de données fermée")
    
    @cache_query(ttl_seconds=600)
    def get_regions(self) -> List[str]:
        """
        Récupère la liste des régions disponibles.
        
        Returns:
            List[str]: Liste des noms de régions
        """
        query = """
        SELECT DISTINCT region_name 
        FROM dm_regional_price_trends
        ORDER BY region_name
        """
        
        df = self.execute_query(query)
        
        if df.empty:
            return []
        
        return df['region_name'].tolist()
    
    @cache_query(ttl_seconds=600)
    def get_neighborhoods(self, region: Optional[str] = None) -> List[str]:
        """
        Récupère la liste des quartiers disponibles.
        
        Args:
            region (str, optional): Filtre par région
            
        Returns:
            List[str]: Liste des noms de quartiers
        """
        if region:
            query = """
            SELECT DISTINCT n.neighborhood_name
            FROM dm_neighborhood_comparison n
            JOIN dm_regional_price_trends r ON n.region_id = r.region_id
            WHERE r.region_name = %s
            ORDER BY n.neighborhood_name
            """
            params = (region,)
        else:
            query = """
            SELECT DISTINCT neighborhood_name
            FROM dm_neighborhood_comparison
            ORDER BY neighborhood_name
            """
            params = None
        
        df = self.execute_query(query, params)
        
        if df.empty:
            return []
        
        return df['neighborhood_name'].tolist()
    
    @cache_query(ttl_seconds=3600)
    def get_date_range(self) -> Tuple[datetime, datetime]:
        """
        Récupère la plage de dates disponibles dans les données.
        
        Returns:
            Tuple[datetime, datetime]: Dates minimale et maximale
        """
        query = """
        SELECT MIN(STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d')) as min_date,
               MAX(STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d')) as max_date
        FROM dm_regional_price_trends
        """
        
        df = self.execute_query(query)
        
        if df.empty or pd.isna(df['min_date'].iloc[0]) or pd.isna(df['max_date'].iloc[0]):
            # Valeurs par défaut si aucune donnée n'est disponible
            today = datetime.now()
            return (today - timedelta(days=365), today)
        
        return (df['min_date'].iloc[0], df['max_date'].iloc[0])
    
    @cache_query(ttl_seconds=3600)
    def get_property_types(self) -> List[str]:
        """
        Récupère la liste des types de propriétés disponibles.
        
        Returns:
            List[str]: Liste des types de propriétés
        """
        # Cette information peut ne pas être disponible dans les datamarts
        # Nous retournons donc une liste prédéfinie
        return ["Appartement", "Maison", "Villa", "Studio"]

    @cache_query(ttl_seconds=300)
    def get_price_by_region(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les prix moyens par région.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les prix par région
        """
        conditions = []
        params = []
        
        if filter_params.get('region'):
            conditions.append("region_name = %s")
            params.append(filter_params['region'])
        
        # Filtrage par date
        if filter_params.get('start_date') and filter_params.get('end_date'):
            date_condition = """
            STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d')
            BETWEEN %s AND %s
            """
            conditions.append(date_condition)
            params.append(filter_params['start_date'])
            params.append(filter_params['end_date'])
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            region_name, 
            region_id,
            AVG(avg_price) as avg_price,
            AVG(median_price) as median_price,
            MAX(max_price) as max_price,
            MIN(min_price) as min_price,
            SUM(transaction_count) as total_transactions
        FROM dm_regional_price_trends
        {where_clause}
        GROUP BY region_name, region_id
        ORDER BY region_name
        """
        
        return self.execute_query(query, tuple(params))
    
    @cache_query(ttl_seconds=300)
    def get_price_trends(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les tendances de prix au fil du temps.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les tendances de prix
        """
        conditions = []
        params = []
        
        if filter_params.get('region'):
            conditions.append("region_name = %s")
            params.append(filter_params['region'])
        
        # Filtrage par date
        if filter_params.get('start_date') and filter_params.get('end_date'):
            date_condition = """
            STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d')
            BETWEEN %s AND %s
            """
            conditions.append(date_condition)
            params.append(filter_params['start_date'])
            params.append(filter_params['end_date'])
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            region_name,
            period_year,
            period_month,
            STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d') as date,
            avg_price,
            median_price,
            transaction_count
        FROM dm_regional_price_trends
        {where_clause}
        ORDER BY period_year, period_month
        """
        
        return self.execute_query(query, tuple(params))
    
    @cache_query(ttl_seconds=300)
    def get_feature_importance(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère l'importance des caractéristiques.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec l'importance des caractéristiques
        """
        # Pas de filtrage par région ici car l'importance des caractéristiques est globale
        query = """
        SELECT 
            feature_id,
            feature_name,
            feature_category,
            global_importance_score,
            correlation_with_price
        FROM dm_feature_importance
        ORDER BY global_importance_score DESC
        """
        
        return self.execute_query(query)
    
    @cache_query(ttl_seconds=300)
    def get_regional_feature_variation(self, feature_ids: List[int], filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les variations régionales de l'importance des caractéristiques.
        
        Args:
            feature_ids (List[int]): Liste des IDs de caractéristiques
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les variations régionales
        """
        conditions = []
        params = []
        
        # Filtrer par caractéristiques sélectionnées
        if feature_ids:
            feature_condition = "rfv.feature_id IN ({})".format(",".join(["%s"] * len(feature_ids)))
            conditions.append(feature_condition)
            params.extend(feature_ids)
        
        # Filtrer par région
        if filter_params.get('region'):
            conditions.append("rpt.region_name = %s")
            params.append(filter_params['region'])
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            fi.feature_name,
            rpt.region_name,
            rfv.importance_score,
            rfv.relative_importance
        FROM dm_regional_feature_variation rfv
        JOIN dm_feature_importance fi ON rfv.feature_id = fi.feature_id
        JOIN dm_regional_price_trends rpt ON rfv.region_id = rpt.region_id
        {where_clause}
        GROUP BY fi.feature_name, rpt.region_name, rfv.importance_score, rfv.relative_importance
        ORDER BY fi.feature_name, rpt.region_name
        """
        
        return self.execute_query(query, tuple(params))
    
    @cache_query(ttl_seconds=300)
    def get_temporal_feature_variation(self, feature_ids: List[int], filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les variations temporelles de l'importance des caractéristiques.
        
        Args:
            feature_ids (List[int]): Liste des IDs de caractéristiques
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les variations temporelles
        """
        conditions = []
        params = []
        
        # Filtrer par caractéristiques sélectionnées
        if feature_ids:
            feature_condition = "tfv.feature_id IN ({})".format(",".join(["%s"] * len(feature_ids)))
            conditions.append(feature_condition)
            params.extend(feature_ids)
        
        # Filtrage par date
        if filter_params.get('start_date') and filter_params.get('end_date'):
            start_year = filter_params['start_date'].year
            end_year = filter_params['end_date'].year
            
            date_condition = "tfv.period_year BETWEEN %s AND %s"
            conditions.append(date_condition)
            params.append(start_year)
            params.append(end_year)
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            fi.feature_name,
            tfv.period_year,
            tfv.period_quarter,
            tfv.importance_score
        FROM dm_temporal_feature_variation tfv
        JOIN dm_feature_importance fi ON tfv.feature_id = fi.feature_id
        {where_clause}
        ORDER BY fi.feature_name, tfv.period_year, tfv.period_quarter
        """
        
        return self.execute_query(query, tuple(params))
    
    @cache_query(ttl_seconds=300)
    def get_predictions(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les prédictions de prix.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les prédictions
        """
        # Obtenir les prédictions du modèle principal (active=1)
        query = """
        SELECT 
            pr.prediction_id,
            pr.property_id,
            pr.predicted_price,
            pr.prediction_interval_low,
            pr.prediction_interval_high,
            pr.confidence_score,
            pr.feature_contributions,
            pm.model_name,
            pm.model_metrics
        FROM dm_prediction_results pr
        JOIN dm_prediction_models pm ON pr.model_id = pm.model_id
        WHERE pm.is_active = 1
        ORDER BY pr.prediction_id
        LIMIT 20
        """
        
        return self.execute_query(query)
    
    @cache_query(ttl_seconds=300)
    def get_prediction_models(self) -> pd.DataFrame:
        """
        Récupère les modèles de prédiction disponibles.
        
        Returns:
            pd.DataFrame: DataFrame avec les modèles de prédiction
        """
        query = """
        SELECT 
            model_id,
            model_name,
            model_version,
            training_date,
            model_metrics,
            is_active
        FROM dm_prediction_models
        ORDER BY is_active DESC, model_name
        """
        
        return self.execute_query(query)
    
    @cache_query(ttl_seconds=300)
    def get_neighborhood_comparison(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les données de comparaison des quartiers.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les comparaisons de quartiers
        """
        conditions = []
        params = []
        
        if filter_params.get('region'):
            conditions.append("rpt.region_name = %s")
            params.append(filter_params['region'])
        
        if filter_params.get('neighborhood'):
            conditions.append("nc.neighborhood_name = %s")
            params.append(filter_params['neighborhood'])
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            nc.neighborhood_id,
            nc.neighborhood_name,
            nc.city_name,
            rpt.region_name,
            nc.avg_price,
            nc.price_per_sqft,
            nc.price_change_yoy,
            nc.avg_days_on_market,
            nc.walkability_score,
            nc.school_rating,
            nc.crime_index
        FROM dm_neighborhood_comparison nc
        JOIN dm_regional_price_trends rpt ON nc.region_id = rpt.region_id
        {where_clause}
        GROUP BY 
            nc.neighborhood_id, nc.neighborhood_name, nc.city_name, rpt.region_name,
            nc.avg_price, nc.price_per_sqft, nc.price_change_yoy, nc.avg_days_on_market,
            nc.walkability_score, nc.school_rating, nc.crime_index
        ORDER BY nc.avg_price DESC
        """
        
        return self.execute_query(query, tuple(params))
    
    @cache_query(ttl_seconds=300)
    def get_seasonal_patterns(self, filter_params: Dict) -> pd.DataFrame:
        """
        Récupère les patterns saisonniers des prix.
        
        Args:
            filter_params (Dict): Paramètres de filtrage
            
        Returns:
            pd.DataFrame: DataFrame avec les patterns saisonniers
        """
        conditions = []
        params = []
        
        if filter_params.get('region'):
            conditions.append("rpt.region_name = %s")
            params.append(filter_params['region'])
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT 
            rpt.region_name,
            sp.month,
            sp.price_index,
            sp.transaction_volume_index,
            sp.days_on_market_index,
            sp.year_of_analysis
        FROM dm_seasonal_patterns sp
        JOIN dm_regional_price_trends rpt ON sp.region_id = rpt.region_id
        {where_clause}
        GROUP BY 
            rpt.region_name, sp.month, sp.price_index, sp.transaction_volume_index,
            sp.days_on_market_index, sp.year_of_analysis
        ORDER BY rpt.region_name, sp.month
        """
        
        return self.execute_query(query, tuple(params))
    
    def __del__(self):
        """Destructeur pour s'assurer que la connexion est fermée"""
        self.close()
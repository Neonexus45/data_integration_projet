#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL pour le Datamart 1: Analyse des Tendances de Prix par Région
Ce module est responsable de transformer et charger les données pour le datamart des tendances de prix
"""

import logging
import traceback
from pyspark.sql.functions import col, expr, lit, coalesce, current_timestamp
from pyspark.sql.types import DecimalType

from etl_base import BaseETL, logger

class PriceTrendsETL(BaseETL):
    """
    ETL spécifique pour le Datamart 1: Analyse des Tendances de Prix
    """
    
    def __init__(self, **kwargs):
        """Initialise l'ETL avec les paramètres de base"""
        super().__init__(**kwargs)
        # Tables Gold nécessaires pour ce datamart
        self.required_tables = ["price_trends", "market_indicators", "seasonal_variations"]
        
    def load_data(self):
        """Charge les données Gold nécessaires pour ce datamart"""
        return self.read_gold_data(self.required_tables)
        
    def transform_regional_price_trends(self):
        """
        Transforme les données GOLD pour la table dm_regional_price_trends du Datamart 1.
        
        Returns:
            DataFrame: DataFrame transformé pour l'insertion dans MySQL
        """
        logger.info("Transformation des données pour dm_regional_price_trends...")
        
        try:
            # Vérifier que les données source sont disponibles
            if self.gold_tables["price_trends"] is None:
                logger.error("Données manquantes pour transformer dm_regional_price_trends")
                return None
            
            # Vérifier les colonnes requises
            required_columns = ["period_type", "region", "sale_year", "sale_month", 
                              "avg_price", "median_price", "min_price", "max_price", "properties_sold"]
            
            df = self.gold_tables["price_trends"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"Colonnes manquantes pour dm_regional_price_trends: {missing_columns}")
                # Ajouter des colonnes manquantes avec des valeurs par défaut
                for missing_col in missing_columns:
                    if missing_col in ["avg_price", "median_price", "min_price", "max_price"]:
                        df = df.withColumn(missing_col, lit(0.0))
                    elif missing_col in ["sale_year", "sale_month", "properties_sold"]:
                        df = df.withColumn(missing_col, lit(0))
                    elif missing_col == "period_type":
                        df = df.withColumn(missing_col, lit("monthly"))
                    elif missing_col == "region":
                        df = df.withColumn(missing_col, lit("Inconnu"))
            
            # Filtrer les données pour ne conserver que les tendances mensuelles par région
            df = df.filter(
                (col("period_type") == "monthly") & col("region").isNotNull()
            )
            
            # Adapter le schéma pour correspondre à celui de la table MySQL
            df_transformed = df.select(
                # Créer un region_id en utilisant le hash du nom de région
                expr("abs(hash(region)) % 1000").alias("region_id"),
                col("region").alias("region_name"),
                col("sale_year").alias("period_year"),
                col("sale_month").alias("period_month"),
                col("avg_price"),
                col("median_price"),
                col("min_price"),
                col("max_price"),
                # Calculer un momentum de prix à partir des données disponibles
                # (variation par rapport au mois précédent, ou 0 si non disponible)
                lit(0).alias("price_momentum"),  # À remplacer par un calcul réel quand disponible
                col("properties_sold").alias("transaction_count")
            )
            
            # Ajouter les colonnes de métadonnées pour MySQL
            df_transformed = df_transformed.withColumn("created_at", current_timestamp())
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation pour dm_regional_price_trends: {e}")
            traceback.print_exc()
            return None
    
    def transform_neighborhood_comparison(self):
        """
        Transforme les données GOLD pour la table dm_neighborhood_comparison du Datamart 1.
        
        Returns:
            DataFrame: DataFrame transformé pour l'insertion dans MySQL
        """
        logger.info("Transformation des données pour dm_neighborhood_comparison...")
        
        try:
            # Vérifier que les données source sont disponibles
            if (self.gold_tables["price_trends"] is None or 
                self.gold_tables["market_indicators"] is None):
                logger.error("Données manquantes pour transformer dm_neighborhood_comparison")
                return None
            
            # Vérifier les colonnes requises dans price_trends
            price_trends_df = self.gold_tables["price_trends"]
            required_columns_pt = ["period_type", "neighborhood", "avg_price", "property_price_per_sqm"]
            
            missing_columns_pt = [col for col in required_columns_pt if col not in price_trends_df.columns]
            if missing_columns_pt:
                logger.warning(f"Colonnes manquantes dans price_trends: {missing_columns_pt}")
                # Ajouter des colonnes manquantes avec des valeurs par défaut
                for missing_col in missing_columns_pt:
                    if missing_col in ["avg_price", "property_price_per_sqm"]:
                        price_trends_df = price_trends_df.withColumn(missing_col, lit(0.0))
                    elif missing_col == "period_type":
                        price_trends_df = price_trends_df.withColumn(missing_col, lit("neighborhood_monthly"))
                    elif missing_col == "neighborhood":
                        price_trends_df = price_trends_df.withColumn(missing_col, lit("Inconnu"))
                
                self.gold_tables["price_trends"] = price_trends_df
            
            # Vérifier les colonnes requises dans market_indicators
            market_indicators_df = self.gold_tables["market_indicators"]
            required_columns_mi = ["location_type", "neighborhood", "avg_days_on_market"]
            
            missing_columns_mi = [col for col in required_columns_mi if col not in market_indicators_df.columns]
            if missing_columns_mi:
                logger.warning(f"Colonnes manquantes dans market_indicators: {missing_columns_mi}")
                # Ajouter des colonnes manquantes avec des valeurs par défaut
                for missing_col in missing_columns_mi:
                    if missing_col == "avg_days_on_market":
                        market_indicators_df = market_indicators_df.withColumn(missing_col, lit(30.0))
                    elif missing_col == "location_type":
                        market_indicators_df = market_indicators_df.withColumn(missing_col, lit("neighborhood"))
                    elif missing_col == "neighborhood":
                        market_indicators_df = market_indicators_df.withColumn(missing_col, lit("Inconnu"))
                        
                self.gold_tables["market_indicators"] = market_indicators_df
            
            # Filtrer les données pour ne conserver que les tendances par quartier
            neighborhood_trends = self.gold_tables["price_trends"].filter(
                (col("period_type") == "neighborhood_monthly") & col("neighborhood").isNotNull()
            )
            
            # Calculer les métriques moyennes par quartier (sur toutes les périodes)
            # Correction: Utiliser des fonctions d'agrégation pour toutes les colonnes non-incluses dans GROUP BY
            from pyspark.sql.functions import avg, first
            
            df = neighborhood_trends.groupBy("neighborhood").agg(
                expr("abs(hash(neighborhood)) % 10000").alias("neighborhood_id"),
                avg("avg_price").alias("avg_price"),  # Utiliser avg() au lieu de col()
                avg(coalesce(col("property_price_per_sqm"), lit(0))).alias("price_per_sqft"),
                lit(None).cast(DecimalType(5, 2)).alias("price_change_yoy")  # À calculer si possible
            )
            
            # Joindre les données des indicateurs de marché pour obtenir les jours sur le marché
            # Correction: Utiliser avg() pour agréger avg_days_on_market
            from pyspark.sql.functions import avg
            
            market_data = self.gold_tables["market_indicators"].filter(
                col("location_type") == "neighborhood"
            ).groupBy("neighborhood").agg(
                avg("avg_days_on_market").alias("avg_days_on_market")
            )
            
            df = df.join(market_data, "neighborhood", "left_outer")
            
            # Compléter avec des données factices pour les colonnes manquantes
            df = df.withColumn("city_name", lit("Ville par défaut"))  # À remplacer par des données réelles si disponibles
            df = df.withColumn("region_id", expr("abs(hash(neighborhood)) % 1000"))  # ID de région fictif
            
            # Ajouter des scores fictifs (à remplacer par des données réelles si disponibles)
            df = df.withColumn("walkability_score", lit(5.0).cast(DecimalType(4, 1)))
            df = df.withColumn("school_rating", lit(4.5).cast(DecimalType(4, 1)))
            df = df.withColumn("crime_index", lit(3.0).cast(DecimalType(4, 1)))
            
            # Ajouter les colonnes de métadonnées pour MySQL
            df = df.withColumn("created_at", current_timestamp())
            df = df.withColumn("last_updated", current_timestamp())
            
            # Adapter le schéma final pour correspondre à celui de la table MySQL
            df_transformed = df.select(
                col("neighborhood_id"),
                col("neighborhood").alias("neighborhood_name"),
                col("city_name"),
                col("region_id"),
                col("avg_price"),
                coalesce(col("price_per_sqft"), lit(0)).alias("price_per_sqft"),
                coalesce(col("price_change_yoy"), lit(0)).alias("price_change_yoy"),
                coalesce(col("avg_days_on_market"), lit(30)).alias("avg_days_on_market"),
                col("walkability_score"),
                col("school_rating"),
                col("crime_index"),
                col("created_at"),
                col("last_updated")
            )
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation pour dm_neighborhood_comparison: {e}")
            traceback.print_exc()
            return None

    def transform_seasonal_patterns(self):
        """
        Transforme les données GOLD pour la table dm_seasonal_patterns du Datamart 1.
        
        Returns:
            DataFrame: DataFrame transformé pour l'insertion dans MySQL
        """
        logger.info("Transformation des données pour dm_seasonal_patterns...")
        
        try:
            # Vérifier que les données source sont disponibles
            if self.gold_tables["seasonal_variations"] is None:
                logger.error("Données manquantes pour transformer dm_seasonal_patterns")
                return None
            
            # Vérifier les colonnes requises
            seasonal_df = self.gold_tables["seasonal_variations"]
            required_columns = ["region", "sale_month", "seasonal_index", "year"]
            
            missing_columns = [col for col in required_columns if col not in seasonal_df.columns]
            if missing_columns:
                logger.warning(f"Colonnes manquantes dans seasonal_variations: {missing_columns}")
                # Ajouter des colonnes manquantes avec des valeurs par défaut
                for missing_col in missing_columns:
                    if missing_col == "seasonal_index":
                        seasonal_df = seasonal_df.withColumn(missing_col, lit(1.0))
                    elif missing_col in ["sale_month", "year"]:
                        seasonal_df = seasonal_df.withColumn(missing_col, lit(0))
                    elif missing_col == "region":
                        seasonal_df = seasonal_df.withColumn(missing_col, lit("Inconnu"))
                
                self.gold_tables["seasonal_variations"] = seasonal_df
            
            # Filtrer les données pour ne conserver que les variations saisonnières par région
            df = self.gold_tables["seasonal_variations"].filter(
                col("region").isNotNull()
            )
            
            # Adapter le schéma pour correspondre à celui de la table MySQL
            df_transformed = df.select(
                # Créer un region_id en utilisant le hash du nom de région
                expr("abs(hash(region)) % 1000").alias("region_id"),
                col("sale_month").alias("month"),
                col("seasonal_index").alias("price_index"),
                # Utiliser d'autres métriques disponibles ou des valeurs par défaut
                lit(1.0).cast(DecimalType(5, 2)).alias("transaction_volume_index"),  # À remplacer par données réelles
                lit(1.0).cast(DecimalType(5, 2)).alias("days_on_market_index"),      # À remplacer par données réelles
                col("year").alias("year_of_analysis")
            )
            
            # Ajouter les colonnes de métadonnées pour MySQL
            df_transformed = df_transformed.withColumn("created_at", current_timestamp())
            df_transformed = df_transformed.withColumn("last_updated", current_timestamp())
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation pour dm_seasonal_patterns: {e}")
            traceback.print_exc()
            return None
    
    def load_datamart(self):
        """
        Charge les données dans les tables du Datamart 1.
        
        Returns:
            dict: Statistiques de chargement
        """
        logger.info("Chargement des données dans le Datamart 1...")
        stats = {}
        
        # 1. dm_regional_price_trends
        df = self.transform_regional_price_trends()
        if df is not None:
            records = self.insert_into_mysql(df, "dm_regional_price_trends")
            stats["dm_regional_price_trends"] = records
        
        # 2. dm_neighborhood_comparison
        df = self.transform_neighborhood_comparison()
        if df is not None:
            records = self.insert_into_mysql(df, "dm_neighborhood_comparison")
            stats["dm_neighborhood_comparison"] = records
        
        # 3. dm_seasonal_patterns
        df = self.transform_seasonal_patterns()
        if df is not None:
            records = self.insert_into_mysql(df, "dm_seasonal_patterns")
            stats["dm_seasonal_patterns"] = records
        
        return stats
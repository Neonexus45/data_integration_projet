#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col, isnan

# Initialiser Spark
spark = SparkSession.builder.appName('DataIntegrityCheck').getOrCreate()

# Liste des tables SILVER à vérifier
tables = [
    'property_details',
    'location_details', 
    'building_features',
    'sale_history'
]

# Analyser chaque table
for table_name in tables:
    # Chemin vers les données
    path = f'data/silver/{table_name}/year=2025/month=05/day=02/{table_name}_20250502.parquet/'
    
    # Lire les données
    df = spark.read.parquet(path)
    
    # Nombre total d'enregistrements
    total_count = df.count()
    
    print(f'\n==== TABLE: {table_name.upper()} (Total: {total_count} enregistrements) ====')
    
    # Calculer le nombre de valeurs non-nulles pour chaque colonne
    print(f'COLONNE                      | NON-NULL     | % REMPLI     | TYPE')
    print(f'-----------------------------|--------------|--------------|-------------')
    
    for column_name in df.columns:
        # Compter les valeurs non-nulles
        non_null_count = df.filter(col(column_name).isNotNull()).count()
        
        # Calculer le pourcentage
        percentage = (non_null_count / total_count) * 100
        
        # Obtenir le type de données
        data_type = str(df.schema[column_name].dataType)
        
        # Formater pour un affichage propre
        col_padded = column_name.ljust(28)
        count_padded = str(non_null_count).ljust(14)
        pct_padded = f"{percentage:.2f}%".ljust(14)
        
        # Afficher les résultats
        print(f'{col_padded}| {count_padded}| {pct_padded}| {data_type}')

# Fermer la session
spark.stop()
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Composant pour afficher une carte des prix par région/quartier
"""

import streamlit as st
import pandas as pd
import numpy as np
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import plotly.express as px
import json
from typing import Dict, List, Tuple, Optional, Any

# Coordonnées approximatives des régions pour la démonstration
# Dans un cas réel, ces coordonnées seraient stockées dans la base de données
DEFAULT_REGIONS = {
    "North": {"lat": 48.9, "lon": 2.3, "color": "#1f77b4"},
    "South": {"lat": 48.7, "lon": 2.3, "color": "#ff7f0e"},
    "East": {"lat": 48.8, "lon": 2.5, "color": "#2ca02c"},
    "West": {"lat": 48.8, "lon": 2.1, "color": "#d62728"}
}

def display_price_map(db_connector, filter_params: Dict) -> None:
    """
    Affiche une carte des prix moyens par région ou quartier.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.header("Carte des prix immobiliers")
    
    # Options d'affichage
    view_option = st.radio(
        "Afficher par",
        options=["Région", "Quartier"],
        horizontal=True
    )
    
    # Récupération des données
    if view_option == "Région":
        df = db_connector.get_price_by_region(filter_params)
        
        if df.empty:
            st.warning("Aucune donnée disponible pour les régions sélectionnées.")
            return
        
        # Création de la carte
        display_region_map(df)
    else:
        # Récupération des données par quartier
        df = db_connector.get_neighborhood_comparison(filter_params)
        
        if df.empty:
            st.warning("Aucune donnée disponible pour les quartiers sélectionnés.")
            return
        
        # Création de la carte
        display_neighborhood_map(df)

def display_region_map(df: pd.DataFrame) -> None:
    """
    Affiche une carte choroplèthe des prix par région.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les prix par région
    """
    # Créer une carte centrée sur la région
    map_center = [48.8, 2.3]  # Coordonnées par défaut (Paris)
    folium_map = folium.Map(location=map_center, zoom_start=11, tiles="CartoDB positron")
    
    # Créer les marqueurs pour chaque région
    for idx, row in df.iterrows():
        region_name = row['region_name']
        avg_price = row['avg_price']
        median_price = row['median_price']
        
        # Obtenir les coordonnées de la région (ou utiliser des valeurs par défaut)
        region_info = DEFAULT_REGIONS.get(region_name, {"lat": 48.8, "lon": 2.3, "color": "#3186cc"})
        
        # Création du popup avec les informations de prix
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin-bottom: 10px;">{region_name}</h4>
            <p><b>Prix moyen:</b> {avg_price:,.0f} €</p>
            <p><b>Prix médian:</b> {median_price:,.0f} €</p>
            <p><b>Transactions:</b> {row['total_transactions']}</p>
        </div>
        """
        
        # Afficher le marqueur
        folium.CircleMarker(
            location=[region_info["lat"], region_info["lon"]],
            radius=20,
            color=region_info["color"],
            fill=True,
            fill_color=region_info["color"],
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(folium_map)
        
        # Ajouter un label avec le nom de la région
        folium.map.Marker(
            [region_info["lat"], region_info["lon"]],
            icon=folium.DivIcon(
                icon_size=(150, 36),
                icon_anchor=(75, 18),
                html=f'<div style="font-size: 12pt; font-weight: bold; text-align: center;">{region_name}</div>'
            )
        ).add_to(folium_map)
    
    # Affichage de la carte dans Streamlit
    folium_static(folium_map)
    
    # Afficher un graphique de comparaison des prix par région
    st.subheader("Comparaison des prix par région")
    
    # Créer un DataFrame pour le graphique
    chart_df = df[['region_name', 'avg_price', 'median_price']].copy()
    chart_df.columns = ['Région', 'Prix moyen', 'Prix médian']
    
    # Réorganiser le DataFrame pour l'adapter à Plotly
    chart_df_melted = pd.melt(
        chart_df, 
        id_vars=['Région'],
        value_vars=['Prix moyen', 'Prix médian'],
        var_name='Métrique',
        value_name='Prix (€)'
    )
    
    # Créer le graphique avec Plotly
    fig = px.bar(
        chart_df_melted,
        x='Région',
        y='Prix (€)',
        color='Métrique',
        barmode='group',
        title="Prix moyens et médians par région",
        labels={'Prix (€)': 'Prix (€)', 'Région': 'Région'},
        height=500
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Région",
        yaxis_title="Prix (€)",
        legend_title="Métrique",
        font=dict(family="Arial", size=12),
        hovermode="x unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)

def display_neighborhood_map(df: pd.DataFrame) -> None:
    """
    Affiche une carte des prix par quartier avec clustering.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les prix par quartier
    """
    # Créer une carte centrée sur la région
    map_center = [48.8, 2.3]  # Coordonnées par défaut (Paris)
    folium_map = folium.Map(location=map_center, zoom_start=12, tiles="CartoDB positron")
    
    # Ajouter un cluster de marqueurs
    marker_cluster = MarkerCluster().add_to(folium_map)
    
    # Pour les besoins de la démonstration, générer des coordonnées aléatoires
    # pour chaque quartier autour du centre de la carte
    # Dans une application réelle, ces coordonnées viendraient de la base de données
    np.random.seed(42)  # Pour reproductibilité
    
    # Attribuer des couleurs en fonction du prix
    min_price = df['avg_price'].min()
    max_price = df['avg_price'].max()
    
    for idx, row in df.iterrows():
        neighborhood_name = row['neighborhood_name']
        avg_price = row['avg_price']
        price_per_sqft = row['price_per_sqft']
        
        # Générer des coordonnées aléatoires autour du centre
        lat = map_center[0] + np.random.normal(0, 0.03)
        lon = map_center[1] + np.random.normal(0, 0.03)
        
        # Calculer une couleur basée sur le prix (du vert au rouge)
        price_ratio = (avg_price - min_price) / (max_price - min_price) if max_price > min_price else 0.5
        color = f"#{int(255 * (1 - price_ratio)):02x}{int(255 * price_ratio):02x}00"
        
        # Création du popup avec les informations de prix
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin-bottom: 10px;">{neighborhood_name}</h4>
            <p><b>Prix moyen:</b> {avg_price:,.0f} €</p>
            <p><b>Prix au m²:</b> {price_per_sqft:,.0f} €/m²</p>
            <p><b>Jours sur le marché:</b> {row['avg_days_on_market']:.1f}</p>
            <p><b>Score de marchabilité:</b> {row['walkability_score']}/10</p>
            <p><b>Écoles:</b> {row['school_rating']}/10</p>
        </div>
        """
        
        # Afficher le marqueur dans le cluster
        folium.CircleMarker(
            location=[lat, lon],
            radius=15,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(marker_cluster)
    
    # Affichage de la carte dans Streamlit
    folium_static(folium_map)
    
    # Afficher un graphique de comparaison des quartiers les plus chers
    st.subheader("Top 10 des quartiers les plus chers")
    
    # Trier les quartiers par prix moyen
    top_df = df.sort_values('avg_price', ascending=False).head(10).copy()
    
    # Créer le graphique avec Plotly
    fig = px.bar(
        top_df,
        x='neighborhood_name',
        y='avg_price',
        color='avg_price',
        color_continuous_scale='Viridis',
        title="Top 10 des quartiers par prix moyen",
        labels={
            'neighborhood_name': 'Quartier', 
            'avg_price': 'Prix moyen (€)'
        },
        height=500
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Quartier",
        yaxis_title="Prix moyen (€)",
        xaxis_tickangle=-45,
        font=dict(family="Arial", size=12),
        coloraxis_showscale=False
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
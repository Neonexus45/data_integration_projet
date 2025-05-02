#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Composant pour afficher la comparaison des quartiers
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional, Any

def display_neighborhood_comparison(db_connector, filter_params: Dict) -> None:
    """
    Affiche les comparaisons entre quartiers.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.header("Comparaison des quartiers")
    
    # Récupération des données
    df = db_connector.get_neighborhood_comparison(filter_params)
    
    if df.empty:
        st.warning("Aucune donnée disponible pour les quartiers sélectionnés.")
        return
    
    # Options d'affichage
    display_option = st.radio(
        "Type de comparaison",
        options=["Vue d'ensemble", "Détails par quartier", "Analyse comparative"],
        horizontal=True
    )
    
    if display_option == "Vue d'ensemble":
        display_overview(df)
    elif display_option == "Détails par quartier":
        display_neighborhood_details(df)
    else:
        display_comparative_analysis(df)

def display_overview(df: pd.DataFrame) -> None:
    """
    Affiche une vue d'ensemble des quartiers.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
    """
    # Afficher un tableau récapitulatif
    st.subheader("Vue d'ensemble des quartiers")
    
    # Créer une version simplifiée pour l'affichage
    overview_df = df.copy()
    
    # Renommer les colonnes pour l'affichage
    overview_df = overview_df.rename(columns={
        'neighborhood_name': 'Quartier',
        'city_name': 'Ville',
        'region_name': 'Région',
        'avg_price': 'Prix moyen (€)',
        'price_per_sqft': 'Prix au m² (€/m²)',
        'price_change_yoy': 'Évolution annuelle (%)',
        'avg_days_on_market': 'Jours sur le marché',
        'walkability_score': 'Score marchabilité',
        'school_rating': 'Note écoles',
        'crime_index': 'Indice criminalité'
    })
    
    # Sélectionner les colonnes à afficher
    display_cols = [
        'Quartier', 'Ville', 'Région', 
        'Prix moyen (€)', 'Prix au m² (€/m²)', 'Évolution annuelle (%)',
        'Jours sur le marché', 'Score marchabilité', 'Note écoles', 'Indice criminalité'
    ]
    
    # Formater les colonnes numériques
    overview_df['Prix moyen (€)'] = overview_df['Prix moyen (€)'].map(lambda x: f"{x:,.0f}")
    overview_df['Prix au m² (€/m²)'] = overview_df['Prix au m² (€/m²)'].map(lambda x: f"{x:,.0f}")
    overview_df['Évolution annuelle (%)'] = overview_df['Évolution annuelle (%)'].map(lambda x: f"{x:.1f}")
    
    # Afficher le tableau
    st.dataframe(overview_df[display_cols], use_container_width=True)
    
    # Graphique de bulle comparative
    st.subheader("Comparaison prix-marchabilité-écoles")
    
    # Créer un graphique de bulles comparatives
    fig = px.scatter(
        df,
        x="walkability_score",
        y="avg_price",
        size="price_per_sqft",
        color="school_rating",
        hover_name="neighborhood_name",
        text="neighborhood_name",
        size_max=50,
        title="Comparaison des quartiers: Prix vs Marchabilité vs Écoles",
        labels={
            "walkability_score": "Score de marchabilité (0-10)",
            "avg_price": "Prix moyen (€)",
            "school_rating": "Note des écoles (0-10)",
            "price_per_sqft": "Prix au m²"
        },
        height=600
    )
    
    # Personnaliser le graphique
    fig.update_traces(
        textposition='top center',
        marker=dict(line=dict(width=1, color='DarkSlateGrey')),
        selector=dict(mode='markers+text')
    )
    
    fig.update_layout(
        xaxis=dict(range=[0, 10]),
        font=dict(family="Arial", size=12)
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Ajouter une explication
    st.markdown("""
    **Comment lire ce graphique:**
    - **Axe X**: Score de marchabilité (accessibilité à pied aux services et commerces)
    - **Axe Y**: Prix moyen des propriétés
    - **Taille des bulles**: Prix au m² (plus la bulle est grande, plus le prix au m² est élevé)
    - **Couleur**: Note des écoles (bleu foncé = meilleures écoles)
    
    Les quartiers situés en haut à droite offrent une bonne marchabilité et de bonnes écoles, mais sont généralement plus chers.
    """)

def display_neighborhood_details(df: pd.DataFrame) -> None:
    """
    Affiche les détails d'un quartier spécifique.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
    """
    # Permettre à l'utilisateur de sélectionner un quartier
    neighborhoods = df['neighborhood_name'].unique()
    
    selected_neighborhood = st.selectbox(
        "Sélectionner un quartier",
        options=neighborhoods
    )
    
    # Filtrer les données pour le quartier sélectionné
    neighborhood_data = df[df['neighborhood_name'] == selected_neighborhood].iloc[0]
    
    # Afficher les informations du quartier
    st.subheader(f"Détails du quartier: {selected_neighborhood}")
    
    # Créer trois colonnes pour les métriques
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Prix moyen",
            value=f"{neighborhood_data['avg_price']:,.0f} €",
            delta=f"{neighborhood_data['price_change_yoy']:.1f}%" if 'price_change_yoy' in neighborhood_data else None,
            help="Prix moyen des propriétés dans ce quartier"
        )
    
    with col2:
        st.metric(
            label="Prix au m²",
            value=f"{neighborhood_data['price_per_sqft']:,.0f} €/m²",
            help="Prix moyen par mètre carré dans ce quartier"
        )
    
    with col3:
        st.metric(
            label="Jours sur le marché",
            value=f"{neighborhood_data['avg_days_on_market']:.0f}",
            delta=None,
            help="Nombre moyen de jours qu'une propriété reste sur le marché avant d'être vendue"
        )
    
    # Créer un graphique radar des indices de qualité de vie
    categories = ['Marchabilité', 'Écoles', 'Sécurité']
    values = [
        neighborhood_data['walkability_score'],
        neighborhood_data['school_rating'],
        10 - neighborhood_data['crime_index']  # Inverser l'indice de criminalité pour qu'une valeur plus élevée soit meilleure
    ]
    
    # Créer le graphique radar
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=selected_neighborhood
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )
        ),
        title="Indice de qualité de vie",
        height=400
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Texte descriptif
    st.markdown(f"""
    ### À propos de {selected_neighborhood}
    
    {selected_neighborhood} est un quartier situé dans la ville de {neighborhood_data['city_name']}, 
    région {neighborhood_data['region_name']}. 
    
    **Points forts:**
    - Score de marchabilité: {neighborhood_data['walkability_score']}/10
    - Qualité des écoles: {neighborhood_data['school_rating']}/10
    - Indice de sécurité: {10 - neighborhood_data['crime_index']}/10
    
    Les propriétés dans ce quartier restent en moyenne {neighborhood_data['avg_days_on_market']:.0f} jours 
    sur le marché avant d'être vendues, avec un prix moyen de {neighborhood_data['avg_price']:,.0f} €.
    """)

def display_comparative_analysis(df: pd.DataFrame) -> None:
    """
    Affiche une analyse comparative entre plusieurs quartiers.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
    """
    st.subheader("Analyse comparative des quartiers")
    
    # Permettre à l'utilisateur de sélectionner des quartiers à comparer
    neighborhoods = df['neighborhood_name'].unique()
    
    selected_neighborhoods = st.multiselect(
        "Sélectionner des quartiers à comparer",
        options=neighborhoods,
        default=neighborhoods[:min(4, len(neighborhoods))]
    )
    
    if not selected_neighborhoods:
        st.warning("Veuillez sélectionner au moins un quartier.")
        return
    
    # Filtrer les données pour les quartiers sélectionnés
    comparison_df = df[df['neighborhood_name'].isin(selected_neighborhoods)]
    
    # Sélectionner les métriques à comparer
    metrics = [
        ("Prix moyen (€)", "avg_price"),
        ("Prix au m² (€/m²)", "price_per_sqft"),
        ("Jours sur le marché", "avg_days_on_market"),
        ("Score de marchabilité", "walkability_score"),
        ("Note des écoles", "school_rating"),
        ("Indice de criminalité", "crime_index")
    ]
    
    selected_metrics = st.multiselect(
        "Sélectionner des métriques à comparer",
        options=[m[0] for m in metrics],
        default=[m[0] for m in metrics[:3]]
    )
    
    if not selected_metrics:
        st.warning("Veuillez sélectionner au moins une métrique.")
        return
    
    # Créer un graphique en radar pour comparer les quartiers
    if len(selected_metrics) > 2 and len(selected_neighborhoods) <= 5:
        display_radar_comparison(comparison_df, selected_neighborhoods, metrics, selected_metrics)
    
    # Créer un graphique en barres pour comparer les quartiers
    display_bar_comparison(comparison_df, selected_neighborhoods, metrics, selected_metrics)
    
    # Afficher le rapport prix/qualité
    display_value_index(comparison_df, selected_neighborhoods)

def display_radar_comparison(df: pd.DataFrame, neighborhoods: List[str], 
                           metrics: List[Tuple[str, str]], selected_metrics: List[str]) -> None:
    """
    Affiche une comparaison radar des quartiers.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
        neighborhoods (List[str]): Liste des quartiers sélectionnés
        metrics (List[Tuple[str, str]]): Liste des métriques disponibles (nom, colonne)
        selected_metrics (List[str]): Liste des métriques sélectionnées
    """
    # Créer le graphique radar
    fig = go.Figure()
    
    # Sélectionner les métriques et les colonnes correspondantes
    radar_metrics = [m for m in metrics if m[0] in selected_metrics]
    categories = [m[0] for m in radar_metrics]
    columns = [m[1] for m in radar_metrics]
    
    # Normaliser les valeurs pour le radar (0-10)
    normalized_df = df.copy()
    
    for _, col in radar_metrics:
        if col in ['crime_index']:
            # Pour ces métriques, une valeur plus basse est meilleure, donc inverser
            max_val = df[col].max()
            normalized_df[f"{col}_norm"] = 10 - (df[col] / max_val * 10)
        else:
            # Pour ces métriques, une valeur plus élevée est meilleure
            max_val = df[col].max()
            normalized_df[f"{col}_norm"] = df[col] / max_val * 10
    
    normalized_columns = [f"{col}_norm" for col in columns]
    
    # Ajouter chaque quartier au radar
    for neighborhood in neighborhoods:
        neighborhood_data = normalized_df[normalized_df['neighborhood_name'] == neighborhood]
        
        if neighborhood_data.empty:
            continue
        
        values = [neighborhood_data[col].iloc[0] for col in normalized_columns]
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name=neighborhood
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )
        ),
        title="Comparaison des quartiers",
        height=500
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Ajouter une explication
    st.caption("""
    *Note: Les valeurs sont normalisées sur une échelle de 0 à 10 pour permettre la comparaison.
    Pour l'indice de criminalité, les valeurs sont inversées (une valeur plus élevée indique une meilleure sécurité).*
    """)

def display_bar_comparison(df: pd.DataFrame, neighborhoods: List[str], 
                          metrics: List[Tuple[str, str]], selected_metrics: List[str]) -> None:
    """
    Affiche une comparaison en barres des quartiers.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
        neighborhoods (List[str]): Liste des quartiers sélectionnés
        metrics (List[Tuple[str, str]]): Liste des métriques disponibles (nom, colonne)
        selected_metrics (List[str]): Liste des métriques sélectionnées
    """
    # Créer un graphique en barres pour chaque métrique sélectionnée
    for metric_name, metric_col in metrics:
        if metric_name not in selected_metrics:
            continue
        
        # Créer le graphique
        fig = px.bar(
            df,
            x='neighborhood_name',
            y=metric_col,
            title=f"Comparaison des quartiers - {metric_name}",
            labels={
                'neighborhood_name': 'Quartier',
                metric_col: metric_name
            },
            height=400,
            color='neighborhood_name'
        )
        
        # Personnaliser le graphique
        fig.update_layout(
            xaxis_title="Quartier",
            yaxis_title=metric_name,
            showlegend=False,
            font=dict(family="Arial", size=12)
        )
        
        # Afficher le graphique
        st.plotly_chart(fig, use_container_width=True)

def display_value_index(df: pd.DataFrame, neighborhoods: List[str]) -> None:
    """
    Affiche un indice de rapport qualité-prix pour les quartiers.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données des quartiers
        neighborhoods (List[str]): Liste des quartiers sélectionnés
    """
    st.subheader("Indice de rapport qualité-prix")
    
    # Calculer un indice de qualité
    value_df = df.copy()
    
    # Calculer un score de qualité (moyenne des scores de marchabilité, écoles, et sécurité)
    value_df['quality_score'] = (
        value_df['walkability_score'] + 
        value_df['school_rating'] + 
        (10 - value_df['crime_index'])  # Inverser l'indice de criminalité
    ) / 3
    
    # Calculer un indice de rapport qualité-prix (plus élevé = meilleur rapport)
    value_df['value_index'] = value_df['quality_score'] / value_df['price_per_sqft'] * 10000
    
    # Trier par rapport qualité-prix
    value_df = value_df.sort_values('value_index', ascending=False)
    
    # Créer le graphique
    fig = px.bar(
        value_df,
        x='neighborhood_name',
        y='value_index',
        title="Indice de rapport qualité-prix par quartier",
        labels={
            'neighborhood_name': 'Quartier',
            'value_index': 'Indice qualité-prix'
        },
        height=400,
        color='value_index',
        color_continuous_scale='viridis'
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Quartier",
        yaxis_title="Indice qualité-prix",
        coloraxis_showscale=False,
        font=dict(family="Arial", size=12)
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Ajouter une explication
    st.markdown("""
    **À propos de l'indice qualité-prix:**
    
    Cet indice représente le rapport entre la qualité de vie dans un quartier (basée sur la marchabilité, 
    les écoles et la sécurité) et le prix au mètre carré. Un indice plus élevé indique un meilleur rapport 
    qualité-prix, ce qui peut aider à identifier des opportunités intéressantes.
    
    *Note: Cet indice est calculé à titre indicatif et ne prend pas en compte tous les facteurs qui peuvent 
    influencer la valeur réelle d'un bien immobilier.*
    """)
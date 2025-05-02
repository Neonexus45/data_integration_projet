#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Composant pour afficher les tendances de prix au fil du temps
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

def display_price_trends(db_connector, filter_params: Dict) -> None:
    """
    Affiche les tendances de prix au fil du temps.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.header("Tendances de prix immobiliers")
    
    # Options d'affichage
    col1, col2 = st.columns(2)
    
    with col1:
        # Type de prix à afficher
        price_type = st.selectbox(
            "Type de prix",
            options=["Prix moyen", "Prix médian"],
            index=0
        )
        
        price_column = "avg_price" if price_type == "Prix moyen" else "median_price"
    
    with col2:
        # Méthode d'agrégation temporelle
        time_agg = st.selectbox(
            "Agrégation temporelle",
            options=["Mensuelle", "Trimestrielle", "Annuelle"],
            index=0
        )
    
    # Récupération des données
    df = db_connector.get_price_trends(filter_params)
    
    if df.empty:
        st.warning("Aucune donnée disponible pour les filtres sélectionnés.")
        return
    
    # Préparation des données pour l'affichage
    df['date'] = pd.to_datetime(df['date'])
    
    # Agrégation des données selon la sélection
    if time_agg == "Trimestrielle":
        df['quarter'] = df['date'].dt.to_period('Q').astype(str)
        agg_df = df.groupby(['region_name', 'quarter']).agg(
            avg_price=('avg_price', 'mean'),
            median_price=('median_price', 'mean'),
            transaction_count=('transaction_count', 'sum'),
            date=('date', lambda x: x.iloc[0])  # Prendre une date représentative
        ).reset_index()
        x_column = 'quarter'
        x_title = 'Trimestre'
    elif time_agg == "Annuelle":
        df['year'] = df['date'].dt.year
        agg_df = df.groupby(['region_name', 'year']).agg(
            avg_price=('avg_price', 'mean'),
            median_price=('median_price', 'mean'),
            transaction_count=('transaction_count', 'sum'),
            date=('date', lambda x: x.iloc[0])  # Prendre une date représentative
        ).reset_index()
        x_column = 'year'
        x_title = 'Année'
    else:  # Mensuelle
        agg_df = df.copy()
        # Formater la date pour l'affichage
        agg_df['month_year'] = agg_df['date'].dt.strftime('%b %Y')
        x_column = 'month_year'
        x_title = 'Mois'
    
    # Graphique des tendances de prix
    display_price_trend_chart(agg_df, price_column, price_type, x_column, x_title)
    
    # Graphique du volume de transactions
    display_transaction_volume_chart(agg_df, x_column, x_title)
    
    # Graphique des variations saisonnières
    display_seasonal_patterns(db_connector, filter_params)

def display_price_trend_chart(df: pd.DataFrame, price_column: str, price_type: str, 
                            x_column: str, x_title: str) -> None:
    """
    Affiche le graphique des tendances de prix.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données de prix
        price_column (str): Colonne contenant les prix à utiliser
        price_type (str): Type de prix (pour le titre)
        x_column (str): Colonne à utiliser pour l'axe X
        x_title (str): Titre de l'axe X
    """
    st.subheader(f"Évolution du {price_type.lower()} au fil du temps")
    
    # Déterminer si nous avons plusieurs régions
    regions = df['region_name'].unique()
    multi_region = len(regions) > 1
    
    if multi_region:
        # Ligne de tendance pour chaque région
        fig = px.line(
            df,
            x=x_column,
            y=price_column,
            color='region_name',
            markers=True,
            title=f"Évolution du {price_type.lower()} par région",
            labels={
                price_column: f"{price_type} (€)",
                x_column: x_title,
                'region_name': 'Région'
            },
            height=500
        )
    else:
        # Tendance pour une seule région avec ligne de tendance
        region_name = regions[0]
        
        # Créer un index numérique pour la régression
        df = df.sort_values('date')
        df['index'] = range(len(df))
        
        # Calculer la ligne de tendance
        from scipy import stats
        slope, intercept, r_value, p_value, std_err = stats.linregress(df['index'], df[price_column])
        df['trend_line'] = intercept + slope * df['index']
        
        # Créer le graphique
        fig = go.Figure()
        
        # Ajouter la ligne principale
        fig.add_trace(
            go.Scatter(
                x=df[x_column],
                y=df[price_column],
                mode='lines+markers',
                name=f"{price_type}",
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8)
            )
        )
        
        # Ajouter la ligne de tendance
        fig.add_trace(
            go.Scatter(
                x=df[x_column],
                y=df['trend_line'],
                mode='lines',
                name="Tendance",
                line=dict(color='red', width=2, dash='dash')
            )
        )
        
        # Configurer le layout
        fig.update_layout(
            title=f"Évolution du {price_type.lower()} - {region_name}",
            xaxis_title=x_title,
            yaxis_title=f"{price_type} (€)",
            height=500,
            hovermode="x unified"
        )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title=f"{price_type} (€)",
        font=dict(family="Arial", size=12),
        hovermode="x unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Calculer les variations de prix
    if len(df) > 1:
        # Calcul de la variation par rapport au début de la période
        first_period = df.iloc[0][price_column]
        last_period = df.iloc[-1][price_column]
        
        variation = (last_period - first_period) / first_period * 100
        
        # Créer deux colonnes pour les métriques
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label=f"{price_type} début de période",
                value=f"{first_period:,.0f} €"
            )
        
        with col2:
            st.metric(
                label=f"{price_type} fin de période",
                value=f"{last_period:,.0f} €"
            )
        
        with col3:
            st.metric(
                label="Variation",
                value=f"{variation:.1f}%",
                delta=f"{variation:.1f}%"
            )

def display_transaction_volume_chart(df: pd.DataFrame, x_column: str, x_title: str) -> None:
    """
    Affiche le graphique du volume de transactions.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données de transactions
        x_column (str): Colonne à utiliser pour l'axe X
        x_title (str): Titre de l'axe X
    """
    st.subheader("Volume de transactions")
    
    # Déterminer si nous avons plusieurs régions
    regions = df['region_name'].unique()
    multi_region = len(regions) > 1
    
    if multi_region:
        # Histogramme pour chaque région
        fig = px.bar(
            df,
            x=x_column,
            y='transaction_count',
            color='region_name',
            title="Volume de transactions par région",
            labels={
                'transaction_count': 'Nombre de transactions',
                x_column: x_title,
                'region_name': 'Région'
            },
            barmode='group',
            height=400
        )
    else:
        # Histogramme pour une seule région
        region_name = regions[0]
        
        fig = px.bar(
            df,
            x=x_column,
            y='transaction_count',
            title=f"Volume de transactions - {region_name}",
            labels={
                'transaction_count': 'Nombre de transactions',
                x_column: x_title
            },
            color='transaction_count',
            color_continuous_scale='Viridis',
            height=400
        )
        fig.update_layout(coloraxis_showscale=False)
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title="Nombre de transactions",
        font=dict(family="Arial", size=12),
        hovermode="x unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)

def display_seasonal_patterns(db_connector, filter_params: Dict) -> None:
    """
    Affiche les patterns saisonniers des prix.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.subheader("Variations saisonnières")
    
    # Récupération des données
    df = db_connector.get_seasonal_patterns(filter_params)
    
    if df.empty:
        st.warning("Aucune donnée disponible sur les variations saisonnières.")
        return
    
    # Préparation des données
    # Convertir le mois en nom de mois
    month_names = [
        "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
        "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
    ]
    df['month_name'] = df['month'].apply(lambda x: month_names[int(x)-1])
    
    # Graphique des indices saisonniers
    fig = go.Figure()
    
    # Ajouter les lignes pour chaque indice
    fig.add_trace(
        go.Scatter(
            x=df['month_name'],
            y=df['price_index'],
            mode='lines+markers',
            name="Indice de prix",
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['month_name'],
            y=df['transaction_volume_index'],
            mode='lines+markers',
            name="Indice de volume",
            line=dict(color='#ff7f0e', width=3),
            marker=dict(size=8)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['month_name'],
            y=df['days_on_market_index'],
            mode='lines+markers',
            name="Indice de jours sur le marché",
            line=dict(color='#2ca02c', width=3),
            marker=dict(size=8)
        )
    )
    
    # Ajouter une ligne de référence à 1.0
    fig.add_hline(
        y=1.0,
        line_dash="dash",
        line_color="gray",
        annotation_text="Référence (1.0)",
        annotation_position="bottom right"
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        title="Indices saisonniers du marché immobilier",
        xaxis_title="Mois",
        yaxis_title="Indice saisonnier",
        xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(month_names))),
            ticktext=month_names
        ),
        font=dict(family="Arial", size=12),
        hovermode="x unified",
        height=500
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Ajouter une explication des indices
    with st.expander("À propos des indices saisonniers"):
        st.markdown("""
        **Interprétation des indices saisonniers:**
        
        - **Indice de prix**: Représente la variation saisonnière des prix par rapport à la moyenne annuelle. 
          Un indice de 1.05 signifie que les prix sont 5% plus élevés que la moyenne annuelle pour ce mois.
        
        - **Indice de volume**: Représente la variation saisonnière du nombre de transactions.
          Un indice de 0.9 signifie que le volume est 10% inférieur à la moyenne annuelle pour ce mois.
        
        - **Indice de jours sur le marché**: Représente la variation saisonnière de la durée moyenne des biens sur le marché.
          Un indice de 1.2 signifie que les biens restent 20% plus longtemps sur le marché que la moyenne annuelle pour ce mois.
        """)
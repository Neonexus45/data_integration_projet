#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Composant pour afficher l'importance des caractéristiques influençant les prix immobiliers
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
from typing import Dict, List, Tuple, Optional, Any

def display_feature_impact(db_connector, filter_params: Dict) -> None:
    """
    Affiche les visualisations sur l'importance des caractéristiques.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.header("Impact des caractéristiques sur les prix")
    
    # Récupération des données
    df_features = db_connector.get_feature_importance(filter_params)
    
    if df_features.empty:
        st.warning("Aucune donnée disponible sur l'importance des caractéristiques.")
        return
    
    # Afficher le graphique d'importance globale
    display_global_importance(df_features)
    
    # Afficher les variations régionales
    display_regional_variations(db_connector, df_features, filter_params)
    
    # Afficher les variations temporelles
    display_temporal_variations(db_connector, df_features, filter_params)

def display_global_importance(df: pd.DataFrame) -> None:
    """
    Affiche le graphique d'importance globale des caractéristiques.
    
    Args:
        df (pd.DataFrame): DataFrame contenant l'importance des caractéristiques
    """
    st.subheader("Importance globale des caractéristiques")
    
    # Sélection des caractéristiques par type
    feature_categories = df['feature_category'].unique()
    
    selected_categories = st.multiselect(
        "Catégories de caractéristiques",
        options=feature_categories,
        default=feature_categories
    )
    
    # Filtrer les données
    if selected_categories:
        filtered_df = df[df['feature_category'].isin(selected_categories)]
    else:
        filtered_df = df
    
    # Trier par importance
    filtered_df = filtered_df.sort_values('global_importance_score', ascending=True)
    
    # Créer le graphique horizontal d'importance
    fig = px.bar(
        filtered_df,
        y='feature_name',
        x='global_importance_score',
        color='feature_category',
        orientation='h',
        title="Importance des caractéristiques sur le prix",
        labels={
            'feature_name': 'Caractéristique',
            'global_importance_score': 'Score d\'importance',
            'feature_category': 'Catégorie'
        },
        text='global_importance_score',
        height=500
    )
    
    # Formater les textes dans les barres
    fig.update_traces(texttemplate='%{text:.3f}', textposition='outside')
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Score d'importance",
        yaxis_title="",
        font=dict(family="Arial", size=12),
        yaxis={'categoryorder':'total ascending'},
        hovermode="y unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Graphique de corrélation avec le prix
    display_correlation_chart(filtered_df)

def display_correlation_chart(df: pd.DataFrame) -> None:
    """
    Affiche le graphique de corrélation des caractéristiques avec le prix.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les corrélations
    """
    # Vérifier si la colonne de corrélation existe
    if 'correlation_with_price' not in df.columns:
        return
    
    # Trier par corrélation absolue
    df = df.copy()
    df['abs_correlation'] = df['correlation_with_price'].abs()
    df = df.sort_values('abs_correlation', ascending=False).head(10)
    
    # Créer le graphique
    fig = go.Figure()
    
    # Ajouter les barres pour les corrélations positives et négatives
    for i, row in df.iterrows():
        color = 'rgba(44, 160, 44, 0.8)' if row['correlation_with_price'] >= 0 else 'rgba(214, 39, 40, 0.8)'
        
        fig.add_trace(
            go.Bar(
                x=[row['correlation_with_price']],
                y=[row['feature_name']],
                orientation='h',
                marker_color=color,
                name=row['feature_name']
            )
        )
    
    # Ajouter une ligne verticale à 0
    fig.add_vline(
        x=0,
        line_width=1,
        line_dash="dash",
        line_color="gray"
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        title="Corrélation des caractéristiques avec le prix",
        xaxis_title="Coefficient de corrélation",
        yaxis_title="",
        showlegend=False,
        height=400,
        font=dict(family="Arial", size=12),
        xaxis=dict(range=[-1, 1]),
        yaxis={'categoryorder':'total ascending'}
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Explication des corrélations
    with st.expander("À propos des corrélations"):
        st.markdown("""
        **Interprétation des corrélations:**
        
        - Une corrélation **positive** (vert) indique que lorsque cette caractéristique augmente, le prix tend à augmenter également.
        - Une corrélation **négative** (rouge) indique que lorsque cette caractéristique augmente, le prix tend à diminuer.
        - La force de la corrélation est indiquée par la longueur de la barre:
          - 0.0 - 0.3: Corrélation faible
          - 0.3 - 0.7: Corrélation modérée
          - 0.7 - 1.0: Corrélation forte
        
        Il est important de noter que la corrélation n'implique pas nécessairement une causalité.
        """)

def display_regional_variations(db_connector, df_features: pd.DataFrame, filter_params: Dict) -> None:
    """
    Affiche les variations régionales de l'importance des caractéristiques.
    
    Args:
        db_connector: Connexion à la base de données
        df_features (pd.DataFrame): DataFrame contenant l'importance des caractéristiques
        filter_params (Dict): Paramètres de filtrage
    """
    st.subheader("Variations régionales de l'importance des caractéristiques")
    
    # Sélectionner les caractéristiques à visualiser
    top_features = df_features.sort_values('global_importance_score', ascending=False).head(5)
    feature_ids = top_features['feature_id'].tolist()
    
    # Récupérer les données de variation régionale
    df_regional = db_connector.get_regional_feature_variation(feature_ids, filter_params)
    
    if df_regional.empty:
        st.info("Aucune donnée disponible sur les variations régionales de l'importance des caractéristiques.")
        return
    
    # Créer un graphique de comparaison par région
    fig = px.bar(
        df_regional,
        x='region_name',
        y='importance_score',
        color='feature_name',
        barmode='group',
        title="Importance des caractéristiques par région",
        labels={
            'region_name': 'Région',
            'importance_score': 'Score d\'importance',
            'feature_name': 'Caractéristique'
        },
        height=500
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Région",
        yaxis_title="Score d'importance",
        font=dict(family="Arial", size=12),
        hovermode="x unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Créer une heatmap de l'importance relative
    if 'relative_importance' in df_regional.columns:
        display_regional_heatmap(df_regional)

def display_regional_heatmap(df: pd.DataFrame) -> None:
    """
    Affiche une heatmap de l'importance relative des caractéristiques par région.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les variations régionales
    """
    # Créer un tableau croisé pour la heatmap
    pivot_df = df.pivot_table(
        values='relative_importance',
        index='feature_name',
        columns='region_name',
        aggfunc='first'
    )
    
    # Normaliser les valeurs pour chaque caractéristique
    for feature in pivot_df.index:
        feature_mean = pivot_df.loc[feature].mean()
        pivot_df.loc[feature] = pivot_df.loc[feature] / feature_mean
    
    # Créer la heatmap
    fig = px.imshow(
        pivot_df,
        title="Importance relative des caractéristiques par région",
        labels=dict(
            x="Région",
            y="Caractéristique",
            color="Importance relative"
        ),
        color_continuous_scale="RdBu_r",
        zmin=0.5,
        zmax=1.5,
        height=400
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        font=dict(family="Arial", size=12),
        coloraxis_colorbar=dict(
            title="Importance<br>relative",
            thicknessmode="pixels", thickness=20,
            lenmode="pixels", len=300,
            titleside="right",
            ticks="outside"
        )
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Explication de l'importance relative
    with st.expander("À propos de l'importance relative"):
        st.markdown("""
        **Interprétation de l'importance relative:**
        
        Cette heatmap montre comment l'importance de chaque caractéristique varie selon la région, par rapport à la moyenne de cette caractéristique.
        
        - **Valeur > 1.0 (rouge)**: La caractéristique est plus importante dans cette région que la moyenne.
        - **Valeur = 1.0 (blanc)**: La caractéristique a une importance moyenne dans cette région.
        - **Valeur < 1.0 (bleu)**: La caractéristique est moins importante dans cette région que la moyenne.
        
        Par exemple, si la "surface habitable" a une valeur de 1.2 dans une région, cela signifie qu'elle est 20% plus importante pour déterminer le prix dans cette région que dans l'ensemble des régions.
        """)

def display_temporal_variations(db_connector, df_features: pd.DataFrame, filter_params: Dict) -> None:
    """
    Affiche les variations temporelles de l'importance des caractéristiques.
    
    Args:
        db_connector: Connexion à la base de données
        df_features (pd.DataFrame): DataFrame contenant l'importance des caractéristiques
        filter_params (Dict): Paramètres de filtrage
    """
    st.subheader("Évolution temporelle de l'importance des caractéristiques")
    
    # Sélectionner les caractéristiques à visualiser
    top_features = df_features.sort_values('global_importance_score', ascending=False).head(5)
    feature_ids = top_features['feature_id'].tolist()
    
    # Récupérer les données de variation temporelle
    df_temporal = db_connector.get_temporal_feature_variation(feature_ids, filter_params)
    
    if df_temporal.empty:
        st.info("Aucune donnée disponible sur l'évolution temporelle de l'importance des caractéristiques.")
        return
    
    # Créer une colonne de période (année-trimestre)
    df_temporal['period'] = df_temporal.apply(
        lambda row: f"{row['period_year']}-Q{row['period_quarter']}", 
        axis=1
    )
    
    # Créer un graphique des tendances temporelles
    fig = px.line(
        df_temporal,
        x='period',
        y='importance_score',
        color='feature_name',
        markers=True,
        title="Évolution de l'importance des caractéristiques au fil du temps",
        labels={
            'period': 'Période',
            'importance_score': 'Score d\'importance',
            'feature_name': 'Caractéristique'
        },
        height=500
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        xaxis_title="Période",
        yaxis_title="Score d'importance",
        font=dict(family="Arial", size=12),
        hovermode="x unified"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Interprétation
    st.markdown("""
    **Interprétation des tendances temporelles:**
    
    Ce graphique montre comment l'importance des principales caractéristiques évolue au fil du temps. 
    Des changements significatifs peuvent indiquer des évolutions dans les préférences des acheteurs 
    ou des changements structurels dans le marché immobilier.
    """)
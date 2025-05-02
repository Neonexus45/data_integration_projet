#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Composant pour afficher les prédictions de prix avec intervalles de confiance
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
from typing import Dict, List, Tuple, Optional, Any

def display_predictions(db_connector, filter_params: Dict) -> None:
    """
    Affiche les prédictions de prix et leurs intervalles de confiance.
    
    Args:
        db_connector: Connexion à la base de données
        filter_params (Dict): Paramètres de filtrage
    """
    st.header("Prédictions de prix immobiliers")
    
    # Récupération des données des modèles
    df_models = db_connector.get_prediction_models()
    
    if df_models.empty:
        st.warning("Aucun modèle de prédiction disponible.")
        return
    
    # Afficher les informations sur les modèles
    display_model_info(df_models)
    
    # Récupération des prédictions
    df_predictions = db_connector.get_predictions(filter_params)
    
    if df_predictions.empty:
        st.warning("Aucune prédiction disponible pour les filtres sélectionnés.")
        return
    
    # Afficher les prédictions avec intervalles de confiance
    display_prediction_intervals(df_predictions)
    
    # Afficher les contributions des caractéristiques
    display_feature_contributions(df_predictions)

def display_model_info(df_models: pd.DataFrame) -> None:
    """
    Affiche les informations sur les modèles de prédiction disponibles.
    
    Args:
        df_models (pd.DataFrame): DataFrame contenant les informations des modèles
    """
    st.subheader("Modèles de prédiction")
    
    # Extraire les métriques des modèles
    models_info = []
    
    for idx, row in df_models.iterrows():
        model_metrics = json.loads(row['model_metrics']) if isinstance(row['model_metrics'], str) else row['model_metrics']
        
        model_info = {
            'Nom du modèle': row['model_name'],
            'Version': row['model_version'],
            'Date d\'entraînement': row['training_date'].strftime('%d/%m/%Y') if hasattr(row['training_date'], 'strftime') else row['training_date'],
            'RMSE': model_metrics.get('rmse', 'N/A'),
            'MAE': model_metrics.get('mae', 'N/A'),
            'R²': model_metrics.get('r2', 'N/A'),
            'Actif': "✅" if row['is_active'] else "❌"
        }
        
        models_info.append(model_info)
    
    # Créer un DataFrame pour l'affichage
    df_display = pd.DataFrame(models_info)
    
    # Afficher le tableau
    st.dataframe(df_display, use_container_width=True)
    
    # Afficher les métriques clés du modèle actif
    active_models = df_models[df_models['is_active'] == 1]
    
    if not active_models.empty:
        model = active_models.iloc[0]
        model_metrics = json.loads(model['model_metrics']) if isinstance(model['model_metrics'], str) else model['model_metrics']
        
        st.subheader(f"Performance du modèle actif: {model['model_name']}")
        
        # Afficher les métriques clés
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="RMSE",
                value=f"{model_metrics.get('rmse', 0):,.0f} €",
                help="Erreur quadratique moyenne. Plus cette valeur est basse, meilleure est la prédiction."
            )
        
        with col2:
            st.metric(
                label="MAE",
                value=f"{model_metrics.get('mae', 0):,.0f} €",
                help="Erreur absolue moyenne. Représente l'erreur moyenne en termes absolus."
            )
        
        with col3:
            r2 = model_metrics.get('r2', 0)
            st.metric(
                label="R²",
                value=f"{r2:.2f}",
                delta=f"{(r2-0.75)*100:.1f}%" if r2 > 0.75 else f"{(r2-0.75)*100:.1f}%",
                delta_color="normal",
                help="Coefficient de détermination. Une valeur de 1.0 indique une prédiction parfaite."
            )
        
        with col4:
            exp_var = model_metrics.get('explained_variance', 0)
            st.metric(
                label="Variance expliquée",
                value=f"{exp_var:.2f}",
                help="Mesure la proportion de la variance dans les données qui est expliquée par le modèle."
            )

def display_prediction_intervals(df_predictions: pd.DataFrame) -> None:
    """
    Affiche les prédictions avec leurs intervalles de confiance.
    
    Args:
        df_predictions (pd.DataFrame): DataFrame contenant les prédictions
    """
    st.subheader("Prédictions de prix avec intervalles de confiance")
    
    # Trier par prix prédit
    df_sorted = df_predictions.sort_values('predicted_price', ascending=False)
    
    # Limiter à 10 propriétés pour la lisibilité
    df_display = df_sorted.head(10).copy()
    
    # Créer un identifiant lisible
    df_display['property_label'] = df_display.apply(
        lambda row: f"Propriété {row['property_id']}", 
        axis=1
    )
    
    # Créer le graphique
    fig = go.Figure()
    
    # Ajouter les intervalles de confiance
    for i, row in df_display.iterrows():
        fig.add_trace(
            go.Scatter(
                x=[row['prediction_interval_low'], row['prediction_interval_high']],
                y=[row['property_label'], row['property_label']],
                mode='lines',
                line=dict(width=8, color='rgba(49, 134, 204, 0.3)'),
                showlegend=False
            )
        )
    
    # Ajouter les points de prédiction
    fig.add_trace(
        go.Scatter(
            x=df_display['predicted_price'],
            y=df_display['property_label'],
            mode='markers',
            marker=dict(
                color='rgba(23, 77, 158, 0.9)',
                size=12,
                symbol='circle'
            ),
            name='Prix prédit'
        )
    )
    
    # Personnaliser le graphique
    fig.update_layout(
        title="Prédictions de prix et intervalles de confiance",
        xaxis_title="Prix (€)",
        yaxis_title="",
        height=500,
        font=dict(family="Arial", size=12),
        hovermode="closest"
    )
    
    # Afficher le graphique
    st.plotly_chart(fig, use_container_width=True)
    
    # Afficher les détails des prédictions
    with st.expander("Détails des prédictions"):
        # Préparer les données pour l'affichage
        details = []
        
        for idx, row in df_display.iterrows():
            confidence_percent = row['confidence_score'] * 100
            
            detail = {
                'ID Propriété': row['property_id'],
                'Prix prédit': f"{row['predicted_price']:,.0f} €",
                'Intervalle bas': f"{row['prediction_interval_low']:,.0f} €",
                'Intervalle haut': f"{row['prediction_interval_high']:,.0f} €",
                'Confiance': f"{confidence_percent:.1f}%",
                'Largeur intervalle': f"{(row['prediction_interval_high'] - row['prediction_interval_low']):,.0f} €"
            }
            
            details.append(detail)
        
        # Créer un DataFrame pour l'affichage
        details_df = pd.DataFrame(details)
        
        # Afficher le tableau
        st.dataframe(details_df, use_container_width=True)
        
        # Explication des intervalles de confiance
        st.markdown("""
        **À propos des intervalles de confiance:**
        
        Les intervalles de confiance représentent la plage de prix dans laquelle le prix réel devrait se trouver 
        avec une probabilité correspondant au score de confiance. Un intervalle plus large indique une plus grande
        incertitude dans la prédiction.
        """)

def display_feature_contributions(df_predictions: pd.DataFrame) -> None:
    """
    Affiche les contributions des caractéristiques aux prédictions.
    
    Args:
        df_predictions (pd.DataFrame): DataFrame contenant les prédictions
    """
    st.subheader("Contribution des caractéristiques aux prédictions")
    
    # Permettre à l'utilisateur de sélectionner une propriété
    property_ids = df_predictions['property_id'].unique()
    
    selected_property = st.selectbox(
        "Sélectionner une propriété",
        options=property_ids,
        format_func=lambda x: f"Propriété {x}"
    )
    
    # Filtrer les données pour la propriété sélectionnée
    property_data = df_predictions[df_predictions['property_id'] == selected_property].iloc[0]
    
    # Extraire les contributions des caractéristiques
    try:
        if isinstance(property_data['feature_contributions'], str):
            contributions = json.loads(property_data['feature_contributions'])
        else:
            contributions = property_data['feature_contributions']
        
        # Convertir en DataFrame
        contrib_df = pd.DataFrame({
            'Caractéristique': list(contributions.keys()),
            'Contribution': list(contributions.values())
        })
        
        # Trier par contribution
        contrib_df = contrib_df.sort_values('Contribution', ascending=True)
        
        # Créer le graphique de contributions
        fig = px.bar(
            contrib_df,
            y='Caractéristique',
            x='Contribution',
            orientation='h',
            title=f"Contribution des caractéristiques au prix - Propriété {selected_property}",
            labels={
                'Caractéristique': '',
                'Contribution': 'Contribution au prix prédit'
            },
            color='Contribution',
            color_continuous_scale='RdBu_r',
            height=400
        )
        
        # Personnaliser le graphique
        fig.update_layout(
            yaxis={'categoryorder':'array', 'categoryarray': contrib_df['Caractéristique']},
            font=dict(family="Arial", size=12),
            coloraxis_showscale=False
        )
        
        # Afficher le graphique
        st.plotly_chart(fig, use_container_width=True)
        
        # Afficher le prix prédit
        st.metric(
            label="Prix prédit",
            value=f"{property_data['predicted_price']:,.0f} €",
            help="Prix prédit par le modèle pour cette propriété"
        )
        
    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
        st.error(f"Erreur lors de l'analyse des contributions des caractéristiques: {e}")
    
    # Explication des contributions
    with st.expander("Comment interpréter les contributions des caractéristiques"):
        st.markdown("""
        **Interprétation des contributions des caractéristiques:**
        
        Ce graphique montre l'impact relatif de chaque caractéristique sur le prix prédit pour la propriété sélectionnée.
        
        - **Valeurs positives (rouge)**: Ces caractéristiques augmentent le prix prédit.
        - **Valeurs négatives (bleu)**: Ces caractéristiques diminuent le prix prédit.
        
        La somme de toutes ces contributions, plus une valeur de base, donne le prix final prédit pour la propriété.
        
        Par exemple, si la "surface habitable" a une contribution élevée, cela signifie que cette caractéristique 
        a un impact important sur le prix de cette propriété spécifique.
        """)
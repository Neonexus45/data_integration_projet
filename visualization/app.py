#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dashboard de visualisation des données immobilières

Ce script est le point d'entrée principal du dashboard Streamlit qui permet de visualiser
les données des datamarts MySQL contenant les analyses des prix immobiliers, les facteurs 
d'impact sur les prix, et les prédictions de prix.
"""

import os
import sys
import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import folium_static

# Import des modules personnalisés
from utils.db_connector import MySQLConnector
from components.price_map import display_price_map
from components.price_trends import display_price_trends
from components.feature_impact import display_feature_impact
from components.predictions import display_predictions
from components.neighborhood import display_neighborhood_comparison

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Tableau de bord immobilier",
    page_icon="🏡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Paramètres de la base de données
DB_CONFIG = {
    'host': 'localhost',
    'user': 'tatane', 
    'password': 'tatane',
    'database': 'immobilier_prediction_db'
}

# Fonction pour initialiser la session
def init_session_state():
    """Initialise les variables de session"""
    if 'db_connector' not in st.session_state:
        st.session_state.db_connector = MySQLConnector(**DB_CONFIG)
    
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()

def main():
    """Fonction principale du dashboard"""
    # Initialisation
    init_session_state()
    
    # Titre et description
    st.title("🏡 Analyse du Marché Immobilier")
    st.caption("Analyse des prix, tendances et prédictions du marché immobilier")
    
    # Sidebar pour les filtres
    st.sidebar.title("Filtres")
    
    # Récupérer les régions disponibles
    regions = st.session_state.db_connector.get_regions()
    selected_region = st.sidebar.selectbox(
        "Région",
        options=["Toutes"] + regions,
        index=0
    )
    
    # Récupérer les quartiers disponibles (en fonction de la région sélectionnée)
    neighborhoods = st.session_state.db_connector.get_neighborhoods(selected_region if selected_region != "Toutes" else None)
    selected_neighborhood = st.sidebar.selectbox(
        "Quartier",
        options=["Tous"] + neighborhoods,
        index=0
    )
    
    # Période d'analyse
    # Récupérer la plage de dates disponibles
    min_date, max_date = st.session_state.db_connector.get_date_range()
    date_range = st.sidebar.date_input(
        "Période d'analyse",
        value=[min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    
    # Assurer que deux dates sont sélectionnées
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date
    
    # Type de propriété
    property_types = st.session_state.db_connector.get_property_types()
    selected_property_type = st.sidebar.multiselect(
        "Type de propriété",
        options=property_types,
        default=property_types
    )
    
    # Bouton de rafraîchissement
    if st.sidebar.button("Rafraîchir les données"):
        st.session_state.last_refresh = datetime.now()
        st.experimental_rerun()
    
    # Affichage de la dernière mise à jour
    st.sidebar.caption(f"Dernière mise à jour: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    
    # Paramètres de filtrage pour les requêtes
    filter_params = {
        'region': selected_region if selected_region != "Toutes" else None,
        'neighborhood': selected_neighborhood if selected_neighborhood != "Tous" else None,
        'start_date': start_date,
        'end_date': end_date,
        'property_types': selected_property_type if selected_property_type else None
    }
    
    # Section principale avec onglets
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Carte des prix", 
        "Tendances de prix", 
        "Importance des caractéristiques", 
        "Prédictions", 
        "Comparaison des quartiers"
    ])
    
    # Onglet 1: Carte des prix
    with tab1:
        display_price_map(st.session_state.db_connector, filter_params)
    
    # Onglet 2: Tendances de prix
    with tab2:
        display_price_trends(st.session_state.db_connector, filter_params)
    
    # Onglet 3: Importance des caractéristiques
    with tab3:
        display_feature_impact(st.session_state.db_connector, filter_params)
    
    # Onglet 4: Prédictions
    with tab4:
        display_predictions(st.session_state.db_connector, filter_params)
    
    # Onglet 5: Comparaison des quartiers
    with tab5:
        display_neighborhood_comparison(st.session_state.db_connector, filter_params)

if __name__ == "__main__":
    main()
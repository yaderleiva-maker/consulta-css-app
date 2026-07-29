# services/bigquery.py
"""
Conexión y utilidades para BigQuery.
"""

from config import Config
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

@st.cache_resource
def get_client():
    """Obtener cliente de BigQuery (cacheado para reutilizar)"""
    try:
        return Config.get_bigquery_client()
    except Exception as e:
        st.error(f"Error conectando a BigQuery: {e}")
        raise


def ejecutar_query(query, params=None):
    """
    Ejecutar una consulta en BigQuery y devolver un DataFrame.
    Args:
        query (str): Consulta SQL
        params (list): Lista de parámetros para la consulta
    Returns:
        pd.DataFrame: Resultado de la consulta
    """
    client = get_client()
    
    try:
        if params:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        p["name"], 
                        p["type"], 
                        p["value"]
                    ) for p in params
                ]
            )
            job = client.query(query, job_config=job_config)
        else:
            job = client.query(query)
        
        return job.to_dataframe()
    
    except GoogleAPIError as e:
        st.error(f"Error en BigQuery: {e}")
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"Error inesperado: {e}")
        return pd.DataFrame()


def leer_sql(nombre_archivo):
    """
    Leer un archivo SQL desde la carpeta sql/
    """
    import os
    path = f"sql/{nombre_archivo}.sql"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""

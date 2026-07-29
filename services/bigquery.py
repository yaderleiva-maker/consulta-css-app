# services/bigquery.py
"""
Conexión y utilidades para BigQuery.
Usa st.secrets para autenticación (compatible con Streamlit Cloud).
"""

import os
import json
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

# ============================================================
# CONFIGURACIÓN
# ============================================================

# Leer project_id desde secrets o variable de entorno
PROJECT_ID = st.secrets.get("gcp", {}).get("project_id", os.getenv("PROJECT_ID", "proyecto-css-panama"))
DATASET = "nexo_people"

# ============================================================
# CLIENTE DE BIGQUERY (cacheado)
# ============================================================

@st.cache_resource
def get_client():
    """
    Obtener cliente de BigQuery usando credenciales de st.secrets.
    """
    try:
        # ---------- MODO 1: Usar st.secrets (Streamlit Cloud) ----------
        if "gcp_service_account" in st.secrets:
            # Las credenciales están en secrets.toml (formato TOML)
            creds_dict = dict(st.secrets["gcp_service_account"])
            
            # Convertir claves a minúsculas si es necesario
            if "private_key_id" in creds_dict:
                # Ya está en el formato correcto
                pass
            
            # Crear credenciales desde el dict
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict
            )
            
            return bigquery.Client(
                project=PROJECT_ID,
                credentials=credentials
            )
        
        # ---------- MODO 2: Usar variable de entorno (local) ----------
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            return bigquery.Client(project=PROJECT_ID)
        
        # ---------- MODO 3: Fallback - credenciales por defecto ----------
        else:
            return bigquery.Client(project=PROJECT_ID)
    
    except Exception as e:
        st.error(f"❌ Error conectando a BigQuery: {e}")
        st.error(f"📋 Detalles: Asegúrate de tener 'gcp_service_account' en secrets.toml")
        st.stop()


# ============================================================
# FUNCIONES DE UTILIDAD
# ============================================================

def ejecutar_query(query, params=None):
    """
    Ejecutar una consulta en BigQuery y devolver un DataFrame.
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
        st.error(f"❌ Error en BigQuery: {e}")
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"❌ Error inesperado en BigQuery: {e}")
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
        # Si no existe, retornamos cadena vacía (no warning para no saturar)
        return ""


def probar_conexion():
    """
    Prueba rápida para verificar la conexión a BigQuery.
    """
    try:
        query = "SELECT 1 AS test"
        df = ejecutar_query(query)
        if not df.empty and df.iloc[0]['test'] == 1:
            st.success("✅ Conexión a BigQuery exitosa")
            return True
        else:
            st.error("❌ La conexión a BigQuery devolvió un resultado inesperado")
            return False
    except Exception as e:
        st.error(f"❌ Error en la prueba de conexión: {e}")
        return False

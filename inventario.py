import streamlit as st
import pandas as pd
import uuid
from datetime import datetime

# Intentar importar BigQuery, pero manejar error gracefulmente
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_DISPONIBLE = True
except ImportError:
    BIGQUERY_DISPONIBLE = False
    st.warning("⚠️ BigQuery no está disponible. Modo demostración.")

PROJECT_ID = "proyecto-css-panama"
DATASET = "inventario"

def get_bq_client():
    if not BIGQUERY_DISPONIBLE:
        return None
    
    try:
        if "BIGQUERY_CREDENTIALS" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["BIGQUERY_CREDENTIALS"]
            )
            return bigquery.Client(credentials=creds, project=PROJECT_ID)
        else:
            return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Error conectando a BigQuery: {e}")
        return None

def run(usuario):
    st.title("📦 NEXO STOCK")
    st.subheader("Carga Masiva de Productos")
    st.write(f"👤 Usuario: {usuario}")
    
    # Verificar disponibilidad
    if not BIGQUERY_DISPONIBLE:
        st.error("❌ BigQuery no está instalado. Verifica requirements.txt")
        return
    
    client = get_bq_client()
    if client is None:
        st.error("❌ No se pudo conectar a BigQuery. Verifica secrets.")
        return
    
    # ... resto de tu código de carga (el que ya tenías)

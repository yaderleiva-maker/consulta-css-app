import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# -------------------------------
# CONFIGURACIÓN BIGQUERY
# -------------------------------
PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"
TABLE_ASESORES = f"{PROJECT_ID}.{DATASET_HOPSA}.asesores"
TABLE_REPORTE = f"{PROJECT_ID}.{DATASET_HOPSA}.reporte_diario"
TABLE_MANUAL = f"{PROJECT_ID}.{DATASET_HOPSA}.datos_manuales"

# -------------------------------
# INICIALIZAR CLIENTE BQ
# -------------------------------
@st.cache_resource
def init_bq_client():
    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        return bigquery.Client(project=PROJECT_ID)

# -------------------------------
# FUNCIÓN PARA ASEGURAR TABLAS
# -------------------------------
def asegurar_tablas():
    """Crea las tablas si no existen"""
    client = init_bq_client()
    
    queries = [
        f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_ASESORES}` (
            id_asesor STRING,
            nombre STRING,
            supervisor STRING,
            fecha_actualizacion TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_REPORTE}` (
            id_asesor STRING,
            nombre STRING,
            supervisor STRING,
            ventas FLOAT64,
            cierres INT64,
            llamadas INT64,
            cantidad_cotizaciones INT64,
            leads INT64,
            nps INT64,
            pra_90 FLOAT64,
            asistencia FLOAT64,
            conversion FLOAT64,
            ticket_promedio FLOAT64,
            fecha DATE,
            mes STRING,
            dia STRING,
            sem_mes INT64,
            sem_año INT64,
            año INT64,
            fecha_creacion TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_MANUAL}` (
            id_asesor STRING,
            fecha DATE,
            leads INT64,
            nps INT64,
            pra_90 FLOAT64,
            asistencia FLOAT64,
            observaciones STRING,
            actualizado_por STRING,
            timestamp TIMESTAMP
        )
        """
    ]
    
    for query in queries:
        try:
            client.query(query).result()
        except Exception as e:
            st.error(f"Error creando tabla: {e}")
            return False
    return True

# -------------------------------
# 1. ACTUALIZAR AGENTES
# -------------------------------
def actualizar_agentes():
    st.subheader("👥 Actualizar tabla de Agentes (HEXAGON)")
    
    st.markdown("""
    **Formato CSV requerido:**
    ```csv
    id_asesor,nombre,supervisor
    MARIA,Maria López,Supervisor A
    JUAN,Juan Pérez,Supervisor B
    101,Celeste Menzane,Celeste Menzane

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
DATASET_HOPSA = "hopsa"  # Corregido: dataset hopsa
TABLE_ASESORES = f"{PROJECT_ID}.{DATASET_HOPSA}.asesores"  # Sin prefijo hopsa_
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
    
    # Tabla de asesores
    query_asesores = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_ASESORES}` (
        id_asesor STRING,
        nombre STRING,
        supervisor STRING,
        fecha_actualizacion TIMESTAMP
    )
    """
    
    # Tabla de reportes
    query_reportes = f"""
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
    """
    
    # Tabla de datos manuales
    query_manual = f"""
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
    
    try:
        client.query(query_asesores).result()
        client.query(query_reportes).result()
        client.query(query_manual).result()
        return True
    except Exception as e:
        st.error(f"Error creando tablas: {e}")
        return False

# -------------------------------
# 1. ACTUALIZAR AGENTES (CORREGIDO)
# -------------------------------
def actualizar_agentes():
    st.subheader("👥 Actualizar tabla de Agentes (HEXAGON)")
    st.markdown("""
    ### 📋 Instrucciones:
    1. Descarga la plantilla o crea un archivo CSV
    2. Columnas requeridas: `id_asesor` (texto/string), `nombre`, `supervisor`
    3. Guarda como CSV
    
    **Ejemplo:**
    ```csv
    id_asesor,nombre,supervisor
    MARIA,Maria López,Supervisor A
    JUAN,Juan Pérez,Supervisor B
    101,Celeste Menzane,Celeste Menzane

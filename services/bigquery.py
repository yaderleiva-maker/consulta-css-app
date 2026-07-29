# services/bigquery.py
"""
Conexión y utilidades para BigQuery.
NO depende de config.py. Usa variables de entorno directamente.
"""

# services/bigquery.py
import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# ============================================================
# CONFIGURACIÓN (desde variables de entorno)
# ============================================================

PROJECT_ID = os.getenv("PROJECT_ID", "tu-proyecto-id")
DATASET = "nexo_people"
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ============================================================
# CLIENTE DE BIGQUERY (cacheado)
# ============================================================

@st.cache_resource
def get_client():
    """
    Obtener cliente de BigQuery (cacheado para reutilizar).
    Usa las credenciales de la variable de entorno.
    """
    try:
        # Si hay credenciales definidas, usarlas
        if CREDENTIALS_PATH:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        
        return bigquery.Client(project=PROJECT_ID)
    
    except Exception as e:
        st.error(f"❌ Error conectando a BigQuery: {e}")
        st.stop()  # Detiene la ejecución si no hay conexión


# ============================================================
# FUNCIONES DE UTILIDAD
# ============================================================

def ejecutar_query(query, params=None):
    """
    Ejecutar una consulta en BigQuery y devolver un DataFrame.
    
    Args:
        query (str): Consulta SQL
        params (list): Lista de parámetros para la consulta
                      Cada parámetro es un dict con: name, type, value
                      Ejemplo: [{"name": "id_empleado", "type": "STRING", "value": "EMP001"}]
    
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
        st.error(f"❌ Error en BigQuery: {e}")
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"❌ Error inesperado: {e}")
        return pd.DataFrame()


def leer_sql(nombre_archivo):
    """
    Leer un archivo SQL desde la carpeta sql/
    
    Args:
        nombre_archivo (str): Nombre del archivo sin extensión .sql
    
    Returns:
        str: Contenido del archivo SQL, o cadena vacía si no existe.
    """
    import os
    path = f"sql/{nombre_archivo}.sql"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        st.warning(f"⚠️ Archivo SQL no encontrado: {nombre_archivo}.sql")
        return ""


def obtener_dataset():
    """Devuelve el nombre del dataset para construir consultas."""
    return DATASET


def obtener_tabla_completa(nombre_tabla):
    """Devuelve el nombre completo de una tabla (dataset.tabla)."""
    return f"`{PROJECT_ID}.{DATASET}.{nombre_tabla}`"


# ============================================================
# FUNCIÓN DE PRUEBA (para verificar la conexión)
# ============================================================

def probar_conexion():
    """Prueba rápida para verificar que la conexión a BigQuery funciona."""
    try:
        query = "SELECT 1 AS test"
        df = ejecutar_query(query)
        if not df.empty:
            st.success("✅ Conexión a BigQuery exitosa")
            return True
        else:
            st.error("❌ La conexión a BigQuery devolvió un resultado vacío")
            return False
    except Exception as e:
        st.error(f"❌ Error en la prueba de conexión: {e}")
        return False

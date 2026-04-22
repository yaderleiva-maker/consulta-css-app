import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"
TABLE_ASESORES = f"{PROJECT_ID}.{DATASET_HOPSA}.asesores"
TABLE_REPORTE = f"{PROJECT_ID}.{DATASET_HOPSA}.reporte_diario"
TABLE_MANUAL = f"{PROJECT_ID}.{DATASET_HOPSA}.datos_manuales"

@st.cache_resource
def init_bq_client():
    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return bigquery.Client(project=PROJECT_ID)

def cargar_agentes():
    try:
        client = init_bq_client()
        query = f"SELECT * FROM `{TABLE_ASESORES}`"
        df = client.query(query).to_dataframe()
        return df if not df.empty else None
    except:
        return None

def actualizar_agentes():
    st.subheader("Gestionar Agentes")
    st.write("Funcion actualizar_agentes OK")

def run(usuario):
    st.title("HOPSA - TEST")
    st.write("Paso 5: Funcion actualizar_agentes definida")
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir", "Reportes"])
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir":
        st.write("Subir informacion - pendiente")
    else:
        st.write("Reportes - pendiente")

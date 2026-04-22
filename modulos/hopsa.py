import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
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
    else:
        return bigquery.Client(project=PROJECT_ID)

def actualizar_agentes():
    st.subheader("👥 Actualizar Agentes")
    
    archivo = st.file_uploader("CSV de agentes", type=['csv'])
    
    if archivo:
        df = pd.read_csv(archivo)
        st.dataframe(df)
        
        if st.button("Guardar"):
            client = init_bq_client()
            df['fecha_actualizacion'] = datetime.datetime.now()
            
            job = client.load_table_from_dataframe(
                df, TABLE_ASESORES,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
            )
            job.result()
            st.success("Guardado!")

def subir_informacion():
    st.subheader("📂 Subir información")
    st.info("En construcción - Próximamente")
    st.write("Subirás ventas, llamadas y cotizaciones aquí")

def descargar_reportes():
    st.subheader("📥 Descargar reportes")
    st.info("En construcción - Próximamente")
    st.write("Aquí podrás descargar reportes históricos")

def run(usuario):
    st.title("🎯 HOPSA - Gestión de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    opcion = st.sidebar.radio(
        "Opciones",
        ["Actualizar Agentes", "Subir Información", "Descargar Reportes"]
    )
    
    if opcion == "Actualizar Agentes":
        actualizar_agentes()
    elif opcion == "Subir Información":
        subir_informacion()
    elif opcion == "Descargar Reportes":
        descargar_reportes()

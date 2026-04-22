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

def run(usuario):
    st.title("HOPSA - Gestion de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    menu = st.sidebar.radio("Menu", ["Agentes", "Subir Datos", "Reportes"])
    
    if menu == "Agentes":
        st.subheader("Gestion de Agentes")
        
        # Intentar cargar agentes existentes
        try:
            client = init_bq_client()
            query = f"SELECT * FROM `{TABLE_ASESORES}` LIMIT 10"
            df = client.query(query).to_dataframe()
            if not df.empty:
                st.info(f"Agentes actuales: {len(df)}")
                st.dataframe(df)
        except:
            st.warning("No hay agentes cargados")
        
        archivo = st.file_uploader("Subir archivo de agentes", type=['csv', 'xlsx'])
        if archivo:
            if archivo.name.endswith('.csv'):
                df_new = pd.read_csv(archivo)
            else:
                df_new = pd.read_excel(archivo)
            
            st.dataframe(df_new)
            
            if st.button("Guardar en BigQuery"):
                df_new['fecha_actualizacion'] = datetime.datetime.now()
                job = client.load_table_from_dataframe(
                    df_new, TABLE_ASESORES,
                    job_config=bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    )
                )
                job.result()
                st.success("Guardado!")
    
    elif menu == "Subir Datos":
        st.subheader("Subir Informacion del Dia")
        fecha = st.date_input("Fecha", datetime.date.today())
        st.info(f"Procesando datos para {fecha}")
        
        col1, col2 = st.columns(2)
        with col1:
            ventas = st.file_uploader("Ventas", type=['csv', 'xlsx'])
            llamadas = st.file_uploader("Llamadas", type=['csv'])
        with col2:
            cotizaciones = st.file_uploader("Cotizaciones", type=['csv', 'xlsx'])
        
        with st.expander("Datos Manuales"):
            leads_total = st.number_input("Leads totales", min_value=0, value=0)
            nps_total = st.number_input("NPS promedio", min_value=0, max_value=10, value=0)
        
        if st.button("Procesar"):
            st.success("Datos procesados (demo)")
    
    else:
        st.subheader("Descargar Reportes")
        fecha_ini = st.date_input("Desde", datetime.date.today() - datetime.timedelta(days=30))
        fecha_fin = st.date_input("Hasta", datetime.date.today())
        
        if st.button("Generar"):
            st.info(f"Reporte de {fecha_ini} a {fecha_fin}")
            st.download_button("Descargar CSV", "data,test\n1,2", "reporte.csv")

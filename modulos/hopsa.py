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
    st.subheader("👥 Gestionar Agentes")
    
    agentes = cargar_agentes()
    if agentes is not None:
        st.info(f"📋 {len(agentes)} agentes actualmente en sistema")
        st.dataframe(agentes, use_container_width=True)
    
    st.markdown("---")
    st.markdown("**Formato requerido:** id_asesor, nombre, supervisor, id_llamadas(opcional)")
    
    archivo = st.file_uploader("Subir archivo", type=['csv', 'xlsx'], key="upload_agentes")
    
    if archivo:
        try:
            if archivo.name.endswith('.csv'):
                df = pd.read_csv(archivo)
            else:
                df = pd.read_excel(archivo)
            
            if 'id_asesor' not in df.columns or 'nombre' not in df.columns:
                st.error("Faltan columnas: id_asesor, nombre")
                return
            
            if 'id_llamadas' not in df.columns:
                df['id_llamadas'] = df['id_asesor']
            if 'supervisor' not in df.columns:
                df['supervisor'] = ''
            
            st.success(f"✅ {len(df)} agentes cargados")
            st.dataframe(df.head(), use_container_width=True)
            
            if st.button("💾 Guardar en BigQuery"):
                client = init_bq_client()
                df['fecha_actualizacion'] = datetime.datetime.now()
                
                job = client.load_table_from_dataframe(
                    df, TABLE_ASESORES,
                    job_config=bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    )
                )
                job.result()
                st.success("✅ Agentes guardados")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

def descargar_reportes():
    st.subheader("📥 Descargar reportes")
    st.info("Funcion descargar_reportes - version simple")
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    if st.button("Generar reporte simple"):
        st.write(f"Rango: {fecha_inicio} a {fecha_fin}")
        st.success("Reporte generado (demo)")

def run(usuario):
    st.title("🎯 HOPSA - Gestion de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Informacion", "Reportes"])
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Informacion":
        st.info("En construccion - Subir informacion (proximamente)")
    else:
        descargar_reportes()

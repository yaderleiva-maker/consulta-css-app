import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# Configuración
PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"
TABLE_ASESORES = f"{PROJECT_ID}.{DATASET_HOPSA}.asesores"  # Tabla persistente
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

# -------------------------------
# CARGAR AGENTES DESDE BIGQUERY (persistente)
# -------------------------------
@st.cache_data(ttl=3600)  # Cache por 1 hora
def cargar_agentes_desde_bq():
    """Carga los agentes desde BigQuery (tabla persistente)"""
    client = init_bq_client()
    try:
        query = f"SELECT * FROM `{TABLE_ASESORES}` ORDER BY nombre"
        df = client.query(query).to_dataframe()
        return df if not df.empty else None
    except Exception as e:
        # La tabla puede no existir aún
        return None

def guardar_agentes_en_bq(df):
    """Guarda agentes en BigQuery (sobrescribe)"""
    client = init_bq_client()
    
    # Agregar fecha de actualización
    df['fecha_actualizacion'] = datetime.datetime.now()
    
    # Schema actualizado con id_llamadas
    schema = [
        bigquery.SchemaField("id_asesor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("nombre", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("supervisor", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_llamadas", "STRING", mode="NULLABLE"),  # NUEVO: alias para llamadas
        bigquery.SchemaField("fecha_actualizacion", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    job = client.load_table_from_dataframe(
        df, TABLE_ASESORES,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema
        )
    )
    job.result()
    st.success(f"✅ {len(df)} agentes guardados en BigQuery")

def actualizar_agentes():
    st.subheader("👥 Gestionar Agentes (HEXAGON)")
    
    # Mostrar agentes actuales (si existen)
    agentes_existentes = cargar_agentes_desde_bq()
    
    if agentes_existentes is not None and not agentes_existentes.empty:
        st.info(f"📋 {len(agentes_existentes)} agentes cargados actualmente")
        st.dataframe(agentes_existentes, use_container_width=True)
        
        if st.button("🔄 Actualizar/Reemplazar agentes"):
            st.session_state.recargar_agentes = True
            st.rerun()
    else:
        st.warning("⚠️ No hay agentes cargados. Sube un archivo para comenzar.")
    
    st.markdown("---")
    st.markdown("""
    ### 📋 Formato del archivo
    
    **Columnas requeridas:**
    - `id_asesor` - ID del sistema de ventas (ej: heyanez)
    - `nombre` - Nombre completo
    - `supervisor` - Nombre del supervisor
    
    **Columna opcional (recomendada):**
    - `id_llamadas` - ID alternativo para cruzar con reporte de llamadas (ej: hyañez)
    
    **Ejemplo:**
    ```csv
    id_asesor,nombre,supervisor,id_llamadas
    heyanez,Heydi Yanez,Supervisor A,hyañez
    jperez,Juan Pérez,Supervisor B,jperez
    mlopez,Maria Lopez,Supervisor C,m_lopez

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

def subir_informacion():
    st.subheader("📂 Subir informacion del dia")
    
    # Verificar agentes
    agentes = cargar_agentes()
    if agentes is None or agentes.empty:
        st.warning("⚠️ Primero carga los agentes")
        return
    
    fecha_reporte = st.date_input("Fecha del reporte", datetime.date.today())
    st.info(f"📋 {len(agentes)} agentes activos")
    
    st.markdown("---")
    st.markdown("### Archivos del dia")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        ventas_file = st.file_uploader("Ventas", type=['csv', 'xlsx'], key="ventas")
    with col2:
        llamadas_file = st.file_uploader("Llamadas", type=['csv'], key="llamadas")
    with col3:
        cotizaciones_file = st.file_uploader("Cotizaciones", type=['csv', 'xlsx'], key="cotizaciones")
    
    # Datos manuales simplificados
    st.markdown("---")
    st.markdown("### Datos manuales")
    
    with st.form("datos_form"):
        datos_manuales = []
        for _, row in agentes.iterrows():
            st.markdown(f"**{row['nombre']}**")
            col_a, col_b = st.columns(2)
            with col_a:
                leads = st.number_input("Leads", min_value=0, value=0, key=f"leads_{row['id_asesor']}")
            with col_b:
                nps = st.number_input("NPS", min_value=0, max_value=10, value=0, key=f"nps_{row['id_asesor']}")
            
            datos_manuales.append({
                "id_asesor": str(row['id_asesor']),
                "leads": leads,
                "nps": nps
            })
        
        submit = st.form_submit_button("Procesar")
    
    if submit:
        if not all([ventas_file, llamadas_file, cotizaciones_file]):
            st.error("❌ Faltan archivos")
            return
        
        try:
            # 1. Procesar ventas - SOLO LECTURA
            if ventas_file.name.endswith('.csv'):
                df_ventas = pd.read_csv(ventas_file)
            else:
                df_ventas = pd.read_excel(ventas_file)
            
            st.success(f"✅ Ventas: {len(df_ventas)} registros leidos")
            st.write("Columnas ventas:", list(df_ventas.columns))
            st.dataframe(df_ventas.head())
            
            # 2. Procesar llamadas - SOLO LECTURA
            df_llamadas = pd.read_csv(llamadas_file)
            st.success(f"✅ Llamadas: {len(df_llamadas)} registros leidos")
            st.write("Columnas llamadas:", list(df_llamadas.columns))
            st.dataframe(df_llamadas.head())
            
            # 3. Procesar cotizaciones - SOLO LECTURA
            if cotizaciones_file.name.endswith('.csv'):
                df_cotizaciones = pd.read_csv(cotizaciones_file)
            else:
                df_cotizaciones = pd.read_excel(cotizaciones_file)
            
            st.success(f"✅ Cotizaciones: {len(df_cotizaciones)} registros leidos")
            st.write("Columnas cotizaciones:", list(df_cotizaciones.columns))
            st.dataframe(df_cotizaciones.head())
            
            st.success("✅ Todos los archivos leidos correctamente")
            
        except Exception as e:
            st.error(f"Error al leer archivos: {e}")
            st.exception(e)

def descargar_reportes():
    st.subheader("📥 Descargar reportes")
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    if st.button("Generar reporte"):
        st.write(f"Rango: {fecha_inicio} a {fecha_fin}")
        st.success("Reporte generado (demo)")

def run(usuario):
    st.title("🎯 HOPSA - Gestion de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Informacion", "Reportes"])
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Informacion":
        subir_informacion()
    else:
        descargar_reportes()

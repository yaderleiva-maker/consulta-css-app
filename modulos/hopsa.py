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
        df = client.query(f"SELECT * FROM `{TABLE_ASESORES}`").to_dataframe()
        return df if not df.empty else None
    except:
        return None

def actualizar_agentes():
    st.subheader("👥 Agentes")
    
    agentes = cargar_agentes()
    if agentes is not None:
        st.dataframe(agentes, use_container_width=True)
    
    archivo = st.file_uploader("Subir CSV (id_asesor, nombre, supervisor, id_llamadas)", type=['csv'])
    
    if archivo:
        df = pd.read_csv(archivo)
        if st.button("Guardar"):
            client = init_bq_client()
            client.load_table_from_dataframe(
                df, TABLE_ASESORES,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            ).result()
            st.success("✅ Guardado")
            st.rerun()

def subir_informacion():
    st.subheader("📂 Subir datos")
    
    agentes = cargar_agentes()
    if agentes is None:
        st.warning("Primero carga los agentes")
        return
    
    fecha = st.date_input("Fecha", datetime.date.today())
    
    # Archivos
    ventas_file = st.file_uploader("Ventas (CSV)", type=['csv'])
    llamadas_file = st.file_uploader("Llamadas (CSV)", type=['csv'])
    cotizaciones_file = st.file_uploader("Cotizaciones (CSV)", type=['csv'])
    
    # Datos manuales simples
    st.markdown("---")
    leads_total = st.number_input("Leads totales del día", min_value=0, value=0)
    
    if st.button("Procesar"):
        if not all([ventas_file, llamadas_file, cotizaciones_file]):
            st.error("Faltan archivos")
            return
        
        try:
            # 1. Ventas
            df_ventas = pd.read_csv(ventas_file)
            df_ventas = df_ventas.rename(columns={'Vendedor': 'id_asesor'})
            ventas_agg = df_ventas.groupby('id_asesor').agg(
                ventas=('Venta', 'sum'),
                cierres=('Factura', 'count')
            ).reset_index()
            
            # 2. Llamadas - Cruce simple por Identificación
            df_llamadas = pd.read_csv(llamadas_file)
            mapeo = dict(zip(agentes['id_llamadas'].astype(str), agentes['id_asesor'].astype(str)))
            df_llamadas['id_asesor'] = df_llamadas['Identificación'].astype(str).map(mapeo)
            llamadas_agg = df_llamadas.groupby('id_asesor')['Llamadas'].sum().reset_index()
            
            # 3. Cotizaciones
            df_cotiz = pd.read_csv(cotizaciones_file)
            df_cotiz = df_cotiz.rename(columns={'Vendedor': 'id_asesor'})
            cotizaciones_agg = df_cotiz.groupby('id_asesor').size().reset_index(name='cotizaciones')
            
            # 4. Unir todo
            reporte = agentes.merge(ventas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
            
            # Rellenar ceros
            for col in ['ventas', 'cierres', 'Llamadas', 'cotizaciones']:
                if col in reporte.columns:
                    reporte[col] = reporte[col].fillna(0)
            
            # Agregar leads (mismo valor para todos por ahora)
            reporte['leads'] = leads_total
            
            # Calcular conversión
            reporte['conversion'] = reporte.apply(
                lambda r: 0 if r['leads'] == 0 else (r['cierres'] / r['leads']) * 100, axis=1
            ).round(2)
            
            # Fecha
            reporte['fecha'] = fecha
            
            # 5. Guardar
            client = init_bq_client()
            client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha}'").result()
            client.load_table_from_dataframe(reporte, TABLE_REPORTE).result()
            
            st.success(f"✅ Reporte del {fecha} guardado")
            
            # Mostrar resultado
            st.dataframe(reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion']], use_container_width=True)
            
        except Exception as e:
            st.error(f"Error: {e}")

def descargar_reportes():
    st.subheader("📥 Descargar")
    client = init_bq_client()
    
    fecha_ini = st.date_input("Desde", datetime.date.today() - datetime.timedelta(days=30))
    fecha_fin = st.date_input("Hasta", datetime.date.today())
    
    if st.button("Generar"):
        df = client.query(f"""
            SELECT fecha, nombre, supervisor, leads, cierres, conversion, ventas
            FROM `{TABLE_REPORTE}`
            WHERE fecha BETWEEN '{fecha_ini}' AND '{fecha_fin}'
            ORDER BY fecha DESC, nombre
        """).to_dataframe()
        
        if df.empty:
            st.warning("Sin datos")
        else:
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("Descargar CSV", csv, "reporte.csv")

def run(usuario):
    st.title("HOPSA")
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Datos", "Reportes"])
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Datos":
        subir_informacion()
    else:
        descargar_reportes()

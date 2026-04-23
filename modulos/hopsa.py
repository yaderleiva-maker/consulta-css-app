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
    st.markdown("""
    **Formato requerido (CSV o Excel):**
    - `id_asesor` - ID del sistema de ventas
    - `nombre` - Nombre completo
    - `supervisor` - Supervisor
    - `id_llamadas` - ID que aparece en columna 'Identificación' del archivo de llamadas
    """)
    
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
    
    # Cargar agentes
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
        ventas_file = st.file_uploader("📊 Ventas", type=['csv', 'xlsx'])
    with col2:
        llamadas_file = st.file_uploader("📞 Llamadas", type=['csv'])
    with col3:
        cotizaciones_file = st.file_uploader("📝 Cotizaciones", type=['csv', 'xlsx'])
    
    st.markdown("---")
    st.markdown("### Datos manuales")
    
    # Datos manuales simplificados
    with st.form("datos_form"):
        datos = []
        for _, row in agentes.iterrows():
            st.markdown(f"**{row['nombre']}**")
            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a:
                leads = st.number_input("Leads", min_value=0, value=0, key=f"l_{row['id_asesor']}")
            with col_b:
                nps = st.number_input("NPS", min_value=0, max_value=10, value=0, key=f"n_{row['id_asesor']}")
            with col_c:
                pra = st.number_input("PRA%", min_value=0, max_value=100, value=90, key=f"p_{row['id_asesor']}")
            with col_d:
                asis = st.selectbox("Asistencia", [100, 0], format_func=lambda x: "✅" if x == 100 else "❌", key=f"a_{row['id_asesor']}")
            datos.append({
                "id_asesor": str(row['id_asesor']),
                "leads": leads,
                "nps": nps,
                "pra_90": pra,
                "asistencia": asis
            })
            st.markdown("---")
        
        submit = st.form_submit_button("🚀 Procesar", type="primary")
    
    if submit:
        if not all([ventas_file, llamadas_file, cotizaciones_file]):
            st.error("❌ Faltan archivos")
            return
        
        try:
            client = init_bq_client()
            df_manual = pd.DataFrame(datos)
            
            # 1. VENTAS
            if ventas_file.name.endswith('.csv'):
                df_ventas = pd.read_csv(ventas_file)
            else:
                df_ventas = pd.read_excel(ventas_file)
            
            df_ventas = df_ventas.rename(columns={'Vendedor': 'id_asesor'})
            df_ventas['id_asesor'] = df_ventas['id_asesor'].astype(str).str.strip()
            
            ventas_agg = df_ventas.groupby('id_asesor').agg(
                ventas=('Venta', 'sum'),
                cierres=('Factura', 'count')
            ).reset_index()
            
            # 2. LLAMADAS - SIMPLE Y DIRECTO
            df_llamadas = pd.read_csv(llamadas_file)
            
            # Mapeo: Identificación -> id_asesor
            mapeo = dict(zip(agentes['id_llamadas'].astype(str), agentes['id_asesor'].astype(str)))
            df_llamadas['id_asesor'] = df_llamadas['Identificación'].astype(str).map(mapeo)
            df_llamadas['id_asesor'] = df_llamadas['id_asesor'].fillna(df_llamadas['Identificación'])
            
            llamadas_agg = df_llamadas.groupby('id_asesor')['Llamadas'].sum().reset_index()
            
            # 3. COTIZACIONES
            if cotizaciones_file.name.endswith('.csv'):
                df_cotiz = pd.read_csv(cotizaciones_file)
            else:
                df_cotiz = pd.read_excel(cotizaciones_file)
            
            df_cotiz = df_cotiz.rename(columns={'Vendedor': 'id_asesor'})
            df_cotiz['id_asesor'] = df_cotiz['id_asesor'].astype(str).str.strip()
            cotizaciones_agg = df_cotiz.groupby('id_asesor').size().reset_index(name='cantidad_cotizaciones')
            
            # 4. ARMAR REPORTE
            reporte = agentes.merge(ventas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
            reporte = reporte.merge(df_manual, on='id_asesor', how='left')
            
            # Rellenar ceros
            for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
                if col in reporte.columns:
                    reporte[col] = reporte[col].fillna(0)
            
            # Métricas
            reporte['conversion'] = reporte.apply(
                lambda r: 0 if r['leads'] == 0 else (r['cierres'] / r['leads']) * 100, axis=1
            ).round(2)
            
            reporte['ticket_promedio'] = reporte.apply(
                lambda r: 0 if r['cierres'] == 0 else r['ventas'] / r['cierres'], axis=1
            ).round(2)
            
            # Fechas
            reporte['fecha'] = fecha_reporte
            reporte['mes'] = fecha_reporte.strftime('%B')
            reporte['dia'] = fecha_reporte.strftime('%A')
            reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
            reporte['sem_año'] = fecha_reporte.isocalendar()[1]
            reporte['año'] = fecha_reporte.year
            reporte['fecha_creacion'] = datetime.datetime.now()
            
            # Manuales con fecha
            df_manual['fecha'] = fecha_reporte
            df_manual['actualizado_por'] = st.session_state.get('usuario', 'unknown')
            df_manual['timestamp'] = datetime.datetime.now()
            
            # 5. GUARDAR (DELETE + INSERT simple)
            with st.spinner("Guardando..."):
                # Eliminar datos existentes de esta fecha
                client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha_reporte}'").result()
                client.query(f"DELETE FROM `{TABLE_MANUAL}` WHERE fecha = '{fecha_reporte}'").result()
                
                # Insertar nuevos
                client.load_table_from_dataframe(reporte, TABLE_REPORTE).result()
                client.load_table_from_dataframe(df_manual, TABLE_MANUAL).result()
            
            st.success(f"✅ Reporte del {fecha_reporte} guardado")
            
            # Resumen
            st.subheader("📊 Resumen")
            resumen = reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion', 'llamadas']].copy()
            resumen['ventas'] = resumen['ventas'].round(2)
            st.dataframe(resumen, use_container_width=True)
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Ventas", f"${reporte['ventas'].sum():,.0f}")
            col2.metric("Total Cierres", f"{reporte['cierres'].sum():,.0f}")
            col3.metric("Total Leads", f"{reporte['leads'].sum():,.0f}")
            col4.metric("Total Llamadas", f"{reporte['llamadas'].sum():,.0f}")
            
        except Exception as e:
            st.error(f"Error: {e}")

def descargar_reportes():
    st.subheader("📥 Descargar reportes")
    client = init_bq_client()
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Hasta", datetime.date.today())
    
    if st.button("Generar"):
        query = f"""
        SELECT fecha, nombre as agente, supervisor, leads, cierres, 
               ROUND(conversion,2) as conversion, ROUND(ventas,2) as ventas,
               llamadas, cantidad_cotizaciones, asistencia
        FROM `{TABLE_REPORTE}`
        WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        ORDER BY fecha DESC, nombre
        """
        df = client.query(query).to_dataframe()
        if df.empty:
            st.warning("No hay datos")
        else:
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Descargar CSV", csv, f"reporte_{fecha_inicio}_{fecha_fin}.csv")

def run(usuario):
    st.title("🎯 HOPSA")
    st.caption(f"Usuario: {usuario}")
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Informacion", "Reportes"])
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Informacion":
        subir_informacion()
    else:
        descargar_reportes()

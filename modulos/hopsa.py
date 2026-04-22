import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# Configuración
PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"
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
# SESSION STATE para agentes
# -------------------------------
def init_session_state():
    if 'agentes_df' not in st.session_state:
        st.session_state.agentes_df = None

def actualizar_agentes():
    st.subheader("👥 Cargar Agentes (HEXAGON)")
    
    st.markdown("""
    **Archivo Excel o CSV:**
    - `id_asesor` (texto o número)
    - `nombre` (texto)
    - `supervisor` (texto)
    
    📌 **Solo 25 personas aprox**, puedes usar Excel directamente.
    """)
    
    # Subir archivo
    archivo = st.file_uploader(
        "Seleccionar archivo", 
        type=['xlsx', 'xls', 'csv'],
        key="upload_agentes"
    )
    
    if archivo:
        try:
            # Leer según extensión
            if archivo.name.endswith('.csv'):
                # Probar diferentes separadores
                try:
                    df = pd.read_csv(archivo, encoding='utf-8')
                except:
                    archivo.seek(0)
                    df = pd.read_csv(archivo, encoding='latin1', sep=';')
            else:
                df = pd.read_excel(archivo)
            
            # Validar columnas
            columnas_necesarias = ['id_asesor', 'nombre', 'supervisor']
            
            # Buscar coincidencias sin importar mayúsculas
            mapeo = {}
            for col_needed in columnas_necesarias:
                for col_exist in df.columns:
                    if col_exist.lower() == col_needed.lower():
                        mapeo[col_needed] = col_exist
                        break
            
            if len(mapeo) != 3:
                st.error(f"❌ Columnas requeridas: {columnas_necesarias}")
                st.write("Columnas encontradas:", list(df.columns))
                return
            
            # Renombrar columnas
            df = df.rename(columns={mapeo['id_asesor']: 'id_asesor', 
                                     mapeo['nombre']: 'nombre', 
                                     mapeo['supervisor']: 'supervisor'})
            
            # Limpiar datos
            df['id_asesor'] = df['id_asesor'].astype(str).str.strip()
            df['nombre'] = df['nombre'].astype(str).str.strip()
            df['supervisor'] = df['supervisor'].astype(str).str.strip()
            
            # Guardar en session state
            st.session_state.agentes_df = df
            
            st.success(f"✅ {len(df)} agentes cargados correctamente")
            st.dataframe(df, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error al leer archivo: {e}")
            st.info("💡 Asegúrate de que el archivo no esté corrupto")
    
    # Mostrar agentes actuales
    if st.session_state.agentes_df is not None:
        st.subheader("📋 Agentes actualmente cargados")
        st.dataframe(st.session_state.agentes_df, use_container_width=True)
        
        if st.button("🗑️ Limpiar agentes"):
            st.session_state.agentes_df = None
            st.rerun()

# -------------------------------
# SUBIR INFORMACIÓN (corregida indentación)
# -------------------------------
def subir_informacion():
    st.subheader("📂 Subir información del día")
    
    # Verificar que hay agentes cargados
    if st.session_state.agentes_df is None:
        st.warning("⚠️ Primero debes cargar los agentes en 'Actualizar Agentes'")
        return
    
    fecha_reporte = st.date_input("Fecha del reporte", datetime.date.today())
    
    st.markdown("---")
    st.markdown("### 1. Archivos del día")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📊 Ventas**")
        ventas_file = st.file_uploader(
            "Ventas (Excel o CSV)", 
            type=['xlsx', 'xls', 'csv'],
            key="ventas_upload"
        )
        if ventas_file:
            st.caption("Columnas: id_asesor, Venta, Factura")
    
    with col2:
        st.markdown("**📞 Llamadas**")
        llamadas_file = st.file_uploader(
            "Llamadas (CSV)", 
            type=['csv'],
            key="llamadas_upload"
        )
        if llamadas_file:
            st.caption("Columna: id_asesor")
    
    with col3:
        st.markdown("**📝 Cotizaciones**")
        cotizaciones_file = st.file_uploader(
            "Cotizaciones (Excel o CSV)", 
            type=['xlsx', 'xls', 'csv'],
            key="cotizaciones_upload"
        )
        if cotizaciones_file:
            st.caption("Columna: Vendedor (id_asesor)")
    
    st.markdown("---")
    st.markdown("### 2. Datos manuales del día")
    st.info("💡 **Ayuda:**\n- **Leads:** Contactos nuevos gestionados\n- **NPS:** Encuesta de satisfacción (0-10)\n- **PRA 90%:** Calidad/Cumplimiento de meta\n- **Asistencia:** Si el agente trabajó el día completo")
    
    # Formulario para datos manuales
    df_asesores = st.session_state.agentes_df
    
    # Opción para llenado rápido
    modo_rapido = st.checkbox("⚡ Modo rápido - Mismo valor para todos los agentes")
    
    if modo_rapido:
        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            leads_default = st.number_input("Leads (todos)", min_value=0, value=0)
        with col_b:
            nps_default = st.number_input("NPS (todos)", min_value=0, max_value=10, value=0)
        with col_c:
            pra_default = st.number_input("PRA 90% (todos)", min_value=0, max_value=100, value=90, 
                                         help="Calidad/Cumplimiento de meta")
        with col_d:
            asistencia_default = st.selectbox("Asistencia (todos)", 
                                             options=[100, 0], 
                                             format_func=lambda x: "✅ Asistió (100%)" if x == 100 else "❌ No asistió (0%)",
                                             help="100% = Trabajó completo, 0% = No trabajó")
        
        st.warning("⚠️ Esto aplicará el mismo valor a TODOS los agentes. Si necesitas valores diferentes, desactiva esta opción.")
    
    with st.form("datos_manuales_form"):
        datos_manuales = []
        
        for _, row in df_asesores.iterrows():
            with st.container():
                st.markdown(f"**👤 {row['nombre']}** (ID: `{row['id_asesor']}`)")
                
                if modo_rapido:
                    # Usar valores por defecto
                    leads = leads_default
                    nps = nps_default
                    pra = pra_default
                    asistencia = asistencia_default
                    
                    # Mostrar valores aplicados
                    st.caption(f"📊 Leads: {leads} | ⭐ NPS: {nps} | 🎯 PRA: {pra}% | 📅 Asistencia: {'✅' if asistencia == 100 else '❌'}")
                    
                else:
                    # Inputs individuales
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        leads = st.number_input(
                            "Leads", 
                            min_value=0, 
                            value=0, 
                            key=f"leads_{row['id_asesor']}",
                            help="Contactos nuevos gestionados en el día"
                        )
                    with col2:
                        nps = st.number_input(
                            "NPS (0-10)", 
                            min_value=0, 
                            max_value=10, 
                            value=0, 
                            key=f"nps_{row['id_asesor']}",
                            help="Encuesta de satisfacción"
                        )
                    with col3:
                        pra = st.number_input(
                            "PRA 90%", 
                            min_value=0, 
                            max_value=100, 
                            value=90, 
                            key=f"pra_{row['id_asesor']}",
                            help="Calidad / Cumplimiento de meta (0-100%)"
                        )
                    with col4:
                        asistencia = st.selectbox(
                            "Asistencia",
                            options=[100, 0],
                            format_func=lambda x: "✅ Asistió (100%)" if x == 100 else "❌ No asistió (0%)",
                            key=f"asis_{row['id_asesor']}",
                            help="100% = Trabajó el día completo, 0% = No trabajó"
                        )
                
                datos_manuales.append({
                    "id_asesor": str(row['id_asesor']),
                    "leads": leads,
                    "nps": nps,
                    "pra_90": pra,
                    "asistencia": asistencia
                })
                
                st.markdown("---")
        
        submit = st.form_submit_button("🚀 Procesar y guardar todo", type="primary")
    
    if submit:
        if not all([ventas_file, llamadas_file, cotizaciones_file]):
            st.error("❌ Faltan archivos por subir")
            return
        
        try:
            client = init_bq_client()
            df_manual = pd.DataFrame(datos_manuales)
            
            # Procesar ventas
            if ventas_file.name.endswith('.csv'):
                df_ventas = pd.read_csv(ventas_file)
            else:
                df_ventas = pd.read_excel(ventas_file)
            
            # Asegurar que las columnas existen
            df_ventas['id_asesor'] = df_ventas['id_asesor'].astype(str)
            ventas_agg = df_ventas.groupby('id_asesor').agg(
                ventas=('Venta', 'sum'),
                cierres=('Factura', 'count')
            ).reset_index()
            
            # Procesar llamadas
            df_llamadas = pd.read_csv(llamadas_file)
            df_llamadas['id_asesor'] = df_llamadas['id_asesor'].astype(str)
            llamadas_agg = df_llamadas.groupby('id_asesor').size().reset_index(name='llamadas')
            
            # Procesar cotizaciones
            if cotizaciones_file.name.endswith('.csv'):
                df_cotizaciones = pd.read_csv(cotizaciones_file)
            else:
                df_cotizaciones = pd.read_excel(cotizaciones_file)
            
            df_cotizaciones['Vendedor'] = df_cotizaciones['Vendedor'].astype(str)
            cotizaciones_agg = df_cotizaciones.groupby('Vendedor').size().reset_index(name='cantidad_cotizaciones')
            cotizaciones_agg = cotizaciones_agg.rename(columns={'Vendedor': 'id_asesor'})
            
            # Construir reporte
            reporte = df_asesores.merge(ventas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
            reporte = reporte.merge(df_manual, on='id_asesor', how='left')
            
            # Rellenar nulos
            for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
                if col in reporte.columns:
                    reporte[col] = reporte[col].fillna(0)
            
            # Calcular métricas
            reporte['conversion'] = (reporte['cierres'] / reporte['leads'].replace(0, 1)) * 100
            reporte['conversion'] = reporte['conversion'].round(2)
            reporte['ticket_promedio'] = (reporte['ventas'] / reporte['cierres'].replace(0, 1)).round(2)
            
            # Fechas
            reporte['fecha'] = fecha_reporte
            reporte['mes'] = fecha_reporte.strftime('%B')
            reporte['dia'] = fecha_reporte.strftime('%A')
            reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
            reporte['sem_año'] = fecha_reporte.isocalendar()[1]
            reporte['año'] = fecha_reporte.year
            reporte['fecha_creacion'] = datetime.datetime.now()
            
            # Guardar en BigQuery
            job = client.load_table_from_dataframe(
                reporte, TABLE_REPORTE,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
            )
            job.result()
            
            # Guardar datos manuales
            df_manual['fecha'] = fecha_reporte
            df_manual['actualizado_por'] = st.session_state.get('usuario', 'unknown')
            df_manual['timestamp'] = datetime.datetime.now()
            
            job_manual = client.load_table_from_dataframe(
                df_manual, TABLE_MANUAL,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
            )
            job_manual.result()
            
            st.success(f"✅ Reporte del {fecha_reporte} guardado exitosamente")
            
            # Mostrar resumen
            st.subheader("📊 Resumen del día")
            resumen = reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion', 'asistencia']].copy()
            resumen['ventas'] = resumen['ventas'].round(2)
            resumen['conversion'] = resumen['conversion'].round(1)
            # Mapear asistencia a texto
            resumen['asistencia'] = resumen['asistencia'].map({100: '✅ Asistió', 0: '❌ No asistió'})
            st.dataframe(resumen, use_container_width=True)
            
            # Estadísticas rápidas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Ventas", f"${reporte['ventas'].sum():,.0f}")
            with col2:
                st.metric("Total Cierres", f"{reporte['cierres'].sum():,.0f}")
            with col3:
                st.metric("Total Leads", f"{reporte['leads'].sum():,.0f}")
            with col4:
                st.metric("Conversión Promedio", f"{reporte['conversion'].mean():.1f}%")
            
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.exception(e)

# -------------------------------
# DESCARGAR REPORTES
# -------------------------------
def descargar_reportes():
    st.subheader("📥 Descargar reportes históricos")
    
    client = init_bq_client()
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    if st.button("🔍 Generar reporte", type="primary"):
        try:
            query = f"""
            SELECT 
                fecha,
                nombre as agente,
                supervisor,
                leads,
                cierres,
                ROUND(conversion, 2) as conversion,
                nps,
                ROUND(ventas, 2) as ventas,
                ROUND(ticket_promedio, 2) as ticket_promedio,
                llamadas,
                cantidad_cotizaciones,
                pra_90,
                asistencia
            FROM `{TABLE_REPORTE}`
            WHERE fecha BETWEEN @fecha_inicio AND @fecha_fin
            ORDER BY fecha DESC, nombre
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
                    bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
                ]
            )
            
            df = client.query(query, job_config=job_config).to_dataframe()
            
            if df.empty:
                st.warning("No hay datos en el rango seleccionado")
            else:
                st.success(f"✅ {len(df)} registros encontrados")
                st.dataframe(df, use_container_width=True)
                
                # Descargar
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar CSV",
                    csv,
                    f"hopsa_report_{fecha_inicio}_{fecha_fin}.csv",
                    "text/csv"
                )
                
        except Exception as e:
            st.error(f"Error: {e}")

# -------------------------------
# FUNCIÓN PRINCIPAL
# -------------------------------
def run(usuario):
    st.title("🎯 HOPSA - Gestión de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    # Inicializar session state
    init_session_state()
    
    opcion = st.sidebar.radio(
        "Opciones",
        ["1. Actualizar Agentes", "2. Subir Información", "3. Descargar Reportes"]
    )
    
    if opcion == "1. Actualizar Agentes":
        actualizar_agentes()
    elif opcion == "2. Subir Información":
        subir_informacion()
    elif opcion == "3. Descargar Reportes":
        descargar_reportes()

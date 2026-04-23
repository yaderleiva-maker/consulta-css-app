import streamlit as st
import pandas as pd
import datetime
import unicodedata
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

def normalizar_texto(texto):
    """Normaliza texto para comparación (mayúsculas, sin tildes)"""
    if pd.isna(texto):
        return ""
    texto = str(texto).strip().upper()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) 
                   if unicodedata.category(c) != 'Mn')
    return texto

def cargar_agentes():
    try:
        client = init_bq_client()
        query = f"SELECT * FROM `{TABLE_ASESORES}`"
        df = client.query(query).to_dataframe()
        return df if not df.empty else None
    except:
        return None

def leer_csv_inteligente(archivo):
    """Lee CSV detectando separador y encoding"""
    contenido = archivo.getvalue().decode('utf-8')
    primera_linea = contenido.split('\n')[0]
    sep = ';' if ';' in primera_linea else ',' if ',' in primera_linea else '\t'
    archivo.seek(0)
    try:
        return pd.read_csv(archivo, sep=sep, encoding='utf-8')
    except:
        archivo.seek(0)
        return pd.read_csv(archivo, sep=sep, encoding='latin1')

def guardar_con_merge(client, df_reporte, df_manual, fecha_reporte, usuario):
    """Guarda usando MERGE con INSERT ROW (moderno y elegante)"""
    
    temp_reporte = f"{PROJECT_ID}.{DATASET_HOPSA}.temp_reporte_{fecha_reporte.strftime('%Y%m%d')}"
    temp_manual = f"{PROJECT_ID}.{DATASET_HOPSA}.temp_manual_{fecha_reporte.strftime('%Y%m%d')}"
    
    # Subir a tablas temporales
    client.load_table_from_dataframe(
        df_reporte, temp_reporte,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()
    
    client.load_table_from_dataframe(
        df_manual, temp_manual,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()
    
    # MERGE para reporte_diario (con INSERT ROW)
    merge_reporte = f"""
    MERGE `{TABLE_REPORTE}` T
    USING `{temp_reporte}` S
    ON T.id_asesor = S.id_asesor AND T.fecha = S.fecha
    WHEN MATCHED THEN UPDATE SET
        nombre = S.nombre,
        supervisor = S.supervisor,
        ventas = S.ventas,
        cierres = S.cierres,
        llamadas = S.llamadas,
        cantidad_cotizaciones = S.cantidad_cotizaciones,
        leads = S.leads,
        nps = S.nps,
        pra_90 = S.pra_90,
        asistencia = S.asistencia,
        conversion = S.conversion,
        ticket_promedio = S.ticket_promedio,
        mes = S.mes,
        dia = S.dia,
        sem_mes = S.sem_mes,
        sem_año = S.sem_año,
        año = S.año,
        fecha_creacion = S.fecha_creacion
    WHEN NOT MATCHED THEN
        INSERT ROW
    """
    client.query(merge_reporte).result()
    
    # MERGE para datos_manuales
    merge_manual = f"""
    MERGE `{TABLE_MANUAL}` T
    USING `{temp_manual}` S
    ON T.id_asesor = S.id_asesor AND T.fecha = S.fecha
    WHEN MATCHED THEN UPDATE SET
        leads = S.leads,
        nps = S.nps,
        pra_90 = S.pra_90,
        asistencia = S.asistencia,
        actualizado_por = S.actualizado_por,
        timestamp = S.timestamp
    WHEN NOT MATCHED THEN
        INSERT ROW
    """
    client.query(merge_manual).result()
    
    # Limpiar tablas temporales
    client.delete_table(temp_reporte, not_found_ok=True)
    client.delete_table(temp_manual, not_found_ok=True)
    
    return True

def actualizar_agentes():
    st.subheader("👥 Gestionar Agentes")
    
    agentes = cargar_agentes()
    if agentes is not None:
        st.info(f"📋 {len(agentes)} agentes actualmente en sistema")
        st.dataframe(agentes, use_container_width=True)
    
    st.markdown("---")
    st.markdown("""
    **Formato requerido:**
    - `id_asesor` - ID del sistema de ventas (ej: heyanez)
    - `nombre` - Nombre completo
    - `supervisor` - Supervisor
    - `id_llamadas` - ID que aparece en el reporte de llamadas (ej: hyañez)
    """)
    
    archivo = st.file_uploader("Subir archivo", type=['csv', 'xlsx'], key="upload_agentes")
    
    if archivo:
        try:
            if archivo.name.endswith('.csv'):
                df = leer_csv_inteligente(archivo)
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
    
    agentes = cargar_agentes()
    if agentes is None or agentes.empty:
        st.warning("⚠️ Primero carga los agentes")
        if st.button("Ir a Gestionar Agentes"):
            st.session_state.menu_hopsa = "Agentes"
            st.rerun()
        return
    
    fecha_reporte = st.date_input("Fecha del reporte", datetime.date.today())
    st.info(f"📋 {len(agentes)} agentes activos")
    
    st.markdown("---")
    st.markdown("### Archivos del dia")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        ventas_file = st.file_uploader("📊 Ventas", type=['csv', 'xlsx'], key="ventas")
    with col2:
        llamadas_file = st.file_uploader("📞 Llamadas", type=['csv'], key="llamadas")
    with col3:
        cotizaciones_file = st.file_uploader("📝 Cotizaciones", type=['csv', 'xlsx'], key="cotizaciones")
    
    st.markdown("---")
    st.markdown("### Datos manuales")
    
    modo_rapido = st.checkbox("⚡ Modo rapido - mismo valor para todos")
    
    if modo_rapido:
        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            leads_default = st.number_input("Leads", min_value=0, value=0)
        with col_b:
            nps_default = st.number_input("NPS", min_value=0, max_value=10, value=0)
        with col_c:
            pra_default = st.number_input("PRA 90%", min_value=0, max_value=100, value=90)
        with col_d:
            asistencia_default = st.selectbox("Asistencia", [100, 0], 
                                             format_func=lambda x: "✅ Asistio" if x == 100 else "❌ No asistio")
    
    with st.form("datos_manuales_form"):
        datos_manuales = []
        for _, row in agentes.iterrows():
            with st.container():
                st.markdown(f"**👤 {row['nombre']}**")
                
                if modo_rapido:
                    leads = leads_default
                    nps = nps_default
                    pra = pra_default
                    asistencia = asistencia_default
                    st.caption(f"📊 Leads: {leads} | ⭐ NPS: {nps} | 🎯 PRA: {pra}% | 📅 Asistencia: {'✅' if asistencia == 100 else '❌'}")
                else:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        leads = st.number_input("Leads", min_value=0, value=0, key=f"leads_{row['id_asesor']}")
                    with col2:
                        nps = st.number_input("NPS", min_value=0, max_value=10, value=0, key=f"nps_{row['id_asesor']}")
                    with col3:
                        pra = st.number_input("PRA 90%", min_value=0, max_value=100, value=90, key=f"pra_{row['id_asesor']}")
                    with col4:
                        asistencia = st.selectbox("Asistencia", [100, 0], 
                                                 format_func=lambda x: "✅" if x == 100 else "❌",
                                                 key=f"asis_{row['id_asesor']}")
                
                datos_manuales.append({
                    "id_asesor": str(row['id_asesor']),
                    "leads": leads,
                    "nps": nps,
                    "pra_90": pra,
                    "asistencia": asistencia
                })
                st.markdown("---")
        
        submit = st.form_submit_button("🚀 Procesar y guardar", type="primary")
    
    if submit:
        if not all([ventas_file, llamadas_file, cotizaciones_file]):
            st.error("❌ Faltan archivos por subir")
            return
        
        try:
            client = init_bq_client()
            df_manual = pd.DataFrame(datos_manuales)
            
            # 1. Procesar VENTAS
            if ventas_file.name.endswith('.csv'):
                df_ventas = leer_csv_inteligente(ventas_file)
            else:
                df_ventas = pd.read_excel(ventas_file)
            
            # Buscar columna de vendedor
            col_vendedor = 'Vendedor' if 'Vendedor' in df_ventas.columns else df_ventas.columns[0]
            df_ventas = df_ventas.rename(columns={col_vendedor: 'id_asesor'})
            df_ventas['id_asesor'] = df_ventas['id_asesor'].astype(str).str.strip()
            
            ventas_agg = df_ventas.groupby('id_asesor').agg(
                ventas=('Venta', 'sum'),
                cierres=('Factura', 'count')
            ).reset_index()
            
            # 2. Procesar LLAMADAS (cruce por id_llamadas)
            df_llamadas = leer_csv_inteligente(llamadas_file)
            
            # Buscar columna de identificación
            col_id = 'Identificación' if 'Identificación' in df_llamadas.columns else 'Usuario'
            df_llamadas['id_original'] = df_llamadas[col_id].astype(str).str.strip()
            
            # Mapeo exacto por id_llamadas
            mapeo = dict(zip(
                agentes['id_llamadas'].astype(str).str.strip(),
                agentes['id_asesor'].astype(str)
            ))
            
            df_llamadas['id_asesor'] = df_llamadas['id_original'].map(mapeo)
            
            # Mostrar no mapeados
            no_mapeados = df_llamadas[df_llamadas['id_asesor'].isna()]['id_original'].nunique()
            if no_mapeados > 0:
                st.warning(f"⚠️ {no_mapeados} IDs de llamadas sin mapeo")
                df_llamadas['id_asesor'] = df_llamadas['id_asesor'].fillna(df_llamadas['id_original'])
            
            col_llamadas = 'Llamadas' if 'Llamadas' in df_llamadas.columns else None
            if col_llamadas:
                llamadas_agg = df_llamadas.groupby('id_asesor')[col_llamadas].sum().reset_index()
                llamadas_agg = llamadas_agg.rename(columns={col_llamadas: 'llamadas'})
            else:
                llamadas_agg = df_llamadas.groupby('id_asesor').size().reset_index(name='llamadas')
            
            # 3. Procesar COTIZACIONES
            if cotizaciones_file.name.endswith('.csv'):
                df_cotizaciones = leer_csv_inteligente(cotizaciones_file)
            else:
                df_cotizaciones = pd.read_excel(cotizaciones_file)
            
            col_cotizador = 'Vendedor' if 'Vendedor' in df_cotizaciones.columns else 'Creador'
            df_cotizaciones = df_cotizaciones.rename(columns={col_cotizador: 'id_asesor'})
            df_cotizaciones['id_asesor'] = df_cotizaciones['id_asesor'].astype(str).str.strip()
            cotizaciones_agg = df_cotizaciones.groupby('id_asesor').size().reset_index(name='cantidad_cotizaciones')
            
            # 4. Construir reporte
            reporte = agentes.merge(ventas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
            reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
            reporte = reporte.merge(df_manual, on='id_asesor', how='left')
            
            # Rellenar nulos
            for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
                if col in reporte.columns:
                    reporte[col] = reporte[col].fillna(0)
            
            # Calcular métricas (evitando división por cero)
            reporte['conversion'] = reporte.apply(
                lambda r: 0 if r['leads'] == 0 else (r['cierres'] / r['leads']) * 100,
                axis=1
            ).round(2)
            
            reporte['ticket_promedio'] = reporte.apply(
                lambda r: 0 if r['cierres'] == 0 else r['ventas'] / r['cierres'],
                axis=1
            ).round(2)
            
            # Fechas
            reporte['fecha'] = fecha_reporte
            reporte['mes'] = fecha_reporte.strftime('%B')
            reporte['dia'] = fecha_reporte.strftime('%A')
            reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
            reporte['sem_año'] = fecha_reporte.isocalendar()[1]
            reporte['año'] = fecha_reporte.year
            reporte['fecha_creacion'] = datetime.datetime.now()
            
            # Preparar datos manuales
            df_manual['fecha'] = fecha_reporte
            df_manual['actualizado_por'] = st.session_state.get('usuario', 'unknown')
            df_manual['timestamp'] = datetime.datetime.now()
            
            # 5. Guardar usando MERGE
            with st.spinner("Guardando con MERGE..."):
                guardar_con_merge(client, reporte, df_manual, fecha_reporte, st.session_state.get('usuario', 'unknown'))
            
            st.success(f"✅ Reporte del {fecha_reporte} guardado exitosamente")
            
            # Resumen
            st.subheader("📊 Resumen del dia")
            resumen = reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion', 'llamadas']].copy()
            resumen['ventas'] = resumen['ventas'].round(2)
            st.dataframe(resumen, use_container_width=True)
            
            # Totales
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Ventas", f"${reporte['ventas'].sum():,.0f}")
            col2.metric("Total Cierres", f"{reporte['cierres'].sum():,.0f}")
            col3.metric("Total Leads", f"{reporte['leads'].sum():,.0f}")
            col4.metric("Total Llamadas", f"{reporte['llamadas'].sum():,.0f}")
            
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.exception(e)

def descargar_reportes():
    st.subheader("📥 Descargar reportes historicos")
    
    client = init_bq_client()
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    tipo = st.radio("Tipo de reporte", ["Diario por agente", "Consolidado por agente"])
    
    if st.button("🔍 Generar reporte", type="primary"):
        try:
            if tipo == "Diario por agente":
                query = f"""
                SELECT 
                    fecha,
                    nombre as agente,
                    supervisor,
                    leads,
                    cierres,
                    ROUND(conversion, 2) as conversion_pct,
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
            else:
                query = f"""
                SELECT 
                    nombre as agente,
                    supervisor,
                    SUM(leads) as total_leads,
                    SUM(cierres) as total_cierres,
                    ROUND(AVG(conversion), 2) as conversion_promedio,
                    ROUND(AVG(nps), 2) as nps_promedio,
                    ROUND(SUM(ventas), 2) as total_ventas,
                    ROUND(AVG(ticket_promedio), 2) as ticket_promedio,
                    SUM(llamadas) as total_llamadas,
                    SUM(cantidad_cotizaciones) as total_cotizaciones,
                    ROUND(AVG(pra_90), 2) as pra_promedio,
                    ROUND(AVG(asistencia), 0) as asistencia_promedio
                FROM `{TABLE_REPORTE}`
                WHERE fecha BETWEEN @fecha_inicio AND @fecha_fin
                GROUP BY nombre, supervisor
                ORDER BY total_ventas DESC
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
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar CSV",
                    csv,
                    f"hopsa_{tipo.replace(' ', '_')}_{fecha_inicio}_{fecha_fin}.csv",
                    "text/csv"
                )
                
        except Exception as e:
            st.error(f"Error: {e}")

def run(usuario):
    st.title("🎯 HOPSA - Gestion de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    if 'menu_hopsa' not in st.session_state:
        st.session_state.menu_hopsa = "Agentes"
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Informacion", "Reportes"],
                              index=["Agentes", "Subir Informacion", "Reportes"].index(st.session_state.menu_hopsa))
    
    st.session_state.menu_hopsa = opcion
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Informacion":
        subir_informacion()
    else:
        descargar_reportes()

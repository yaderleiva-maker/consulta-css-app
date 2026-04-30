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
TABLE_HISTORICO_VENTAS = f"{PROJECT_ID}.{DATASET_HOPSA}.hechos_ventas"

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
        
def guardar_historico_ventas(client, df_ventas, fecha_periodo, usuario, modo="REEMPLAZAR"):
    """Guarda todas las ventas individuales en tabla histórica
    
    Args:
        modo: "REEMPLAZAR" (borra y guarda nuevo) o "AGREGAR" (append)
    """
    
    df_historico = df_ventas.copy()
    
    # Buscar columna de vendedor
    col_vendedor = None
    for col in df_historico.columns:
        if col.lower() in ['vendedor', 'creador', 'id_asesor']:
            col_vendedor = col
            break
    
    if col_vendedor is None:
        col_vendedor = df_historico.columns[0]
    
    # Renombrar y limpiar
    df_historico = df_historico.rename(columns={col_vendedor: 'id_asesor'})
    df_historico['id_asesor'] = df_historico['id_asesor'].astype(str).str.strip().str.upper()
    
    # Agregar metadatos
    df_historico['fecha_carga'] = datetime.datetime.now()
    df_historico['usuario_carga'] = usuario
    df_historico['periodo_actualizado'] = fecha_periodo
    
    # Generar ID único
    df_historico['id_venta'] = df_historico['Factura'].astype(str) + '_' + df_historico['fecha_carga'].astype(str)
    
    # Seleccionar columnas que existen
    columnas_destino = ['id_venta', 'id_asesor', 'periodo_actualizado', 'Factura', 'Venta', 'Costo', 'Margen', 'Descuento', 'fecha_carga', 'usuario_carga']
    columnas_origen = [col for col in columnas_destino if col in df_historico.columns]
    
    if modo == "REEMPLAZAR":
        # Eliminar registros antiguos para este período
        try:
            client.query(f"DELETE FROM `{TABLE_HISTORICO_VENTAS}` WHERE periodo_actualizado = '{fecha_periodo}'").result()
        except Exception as e:
            st.warning(f"No se pudieron eliminar registros antiguos: {e}")
    
    # Guardar en BigQuery
    job = client.load_table_from_dataframe(
        df_historico[columnas_origen], 
        TABLE_HISTORICO_VENTAS,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job.result()
    
    return len(df_historico)

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
    
    if fecha_reporte != datetime.date.today():
        st.warning(f"⚠️ Estás cargando datos para {fecha_reporte} (no para hoy). Los datos existentes de esta fecha serán REEMPLAZADOS.")
    else:
        st.info(f"📅 Cargando datos para HOY ({fecha_reporte})")
    
    # Botón para borrar la fecha manualmente
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("🗑️ Borrar datos de esta fecha", type="secondary"):
            try:
                client = init_bq_client()
                client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha_reporte}'").result()
                client.query(f"DELETE FROM `{TABLE_MANUAL}` WHERE fecha = '{fecha_reporte}'").result()
                st.success(f"✅ Datos del {fecha_reporte} eliminados")
                st.rerun()
            except Exception as e:
                st.error(f"Error al borrar: {e}")
    with col_btn2:
        st.markdown("*Los datos se reemplazarán automáticamente al subir nuevos archivos*")
    
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
    
    with st.form("datos_manuales_form"):
        datos_manuales = []
        for _, row in agentes.iterrows():
            with st.container():
                st.markdown(f"**👤 {row['nombre']}**")
                
                if modo_rapido:
                    col_a, col_b, col_c, col_d = st.columns(4)
                    with col_a:
                        leads_default = st.number_input("Leads", min_value=0, value=0, key="leads_default")
                    with col_b:
                        nps_default = st.number_input("NPS", min_value=0, max_value=10, value=0, key="nps_default")
                    with col_c:
                        pra_default = st.number_input("PRA 90%", min_value=0, max_value=100, value=90, key="pra_default")
                    with col_d:
                        asistencia_default = st.selectbox("Asistencia", [100, 0], 
                                                         format_func=lambda x: "✅" if x == 100 else "❌",
                                                         key="asis_default")
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
            
            col_vendedor = 'Vendedor' if 'Vendedor' in df_ventas.columns else df_ventas.columns[0]
            df_ventas = df_ventas.rename(columns={col_vendedor: 'id_asesor'})
            df_ventas['id_asesor'] = df_ventas['id_asesor'].astype(str).str.strip()
            
            ventas_agg = df_ventas.groupby('id_asesor').agg(
                ventas=('Venta', 'sum'),
                cierres=('Factura', 'count')
            ).reset_index()
            
            # 2. Procesar LLAMADAS
            df_llamadas = leer_csv_inteligente(llamadas_file)
            
            col_id = 'Identificación' if 'Identificación' in df_llamadas.columns else 'Usuario'
            df_llamadas['id_original'] = df_llamadas[col_id].astype(str).str.strip()
            
            mapeo = dict(zip(
                agentes['id_llamadas'].astype(str).str.strip(),
                agentes['id_asesor'].astype(str)
            ))
            
            df_llamadas['id_asesor'] = df_llamadas['id_original'].map(mapeo)
            
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
            
            for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
                if col in reporte.columns:
                    reporte[col] = reporte[col].fillna(0)
            
            reporte['conversion'] = reporte.apply(
                lambda r: 0 if r['leads'] == 0 else (r['cierres'] / r['leads']) * 100, axis=1
            ).round(2)
            
            reporte['ticket_promedio'] = reporte.apply(
                lambda r: 0 if r['cierres'] == 0 else r['ventas'] / r['cierres'], axis=1
            ).round(2)
            
            reporte['fecha'] = fecha_reporte
            reporte['mes'] = fecha_reporte.strftime('%B')
            reporte['dia'] = fecha_reporte.strftime('%A')
            reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
            reporte['sem_año'] = fecha_reporte.isocalendar()[1]
            reporte['año'] = fecha_reporte.year
            reporte['fecha_creacion'] = datetime.datetime.now()
            
            # 5. Guardar (REEMPLAZAR, no sumar)
            with st.spinner("Guardando en BigQuery..."):
                # Eliminar datos existentes de esta fecha
                client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha_reporte}'").result()
                client.query(f"DELETE FROM `{TABLE_MANUAL}` WHERE fecha = '{fecha_reporte}'").result()
                
                # Insertar nuevos datos
                client.load_table_from_dataframe(reporte, TABLE_REPORTE).result()
                client.load_table_from_dataframe(df_manual, TABLE_MANUAL).result()
                
                # Guardar histórico de ventas (también reemplaza para esta fecha)
                registros = guardar_historico_ventas(client, df_ventas, fecha_reporte, 
                                                    st.session_state.get('usuario', 'unknown'), "REEMPLAZAR")
                st.caption(f"📝 {registros} registros guardados en histórico de ventas")
            
            st.success(f"✅ Reporte del {fecha_reporte} guardado exitosamente")
            
            # Resumen
            st.subheader("📊 Resumen del dia")
            columnas_resumen = ['nombre', 'leads', 'cierres', 'ventas', 'conversion']
            if 'llamadas' in reporte.columns:
                columnas_resumen.append('llamadas')
            if 'cantidad_cotizaciones' in reporte.columns:
                columnas_resumen.append('cantidad_cotizaciones')
            
            resumen = reporte[columnas_resumen].copy()
            if 'ventas' in resumen.columns:
                resumen['ventas'] = resumen['ventas'].round(2)
            st.dataframe(resumen, use_container_width=True)
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Ventas", f"${reporte['ventas'].sum():,.0f}")
            col2.metric("Total Cierres", f"{reporte['cierres'].sum():,.0f}")
            col3.metric("Total Leads", f"{reporte['leads'].sum():,.0f}")
            col4.metric("Total Llamadas", f"{reporte['llamadas'].sum():,.0f}")
            
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.exception(e)

def actualizar_ventas_periodo():
    st.subheader("🔄 Actualizar ventas por período")
    
    agentes = cargar_agentes()
    if agentes is None or agentes.empty:
        st.warning("⚠️ Primero carga los agentes")
        return
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=7))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    st.warning(f"⚠️ Esto REEMPLAZARÁ SOLO las ventas para las fechas: {fecha_inicio} a {fecha_fin}")
    st.info("📌 Los demás datos (llamadas, cotizaciones, leads, NPS, asistencia) se conservan.")
    
    archivo_ventas = st.file_uploader("📊 Archivo de ventas (Excel o CSV)", type=['csv', 'xlsx'], key="ventas_periodo")
    
    if archivo_ventas and st.button("🚀 Actualizar ventas del período", type="primary"):
        try:
            client = init_bq_client()
            
            # Leer archivo
            if archivo_ventas.name.endswith('.csv'):
                df_ventas = pd.read_csv(archivo_ventas)
            else:
                df_ventas = pd.read_excel(archivo_ventas)
            
            # Verificar que existe columna Fecha
            if 'Fecha' not in df_ventas.columns:
                st.error("❌ El archivo debe tener una columna 'Fecha'")
                return
            
            # Asegurar formato de fecha
            df_ventas['Fecha'] = pd.to_datetime(df_ventas['Fecha']).dt.date
            
            # Buscar columna de vendedor
            col_vendedor = None
            for col in df_ventas.columns:
                if col.lower() in ['vendedor', 'creador', 'id_asesor']:
                    col_vendedor = col
                    break
            
            if col_vendedor is None:
                col_vendedor = df_ventas.columns[0]
                st.warning(f"Usando '{col_vendedor}' como columna de vendedor")
            
            # Fechas a procesar
            fechas_procesar = pd.date_range(fecha_inicio, fecha_fin).tolist()
            progreso = st.progress(0)
            
            for i, fecha in enumerate(fechas_procesar):
                st.info(f"📅 Procesando {fecha.strftime('%Y-%m-%d')}...")
                
                # Filtrar ventas de esta fecha específica
                df_fecha = df_ventas[df_ventas['Fecha'] == fecha.date()].copy()
                
                if df_fecha.empty:
                    st.info(f"   ℹ️ No hay ventas para {fecha.strftime('%Y-%m-%d')}, saltando...")
                    progreso.progress((i + 1) / len(fechas_procesar))
                    continue
                
                # Renombrar columna de vendedor
                df_fecha = df_fecha.rename(columns={col_vendedor: 'id_asesor'})
                df_fecha['id_asesor'] = df_fecha['id_asesor'].astype(str).str.strip().str.upper()
                
                # Agrupar ventas por agente
                ventas_agg = df_fecha.groupby('id_asesor').agg(
                    ventas=('Venta', 'sum'),
                    cierres=('Factura', 'count')
                ).reset_index()
                
                # Calcular devoluciones
                df_devoluciones = df_fecha[df_fecha['Venta'] < 0].groupby('id_asesor')['Venta'].sum().reset_index()
                if not df_devoluciones.empty:
                    df_devoluciones = df_devoluciones.rename(columns={'Venta': 'devoluciones'})
                    ventas_agg = ventas_agg.merge(df_devoluciones, on='id_asesor', how='left')
                    ventas_agg['devoluciones'] = ventas_agg['devoluciones'].fillna(0)
                else:
                    ventas_agg['devoluciones'] = 0
                
                # Obtener reporte existente (para conservar llamadas, leads, etc.)
                query_existente = f"SELECT * FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha.strftime('%Y-%m-%d')}'"
                df_existente = client.query(query_existente).to_dataframe()
                
                if df_existente.empty:
                    # No hay datos previos, crear estructura básica
                    nuevo_reporte = agentes.merge(ventas_agg, on='id_asesor', how='left')
                    for col in ['llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
                        if col not in nuevo_reporte.columns:
                            nuevo_reporte[col] = 0
                    if 'devoluciones' not in nuevo_reporte.columns:
                        nuevo_reporte['devoluciones'] = 0
                else:
                    # Conservar todos los datos existentes, solo actualizar ventas
                    nuevo_reporte = df_existente.copy()
                    # Actualizar ventas y cierres
                    for _, row in ventas_agg.iterrows():
                        mask = nuevo_reporte['id_asesor'] == row['id_asesor']
                        nuevo_reporte.loc[mask, 'ventas'] = row['ventas']
                        nuevo_reporte.loc[mask, 'cierres'] = row['cierres']
                        if 'devoluciones' in row:
                            nuevo_reporte.loc[mask, 'devoluciones'] = row['devoluciones']
                    
                    # Asegurar que las columnas nuevas tengan valor 0 si no existen
                    if 'devoluciones' not in nuevo_reporte.columns:
                        nuevo_reporte['devoluciones'] = 0
                    else:
                        nuevo_reporte['devoluciones'] = nuevo_reporte['devoluciones'].fillna(0)
                
                # Rellenar nulos en columnas numéricas
                for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia', 'devoluciones']:
                    if col in nuevo_reporte.columns:
                        nuevo_reporte[col] = nuevo_reporte[col].fillna(0)
                
                # Recalcular métricas
                nuevo_reporte['conversion'] = nuevo_reporte.apply(
                    lambda r: 0 if r.get('leads', 0) == 0 else (r['cierres'] / r['leads']) * 100, axis=1
                ).round(2)
                
                nuevo_reporte['ticket_promedio'] = nuevo_reporte.apply(
                    lambda r: 0 if r['cierres'] == 0 else r['ventas'] / r['cierres'], axis=1
                ).round(2)
                
                # Fechas
                nuevo_reporte['fecha'] = fecha.date()
                nuevo_reporte['mes'] = fecha.strftime('%B')
                nuevo_reporte['dia'] = fecha.strftime('%A')
                nuevo_reporte['sem_mes'] = (fecha.day - 1) // 7 + 1
                nuevo_reporte['sem_año'] = fecha.isocalendar()[1]
                nuevo_reporte['año'] = fecha.year
                nuevo_reporte['fecha_creacion'] = datetime.datetime.now()
                
                # Guardar reporte (reemplazar la fecha completa)
                client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha.strftime('%Y-%m-%d')}'").result()
                client.load_table_from_dataframe(nuevo_reporte, TABLE_REPORTE).result()
                
                # Guardar histórico de ventas (reemplaza para esta fecha)
                registros = guardar_historico_ventas(client, df_fecha, fecha.date(), 
                                                    st.session_state.get('usuario', 'unknown'), "REEMPLAZAR")
                st.caption(f"   📝 {registros} registros guardados en histórico")
                
                st.success(f"✅ {fecha.strftime('%Y-%m-%d')} actualizado")
                progreso.progress((i + 1) / len(fechas_procesar))
            
            # Mostrar resumen final
            st.subheader("📊 Nuevos totales del período")
            query_totales = f"""
            SELECT 
                SUM(ventas) as total_ventas,
                SUM(cierres) as total_cierres,
                SUM(llamadas) as total_llamadas,
                SUM(leads) as total_leads
            FROM `{TABLE_REPORTE}`
            WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            """
            df_totales = client.query(query_totales).to_dataframe()
            st.dataframe(df_totales, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error: {e}")
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
    st.image("assets/logo2.png", width=500)
    st.markdown("---")
    st.title("HOPSA - Gestion de Ventas")
    st.caption(f"Usuario: {usuario}")
    
    if 'menu_hopsa' not in st.session_state:
        st.session_state.menu_hopsa = "Agentes"
    
    opcion = st.sidebar.radio("Menu", ["Agentes", "Subir Informacion", "Actualizar Ventas", "Reportes"],
                              index=["Agentes", "Subir Informacion", "Actualizar Ventas", "Reportes"].index(st.session_state.menu_hopsa))
    
    st.session_state.menu_hopsa = opcion
    
    # Botón de borrado en el sidebar
    with st.sidebar.expander("🗑️ Herramientas de limpieza"):
        fecha_borrar = st.date_input("Fecha a eliminar", datetime.date.today(), key="fecha_borrar")
        
        # Checkbox de confirmación
        confirmar = st.checkbox("⚠️ Confirmo que quiero eliminar estos datos (reporte, manuales y histórico)")
        
        if st.button("Eliminar datos de esta fecha", type="secondary"):
            if not confirmar:
                st.sidebar.warning("Marca la casilla de confirmación primero")
            else:
                try:
                    client = init_bq_client()
                    # Eliminar de reporte_diario
                    client.query(f"DELETE FROM `{TABLE_REPORTE}` WHERE fecha = '{fecha_borrar}'").result()
                    # Eliminar de datos_manuales
                    client.query(f"DELETE FROM `{TABLE_MANUAL}` WHERE fecha = '{fecha_borrar}'").result()
                    # Eliminar de hechos_ventas (histórico)
                    client.query(f"DELETE FROM `{TABLE_HISTORICO_VENTAS}` WHERE periodo_actualizado = '{fecha_borrar}'").result()
                    
                    st.sidebar.success(f"✅ Datos del {fecha_borrar} eliminados (reporte, manuales e histórico)")
                    st.rerun()
                except Exception as e:
                    st.sidebar.error(f"Error al borrar: {e}")
    
    if opcion == "Agentes":
        actualizar_agentes()
    elif opcion == "Subir Informacion":
        subir_informacion()
    elif opcion == "Actualizar Ventas":
        actualizar_ventas_periodo()
    else:
        descargar_reportes()

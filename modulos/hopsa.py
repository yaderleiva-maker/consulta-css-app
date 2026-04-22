import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import hashlib
import io

# -------------------------------
# CONFIGURACIÓN BIGQUERY
# -------------------------------
PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"  # Usando tu dataset existente
TABLE_ASESORES = f"{PROJECT_ID}.{DATASET_HOPSA}.asesores"
TABLE_REPORTE = f"{PROJECT_ID}.{DATASET_HOPSA}.hopsa_reporte_diario"
TABLE_MANUAL = f"{PROJECT_ID}.{DATASET_HOPSA}.hopsa_datos_manuales"

# -------------------------------
# INICIALIZAR CLIENTE BQ
# -------------------------------
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
# 1. ACTUALIZAR AGENTES
# -------------------------------
def actualizar_agentes():
    st.subheader("👥 Actualizar tabla de Agentes (HEXAGON)")
    st.markdown("Sube un archivo Excel o CSV con la estructura: `id_asesor`, `nombre`, `supervisor`")
    
    archivo = st.file_uploader(
        "Seleccionar archivo de agentes",
        type=['xlsx', 'xls', 'csv'],
        key="upload_agentes"
    )
    
    if archivo:
        try:
            # Leer archivo
            if archivo.name.endswith('.csv'):
                df = pd.read_csv(archivo)
            else:
                df = pd.read_excel(archivo)
            
            # Validar columnas necesarias
            columnas_requeridas = ['id_asesor', 'nombre', 'supervisor']
            if not all(col in df.columns for col in columnas_requeridas):
                st.error(f"❌ El archivo debe tener las columnas: {', '.join(columnas_requeridas)}")
                st.write("Columnas encontradas:", list(df.columns))
                return
            
            # Mostrar preview
            st.write("**Vista previa:**")
            st.dataframe(df.head(), use_container_width=True)
            
            if st.button("✅ Guardar en BigQuery", key="save_agentes"):
                client = init_bq_client()
                
                # Crear tabla si no existe
                schema = [
                    bigquery.SchemaField("id_asesor", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("nombre", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("supervisor", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("fecha_actualizacion", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("activo", "BOOLEAN", mode="NULLABLE"),
                ]
                
                # Agregar metadatos
                df['fecha_actualizacion'] = datetime.datetime.now()
                df['activo'] = True
                
                # Hacer upsert: marcar inactivos los que ya no están
                # Primero, obtener ids actuales
                try:
                    query_ids = f"SELECT id_asesor FROM `{TABLE_ASESORES}` WHERE activo = true"
                    df_existente = client.query(query_ids).to_dataframe()
                    ids_actuales = set(df_existente['id_asesor'])
                    ids_nuevos = set(df['id_asesor'])
                    
                    # Desactivar los que ya no están
                    ids_desactivar = ids_actuales - ids_nuevos
                    if ids_desactivar:
                        update_query = f"""
                        UPDATE `{TABLE_ASESORES}`
                        SET activo = false, fecha_actualizacion = CURRENT_TIMESTAMP()
                        WHERE id_asesor IN UNNEST(@ids)
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", list(ids_desactivar))]
                        )
                        client.query(update_query, job_config=job_config).result()
                        st.info(f"📌 {len(ids_desactivar)} agentes desactivados (ya no están en el archivo)")
                except Exception as e:
                    st.warning(f"No se pudo hacer desactivación: {e}")
                
                # Subir/actualizar datos
                job = client.load_table_from_dataframe(
                    df, TABLE_ASESORES, 
                    job_config=bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        schema=schema
                    )
                )
                job.result()
                
                st.success(f"✅ {len(df)} agentes guardados/actualizados en BigQuery")
                
                # Mostrar agentes activos
                st.subheader("📋 Agentes activos actualmente")
                df_activos = client.query(f"SELECT * FROM `{TABLE_ASESORES}` WHERE activo = true").to_dataframe()
                st.dataframe(df_activos, use_container_width=True)
                
        except Exception as e:
            st.error(f"Error al procesar archivo: {e}")

# -------------------------------
# 2. SUBIR INFORMACIÓN (ventas, llamadas, cotizaciones)
# -------------------------------
def subir_informacion():
    st.subheader("📂 Subir información del día")
    
    fecha_reporte = st.date_input("Fecha del reporte", datetime.date.today())
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Ventas**")
        ventas_file = st.file_uploader(
            "Ventas_Detalle.xls", 
            type=['xls', 'xlsx'],
            key="ventas_upload"
        )
    
    with col2:
        st.markdown("**Llamadas**")
        llamadas_file = st.file_uploader(
            "llamadas.csv/txt", 
            type=['csv', 'txt'],
            key="llamadas_upload"
        )
    
    with col3:
        st.markdown("**Cotizaciones**")
        cotizaciones_file = st.file_uploader(
            "cotizaciones.xls", 
            type=['xls', 'xlsx'],
            key="cotizaciones_upload"
        )
    
    # Datos manuales (NPS, PRA, Asistencia)
    st.markdown("---")
    st.subheader("✏️ Datos manuales (NPS, PRA, Asistencia)")
    
    # Cargar agentes activos
    client = init_bq_client()
    try:
        df_asesores = client.query(f"SELECT * FROM `{TABLE_ASESORES}` WHERE activo = true").to_dataframe()
        if df_asesores.empty:
            st.warning("⚠️ No hay agentes cargados. Ve a 'Actualizar Agentes' primero.")
            return
    except Exception as e:
        st.error(f"Error al cargar agentes: {e}")
        return
    
    # Cargar datos manuales existentes si los hay
    try:
        query_existente = f"""
        SELECT id_asesor, nps, pra_90, asistencia, leads, observaciones
        FROM `{TABLE_MANUAL}`
        WHERE fecha = @fecha
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("fecha", "DATE", fecha_reporte)]
        )
        df_existente = client.query(query_existente, job_config=job_config).to_dataframe()
        existente_dict = df_existente.set_index('id_asesor').to_dict('index')
    except:
        existente_dict = {}
    
    datos_manuales = []
    for _, row in df_asesores.iterrows():
        with st.expander(f"📌 {row['nombre']} (ID: {row['id_asesor']})"):
            existente = existente_dict.get(row['id_asesor'], {})
            
            leads = st.number_input(
                "Leads tocados", 
                min_value=0, 
                value=int(existente.get('leads', 0)),
                key=f"leads_{row['id_asesor']}"
            )
            nps = st.number_input(
                "NPS (0-10)", 
                min_value=0, 
                max_value=10,
                value=int(existente.get('nps', 0)),
                key=f"nps_{row['id_asesor']}"
            )
            pra = st.number_input(
                "PRA 90%", 
                min_value=0.0, 
                max_value=100.0, 
                value=float(existente.get('pra_90', 90.0)),
                key=f"pra_{row['id_asesor']}"
            )
            asistencia = st.number_input(
                "Asistencia %", 
                min_value=0.0, 
                max_value=100.0,
                value=float(existente.get('asistencia', 100.0)),
                key=f"asis_{row['id_asesor']}"
            )
            observaciones = st.text_area(
                "Observaciones",
                value=existente.get('observaciones', ''),
                key=f"obs_{row['id_asesor']}"
            )
            
            datos_manuales.append({
                "id_asesor": row['id_asesor'],
                "leads": leads,
                "nps": nps,
                "pra_90": pra,
                "asistencia": asistencia,
                "observaciones": observaciones
            })
    
    df_manual = pd.DataFrame(datos_manuales)
    
    # Botón para procesar todo
    if st.button("🚀 Procesar y guardar todo", type="primary"):
        if not ventas_file or not llamadas_file or not cotizaciones_file:
            st.error("❌ Faltan archivos por subir (Ventas, Llamadas o Cotizaciones)")
            return
        
        # Procesar ventas
        df_ventas = pd.read_excel(ventas_file)
        # Asumiendo columnas: id_asesor, Venta, Factura
        ventas_agg = df_ventas.groupby('id_asesor').agg(
            ventas=('Venta', 'sum'),
            cierres=('Factura', 'count')
        ).reset_index()
        
        # Procesar llamadas
        df_llamadas = pd.read_csv(llamadas_file)
        llamadas_agg = df_llamadas.groupby('id_asesor').size().reset_index(name='llamadas')
        
        # Procesar cotizaciones
        df_cotizaciones = pd.read_excel(cotizaciones_file)
        cotizaciones_agg = df_cotizaciones.groupby('Vendedor').size().reset_index(name='cantidad_cotizaciones')
        cotizaciones_agg = cotizaciones_agg.rename(columns={'Vendedor': 'id_asesor'})
        
        # Construir reporte final
        reporte = df_asesores.merge(ventas_agg, on='id_asesor', how='left')
        reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
        reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
        reporte = reporte.merge(df_manual[['id_asesor', 'leads', 'nps', 'pra_90', 'asistencia']], on='id_asesor', how='left')
        
        # Rellenar nulos
        for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads']:
            if col in reporte.columns:
                reporte[col] = reporte[col].fillna(0)
        
        # Calcular métricas
        reporte['conversion'] = (reporte['cierres'] / reporte['leads'].replace(0, 1)) * 100
        reporte['conversion'] = reporte['conversion'].round(2)
        reporte['ticket_promedio'] = (reporte['ventas'] / reporte['cierres'].replace(0, 1)).round(2)
        
        # Dimensiones temporales
        reporte['fecha'] = fecha_reporte
        reporte['mes'] = fecha_reporte.strftime('%B')
        reporte['dia'] = fecha_reporte.strftime('%A')
        reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
        reporte['sem_año'] = fecha_reporte.isocalendar()[1]
        reporte['año'] = fecha_reporte.year
        reporte['fecha_creacion'] = datetime.datetime.now()
        
        # Guardar en BigQuery
        try:
            # Guardar reporte
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
            
            st.success(f"✅ Reporte del {fecha_reporte} guardado exitosamente en BigQuery")
            
            # Mostrar resumen
            st.subheader("📊 Resumen del reporte generado")
            st.dataframe(reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion']], use_container_width=True)
            
        except Exception as e:
            st.error(f"Error al guardar: {e}")

# -------------------------------
# 3. DESCARGAR REPORTES
# -------------------------------
def descargar_reportes():
    st.subheader("📥 Descargar reportes históricos")
    
    client = init_bq_client()
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Fecha fin", datetime.date.today())
    
    # Opciones de descarga
    tipo_reporte = st.radio(
        "Tipo de reporte",
        ["Resumen diario por agente", "Resumen consolidado por agente", "Resumen por supervisor"]
    )
    
    if st.button("🔍 Generar reporte", type="primary"):
        try:
            if tipo_reporte == "Resumen diario por agente":
                query = f"""
                SELECT 
                    r.fecha,
                    a.nombre as agente,
                    a.supervisor,
                    r.leads,
                    r.cierres,
                    ROUND(r.conversion, 2) as conversion_pct,
                    r.nps,
                    ROUND(r.ventas, 2) as ventas,
                    ROUND(r.ticket_promedio, 2) as ticket_promedio,
                    r.llamadas,
                    r.cantidad_cotizaciones,
                    r.pra_90,
                    r.asistencia
                FROM `{TABLE_REPORTE}` r
                JOIN `{TABLE_ASESORES}` a ON r.id_asesor = a.id_asesor
                WHERE r.fecha BETWEEN @fecha_inicio AND @fecha_fin
                  AND a.activo = true
                ORDER BY r.fecha DESC, a.nombre
                """
            
            elif tipo_reporte == "Resumen consolidado por agente":
                query = f"""
                SELECT 
                    a.nombre as agente,
                    a.supervisor,
                    SUM(r.leads) as total_leads,
                    SUM(r.cierres) as total_cierres,
                    ROUND(AVG(r.conversion), 2) as conversion_promedio,
                    ROUND(AVG(r.nps), 2) as nps_promedio,
                    SUM(r.ventas) as total_ventas,
                    ROUND(AVG(r.ticket_promedio), 2) as ticket_promedio,
                    SUM(r.llamadas) as total_llamadas,
                    SUM(r.cantidad_cotizaciones) as total_cotizaciones,
                    ROUND(AVG(r.pra_90), 2) as pra_promedio,
                    ROUND(AVG(r.asistencia), 2) as asistencia_promedio
                FROM `{TABLE_REPORTE}` r
                JOIN `{TABLE_ASESORES}` a ON r.id_asesor = a.id_asesor
                WHERE r.fecha BETWEEN @fecha_inicio AND @fecha_fin
                  AND a.activo = true
                GROUP BY a.nombre, a.supervisor
                ORDER BY total_ventas DESC
                """
            
            else:  # Resumen por supervisor
                query = f"""
                SELECT 
                    a.supervisor,
                    COUNT(DISTINCT a.id_asesor) as total_agentes,
                    SUM(r.leads) as total_leads,
                    SUM(r.cierres) as total_cierres,
                    ROUND(AVG(r.conversion), 2) as conversion_promedio,
                    SUM(r.ventas) as total_ventas,
                    SUM(r.llamadas) as total_llamadas,
                    SUM(r.cantidad_cotizaciones) as total_cotizaciones
                FROM `{TABLE_REPORTE}` r
                JOIN `{TABLE_ASESORES}` a ON r.id_asesor = a.id_asesor
                WHERE r.fecha BETWEEN @fecha_inicio AND @fecha_fin
                  AND a.activo = true
                GROUP BY a.supervisor
                ORDER BY total_ventas DESC
                """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
                    bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
                ]
            )
            
            df_resultado = client.query(query, job_config=job_config).to_dataframe()
            
            if df_resultado.empty:
                st.warning("No hay datos en el rango seleccionado")
            else:
                st.success(f"✅ {len(df_resultado)} registros encontrados")
                st.dataframe(df_resultado, use_container_width=True)
                
                # Descargar CSV
                csv = df_resultado.to_csv(index=False).encode('utf-8')
                nombre_archivo = f"hopsa_{tipo_reporte.replace(' ', '_')}_{fecha_inicio}_{fecha_fin}.csv"
                st.download_button(
                    "📥 Descargar CSV",
                    csv,
                    nombre_archivo,
                    "text/csv",
                    key="download_report"
                )
                
                # Descargar Excel (opcional)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_resultado.to_excel(writer, sheet_name='Reporte', index=False)
                excel_data = output.getvalue()
                st.download_button(
                    "📥 Descargar Excel",
                    excel_data,
                    nombre_archivo.replace('.csv', '.xlsx'),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel"
                )
                
        except Exception as e:
            st.error(f"Error al generar reporte: {e}")

# -------------------------------
# FUNCIÓN PRINCIPAL
# -------------------------------
def run(usuario):
    st.title("🎯 HOPSA - Gestión de Ventas y Métricas")
    st.caption(f"Usuario: {usuario}")
    
    # Menú lateral para HOPSA
    opcion = st.sidebar.radio(
        "Opciones HOPSA",
        ["1. Actualizar Agentes", "2. Subir Información", "3. Descargar Reportes"]
    )
    
    if opcion == "1. Actualizar Agentes":
        actualizar_agentes()
    elif opcion == "2. Subir Información":
        subir_informacion()
    elif opcion == "3. Descargar Reportes":
        descargar_reportes()

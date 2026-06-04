import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from datetime import datetime

def run(usuario, tipo_carga):
    """Módulo de control de almuerzos HX"""
    
    st.title("🍽️ HOPSA - Control de Almuerzos HX")
    st.caption(f"Usuario: {usuario} | Módulo: {tipo_carga}")
    st.markdown("---")

# ========== CONFIGURACIÓN ==========
st.set_page_config(page_title="HOPSA - Control de Almuerzos", layout="wide")

# Usar secrets de Streamlit Cloud
SERVICE_ACCOUNT_INFO = st.secrets["gcp_service_account"]
PROJECT_ID = SERVICE_ACCOUNT_INFO["project_id"]
SHEET_NAME = "HOPSA_Almuerzos_Hoy"
DATASET_ID = "reportes"
TABLE_ID = "almuerzos_hx"

# Columnas requeridas
COLUMNAS_REQUERIDAS = ['Agente', 'ALMUERZO']

# ========== FUNCIONES ==========

def get_bigquery_client():
    return bigquery.Client.from_service_account_info(SERVICE_ACCOUNT_INFO)

def get_google_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scope)
    return gspread.authorize(creds)

def parse_time_to_seconds(time_str):
    """Convierte formato HH:MM:SS a segundos"""
    try:
        if pd.isna(time_str) or time_str == "" or time_str == "00:00:00":
            return 0
        parts = str(time_str).split(':')
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
            return hours * 3600 + minutes * 60 + seconds
        return 0
    except:
        return 0

def evaluate_lunch_advanced(lunch_str):
    """Evalúa el almuerzo y devuelve métricas detalladas"""
    segundos = parse_time_to_seconds(lunch_str)
    minutos = segundos / 60
    limite_segundos = 30 * 60
    
    if segundos == 0:
        return {
            "almuerzo_original": "00:00:00",
            "almuerzo_segundos": 0,
            "almuerzo_minutos": 0,
            "exceso_segundos": 0,
            "exceso_minutos": 0,
            "estado": "SIN_ALMUERZO",
            "icono": "❌",
            "display": "❌ Sin almuerzo"
        }
    elif segundos <= limite_segundos:
        return {
            "almuerzo_original": lunch_str,
            "almuerzo_segundos": segundos,
            "almuerzo_minutos": round(minutos, 1),
            "exceso_segundos": 0,
            "exceso_minutos": 0,
            "estado": "OK",
            "icono": "🟢",
            "display": f"🟢 {int(minutos)} min (OK)"
        }
    else:
        exceso_segundos = segundos - limite_segundos
        exceso_minutos = exceso_segundos / 60
        return {
            "almuerzo_original": lunch_str,
            "almuerzo_segundos": segundos,
            "almuerzo_minutos": round(minutos, 1),
            "exceso_segundos": exceso_segundos,
            "exceso_minutos": round(exceso_minutos, 1),
            "estado": "EXCESO",
            "icono": "🔴",
            "display": f"🔴 {int(minutos)} min (Exceso {round(exceso_minutos, 1)} min)"
        }

def validar_columnas(df):
    """Valida que el archivo tenga las columnas requeridas"""
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        st.error(f"❌ El archivo no contiene las columnas requeridas: {', '.join(faltantes)}")
        st.stop()
    return True

def normalizar_nombre_agente(nombre):
    """Normaliza el nombre del agente (mayúsculas, sin espacios dobles, sin espacios al inicio/final)"""
    return " ".join(str(nombre).upper().strip().split())

def filtrar_agentes_hx(df):
    """Filtra solo agentes que terminan con HX"""
    # Trabajar con una copia para no modificar el original
    df_hx = df.copy()
    df_hx['Agente_Normalizado'] = df_hx['Agente'].apply(normalizar_nombre_agente)
    
    # Solo agentes que TERMINAN con HX (no que lo contengan)
    termina_con_hx = df_hx['Agente_Normalizado'].str.endswith("HX", na=False)
    df_hx_filtrado = df_hx[termina_con_hx].copy()
    
    # Normalizar el nombre para guardar
    df_hx_filtrado['Agente'] = df_hx_filtrado['Agente_Normalizado']
    
    # Mostrar excluidos (para depuración)
    excluidos = df_hx[~termina_con_hx]
    if len(excluidos) > 0:
        with st.expander(f"ℹ️ {len(excluidos)} registros ignorados (no terminan con HX)"):
            st.dataframe(excluidos[['Agente']].head(10))
    
    return df_hx_filtrado

def verificar_fecha_existente(fecha_reporte):
    """Verifica si ya existe información para la fecha en BigQuery"""
    client = get_bigquery_client()
    query = f"""
    SELECT COUNT(*) as total
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE fecha_reporte = @fecha
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("fecha", "DATE", fecha_reporte)]
    )
    result = client.query(query, job_config=job_config).to_dataframe()
    return result['total'].iloc[0] > 0

def actualizar_google_sheets_hoy(records, fecha_reporte):
    """Actualiza Google Sheets solo si la fecha es hoy"""
    hoy = datetime.now().date()
    
    if fecha_reporte != hoy:
        st.info(f"📅 La fecha {fecha_reporte} no es hoy. Solo se guardará en BigQuery (no se actualiza la hoja visual)")
        return 0
    
    client = get_google_sheets_client()
    
    try:
        sheet = client.open(SHEET_NAME).sheet1
        sheet.clear()
    except:
        sheet = client.create(SHEET_NAME).sheet1
    
    # Preparar datos
    data = [["Fecha", "Agente", "Almuerzo", "Estado", "Exceso (min)"]]
    for record in records:
        data.append([
            fecha_reporte.strftime("%Y-%m-%d"),
            record['Agente'],
            record['almuerzo_original'],
            record['display'],
            record['exceso_minutos']
        ])
    
    sheet.update(data, value_input_option="USER_ENTERED")
    st.success(f"📊 Hoja de hoy actualizada en Google Sheets")
    return len(records)

def guardar_en_bigquery(records, fecha_reporte, nombre_archivo, modo_actualizar=False):
    """Guarda o actualiza los registros en BigQuery"""
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    rows_to_insert = []
    fecha_carga = datetime.now()
    
    for record in records:
        rows_to_insert.append({
            "fecha_reporte": fecha_reporte,
            "agente": record['Agente'],
            "almuerzo_original": record['almuerzo_original'],
            "almuerzo_segundos": record['almuerzo_segundos'],
            "almuerzo_minutos": record['almuerzo_minutos'],
            "exceso_segundos": record['exceso_segundos'],
            "exceso_minutos": record['exceso_minutos'],
            "estado": record['estado'],
            "nombre_archivo": nombre_archivo,
            "fecha_carga": fecha_carga.isoformat()
        })
    
    if modo_actualizar:
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE fecha_reporte = @fecha
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("fecha", "DATE", fecha_reporte)]
        )
        client.query(delete_query, job_config=job_config).result()
        st.info(f"🗑️ Registros anteriores eliminados para {fecha_reporte}")
    
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        st.error(f"Error en BigQuery: {errors}")
        return False
    else:
        st.success(f"✅ {len(rows_to_insert)} registros guardados en BigQuery")
        st.info(f"📄 Archivo: {nombre_archivo}")
        st.info(f"🕒 Fecha de carga: {fecha_carga.strftime('%Y-%m-%d %H:%M:%S')}")
        return True

# ========== INTERFAZ DE STREAMLIT ==========

st.title("🍽️ HOPSA - Control de Almuerzos")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("📁 Cargar Reporte")
    uploaded_file = st.file_uploader("Selecciona el archivo Excel/CSV", type=['xlsx', 'csv'])
    
    st.markdown("---")
    st.caption("Los datos se guardan en BigQuery y se actualiza la hoja visual del día")

# Main content
if uploaded_file is not None:
    try:
        # Leer archivo
        if uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)
        else:
            df = pd.read_csv(uploaded_file)
        
        st.success(f"✅ Archivo cargado: {len(df)} filas | {uploaded_file.name}")
        
        # Validar columnas
        validar_columnas(df)
        
        # Fecha del reporte
        st.markdown("### 📅 Fecha del Reporte")
        fecha_reporte = st.date_input(
            "Selecciona la fecha del reporte",
            value=datetime.now(),
            help="Selecciona la fecha a la que corresponde este reporte"
        )
        
        st.info(f"📅 Fecha seleccionada: {fecha_reporte.strftime('%d/%m/%Y')}")
        
        # Filtrar agentes HX
        df_hx = filtrar_agentes_hx(df)
        
        if df_hx.empty:
            st.error("❌ No se encontraron agentes HX válidos en el archivo")
            st.stop()
        
        # Procesar registros (sin eliminar duplicados, el archivo es la fuente de verdad)
        records = []
        for _, row in df_hx.iterrows():
            lunch_data = evaluate_lunch_advanced(row.get('ALMUERZO', '00:00:00'))
            records.append({
                "Agente": row['Agente'],
                "almuerzo_original": lunch_data['almuerzo_original'],
                "almuerzo_segundos": lunch_data['almuerzo_segundos'],
                "almuerzo_minutos": lunch_data['almuerzo_minutos'],
                "exceso_segundos": lunch_data['exceso_segundos'],
                "exceso_minutos": lunch_data['exceso_minutos'],
                "estado": lunch_data['estado'],
                "display": lunch_data['display']
            })
        
        # Mostrar previsualización
        st.markdown("### 📊 Previsualización del Reporte")
        
        preview_data = []
        for r in records:
            preview_data.append({
                "Agente": r['Agente'],
                "Almuerzo": r['almuerzo_original'],
                "Estado": r['display'],
                "Exceso (min)": r['exceso_minutos']
            })
        
        df_preview = pd.DataFrame(preview_data)
        
        # Estadísticas
        total = len(df_preview)
        dentro = len([r for r in records if r['estado'] == 'OK'])
        exceso = len([r for r in records if r['estado'] == 'EXCESO'])
        sin_almuerzo = len([r for r in records if r['estado'] == 'SIN_ALMUERZO'])
        exceso_promedio = df_preview[df_preview['Exceso (min)'] > 0]['Exceso (min)'].mean() if exceso > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Agentes HX", total)
        col2.metric("🟢 Dentro del tiempo", dentro)
        col3.metric("🔴 Con exceso", exceso, delta=f"{exceso_promedio:.1f} min promedio")
        col4.metric("❌ Sin almuerzo", sin_almuerzo)
        
        st.dataframe(df_preview, use_container_width=True)
        
        # Verificar si ya existe información para esta fecha
        fecha_str = fecha_reporte.strftime("%Y-%m-%d")
        existe = verificar_fecha_existente(fecha_str)
        
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if existe:
                if st.button("🔄 Actualizar información existente", type="primary"):
                    with st.spinner("Actualizando BigQuery..."):
                        if guardar_en_bigquery(records, fecha_str, uploaded_file.name, modo_actualizar=True):
                            actualizar_google_sheets_hoy(records, fecha_reporte)
                            st.balloons()
                            st.success("✅ ¡Datos actualizados correctamente!")
            else:
                if st.button("💾 Guardar en BigQuery", type="primary"):
                    with st.spinner("Guardando en BigQuery..."):
                        if guardar_en_bigquery(records, fecha_str, uploaded_file.name, modo_actualizar=False):
                            actualizar_google_sheets_hoy(records, fecha_reporte)
                            st.balloons()
                            st.success("✅ ¡Datos guardados correctamente!")
        
        with col2:
            if st.button("📊 Ver Dashboard en Looker"):
                st.info("Dashboard disponible en: [link a Looker Studio]")
        
        with col3:
            if st.button("📋 Solo previsualizar"):
                st.info("Puedes revisar los datos. No se guardó nada aún.")
    
    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")

else:
    st.info("👈 Sube un archivo Excel o CSV para comenzar")
    
    with st.expander("📖 Ver formato esperado"):
        st.markdown("""
        El archivo debe contener las siguientes columnas:
        - **Agente** - Nombre del agente (debe terminar con HX)
        - **ALMUERZO** - Tiempo de almuerzo en formato HH:MM:SS
        
        **Ejemplo:**
        Agente | ALMUERZO
        ELBA ORTEGA HX | 00:46:24
        VERONICA ALVENDAS HX| 00:52:41
        """)

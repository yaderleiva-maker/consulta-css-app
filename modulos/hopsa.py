import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# ========== CONFIGURACIÓN ==========
PROJECT_ID = "proyecto-css-panama"
DATASET_ID = "hopsa"
TABLE_ID = "almuerzos_hx"  # Tabla de histórico (append-only)
COLUMNAS_REQUERIDAS = ['Agente', 'ALMUERZO']

def get_bigquery_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

def parse_time_to_seconds(time_str):
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
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        st.error(f"❌ El archivo no contiene las columnas requeridas: {', '.join(faltantes)}")
        st.stop()
    return True

def normalizar_nombre_agente(nombre):
    return " ".join(str(nombre).upper().strip().split())

def filtrar_agentes_hx(df):
    df_hx = df.copy()
    df_hx['Agente_Normalizado'] = df_hx['Agente'].apply(normalizar_nombre_agente)
    termina_con_hx = df_hx['Agente_Normalizado'].str.endswith("HX", na=False)
    df_hx_filtrado = df_hx[termina_con_hx].copy()
    df_hx_filtrado['Agente'] = df_hx_filtrado['Agente_Normalizado']
    
    # Mostrar excluidos
    excluidos = df_hx[~termina_con_hx]
    if len(excluidos) > 0:
        with st.expander(f"ℹ️ {len(excluidos)} registros ignorados (no terminan con HX)"):
            st.dataframe(excluidos[['Agente']].head(10))
    
    return df_hx_filtrado

def guardar_en_bigquery(records, fecha_reporte, nombre_archivo):
    """Siempre inserta nuevos registros (append-only)"""
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
    
    # Insertar siempre (append)
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        st.error(f"Error en BigQuery: {errors}")
        return False
    else:
        st.success(f"✅ {len(rows_to_insert)} registros guardados (versión {fecha_carga.strftime('%H:%M:%S')})")
        return True

def mostrar_carga_archivos(usuario):
    """Función de carga de archivos (append-only)"""
    st.markdown("### 📁 Cargar Reporte de Almuerzos")
    st.caption(f"Usuario: {usuario}")
    st.info("💡 **Nota:** Si cometiste un error, solo sube el archivo corregido. El sistema guardará una nueva versión y el dashboard mostrará siempre la última.")
    
    uploaded_file = st.file_uploader("Sube el archivo Excel/CSV", type=['xlsx', 'csv'])
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.xlsx'):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_csv(uploaded_file)
            
            st.success(f"✅ Archivo cargado: {len(df)} filas | {uploaded_file.name}")
            validar_columnas(df)
            
            st.markdown("### 📅 Fecha del Reporte")
            st.caption("⚠️ **Importante:** Esta es la fecha a la que corresponde el reporte")
            
            fecha_reporte = st.date_input(
                "Selecciona la fecha del reporte",
                value=None,
                help="Ejemplo: Si el reporte es de los almuerzos del 3 de junio, selecciona 2026-06-03"
            )
            
            if fecha_reporte is None:
                st.warning("⚠️ **Debes seleccionar una fecha antes de continuar**")
                st.stop()
            
            st.info(f"📅 Fecha del reporte seleccionada: **{fecha_reporte.strftime('%d/%m/%Y')}**")
            
            df_hx = filtrar_agentes_hx(df)
            
            if df_hx.empty:
                st.error("❌ No se encontraron agentes HX válidos en el archivo")
                st.stop()
            
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
            
            # Previsualización
            st.markdown("### 📊 Previsualización")
            preview_data = [{"Agente": r['Agente'], "Almuerzo": r['almuerzo_original'], "Estado": r['display'], "Exceso (min)": r['exceso_minutos']} for r in records]
            df_preview = pd.DataFrame(preview_data)
            
            total = len(df_preview)
            dentro = len([r for r in records if r['estado'] == 'OK'])
            exceso = len([r for r in records if r['estado'] == 'EXCESO'])
            sin_almuerzo = len([r for r in records if r['estado'] == 'SIN_ALMUERZO'])
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Agentes HX", total)
            col2.metric("🟢 Dentro del tiempo", dentro)
            col3.metric("🔴 Con exceso", exceso)
            col4.metric("❌ Sin almuerzo", sin_almuerzo)
            
            st.dataframe(df_preview, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Guardar nueva versión", type="primary"):
                    with st.spinner("Guardando..."):
                        fecha_str = fecha_reporte.strftime("%Y-%m-%d")
                        if guardar_en_bigquery(records, fecha_str, uploaded_file.name):
                            st.balloons()
                            st.success("✅ ¡Datos guardados correctamente!")
                            st.info("📌 Si cometiste un error, puedes subir el archivo corregido y se guardará como una nueva versión")
            
            with col2:
                if st.button("📋 Solo previsualizar"):
                    st.info("No se guardó nada aún")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ========== FUNCIÓN PRINCIPAL ==========
def run(usuario, tipo_carga):
    st.title("🍽️ Control de Almuerzos HX")
    mostrar_carga_archivos(usuario)

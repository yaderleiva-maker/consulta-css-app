import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACION
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"
TABLA_GESTIONES = f"`{PROYECTO_BQ}.gestiones_jamar`"
TABLA_MAPEO = f"`{PROYECTO_BQ}.mapeo_codigos_gestion`"

# ============================================================
# FUNCIONES DE NORMALIZACION
# ============================================================

def normalizar_texto(valor):
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    if texto == '' or texto == 'nan' or texto == 'None' or texto == 'NULL':
        return None
    texto = texto.replace("'", "''")
    texto = texto.replace("\\", "\\\\")
    return texto

def normalizar_numero(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        limpiar = re.sub(r'[^\d.,-]', '', valor)
        limpiar = limpiar.replace(',', '.')
        try:
            return float(limpiar)
        except:
            return None
    return None

def normalizar_fecha_hora(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()
    if isinstance(valor, str):
        valor = str(valor).strip()
        if not valor or valor == 'nan' or valor == 'None':
            return None
        for fmt in ['%d-%m-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']:
            try:
                return datetime.strptime(valor, fmt).isoformat()
            except:
                continue
    return None

def normalizar_fecha(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.date().isoformat()
    if isinstance(valor, str):
        valor = str(valor).strip()
        if not valor or valor == 'nan' or valor == 'None':
            return None
        for fmt in ['%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y']:
            try:
                return datetime.strptime(valor, fmt).date().isoformat()
            except:
                continue
    return None

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_mapeo_codigos():
    try:
        query = f"""
            SELECT codigo_gestion, mejor_gestion_jamar, resultado
            FROM {TABLA_MAPEO}
        """
        df = ejecutar_query(query)
        if df.empty:
            return {}
        return dict(zip(df['codigo_gestion'], zip(df['mejor_gestion_jamar'], df['resultado'])))
    except Exception as e:
        st.warning(f"No se pudo obtener mapeo: {e}")
        return {}

def verificar_cartera_cargada():
    try:
        query = f"""
            SELECT COUNT(*) AS total
            FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        if not df.empty:
            return df['total'].iloc[0] > 0
        return False
    except:
        return False

def obtener_total_historico():
    try:
        query = f"""
            SELECT COUNT(*) AS total
            FROM {TABLA_GESTIONES}
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        if not df.empty:
            return df['total'].iloc[0]
        return 0
    except:
        return 0

def obtener_ultima_fecha_carga():
    try:
        query = f"""
            SELECT 
                MAX(fechahoragestion) AS ultima_fecha,
                COUNT(*) AS total
            FROM {TABLA_GESTIONES}
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        if not df.empty and df['ultima_fecha'].iloc[0] is not None:
            return {
                'fecha': df['ultima_fecha'].iloc[0],
                'total': df['total'].iloc[0]
            }
        return None
    except:
        return None

def obtener_historial_cargas():
    try:
        query = f"""
            SELECT 
                id_carga,
                archivo_nombre,
                fecha_carga,
                total_registros,
                registros_guardados,
                errores
            FROM `{PROYECTO_BQ}.historial_cargas_gestiones`
            WHERE id_proyecto = '{PROYECTO_ID}'
            ORDER BY fecha_carga DESC
            LIMIT 20
        """
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.warning(f"Error al consultar historial de cargas: {e}")
        return pd.DataFrame()

def obtener_resumen_por_fecha_carga():
    """Resumen de gestiones por fecha de carga (para historial visual)"""
    query = f"""
        SELECT
            DATE(fecha_carga) AS fecha_carga,
            COUNT(*) AS total_registros
        FROM `{PROYECTO_BQ}.gestiones_jamar`
        WHERE id_proyecto = '{PROYECTO_ID}'
        GROUP BY 1
        ORDER BY 1 DESC
    """
    return ejecutar_query(query)

def guardar_gestiones_jamar(df, proyecto_id, archivo_nombre):
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    registros_guardados = 0
    detalles = []
    
    # Generar ID de carga único para este lote
    id_carga = str(uuid.uuid4())
    
    mapeo = obtener_mapeo_codigos()
    if not mapeo:
        st.warning(
            "No se encontró mapeo de códigos. "
            "Las gestiones se guardarán sin mejor_gestion_jamar ni resultado_gestion."
        )
        mapeo = {}

    if df.empty:
        st.warning("El archivo no contiene datos.")
        return 0, total, "Archivo vacio"
    
    registros = []
    
    for idx, row in df.iterrows():
        try:
            llave_raw = row.get('Llave')
            
            codigo_agencia = normalizar_texto(
                row.get("Codigo de la Agencia", row.get("Código de la Agencia"))
            )
            numero_cuenta = normalizar_texto(
                row.get("Número de Cuenta", row.get("Numero de Cuenta"))
            )
            codigo_cliente = normalizar_texto(
                row.get("Codigo del Cliente", row.get("Código del Cliente"))
            )
            codigo_cobrador = normalizar_texto(
                row.get("Codigo del cobrador", row.get("Código del cobrador"))
            )
            
            if pd.isna(llave_raw) or not str(llave_raw).strip():
                if codigo_agencia and numero_cuenta:
                    llave = f"{codigo_agencia}{numero_cuenta}"
                else:
                    llave = None
            else:
                llave = normalizar_texto(llave_raw)
            
            if not llave:
                errores += 1
                detalles.append(f"Fila {idx+2}: No se pudo generar llave")
                continue
            
            codigo_gestion = normalizar_texto(row.get('codigo_gestion'))
            
            mejor_gestion = None
            resultado = None
            if codigo_gestion and codigo_gestion in mapeo:
                mejor_gestion, resultado = mapeo[codigo_gestion]
            
            fechahora = normalizar_fecha_hora(row.get('fechahoragestion'))
            fechapromesa = normalizar_fecha(row.get('fechapromesa'))
            fecha = normalizar_fecha(row.get('Fecha'))
            
            valorpromesa = normalizar_numero(row.get('valorpromesa'))
            min_prioridad = None
            try:
                val = row.get('MinDePrioridad')
                if pd.notna(val):
                    min_prioridad = int(float(val))
            except:
                pass
            
            id_gestion = str(uuid.uuid4())
            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            registro = {
                'id_gestion': id_gestion,
                'id_carga': id_carga,
                'id_proyecto': PROYECTO_ID,
                'llave': llave,
                'codigo_agencia': codigo_agencia,
                'numero_cuenta': numero_cuenta,
                'codigo_cliente': codigo_cliente,
                'fechahoragestion': fechahora,
                'codigo_gestion': codigo_gestion,
                'observacion': normalizar_texto(row.get('Observacion')),
                'codigo_cobrador': codigo_cobrador,
                'area_gestion': normalizar_texto(row.get('area_gestion')),
                'tipo_gestion': normalizar_texto(row.get('tipo_gestion')),
                'numeromarcado': normalizar_texto(row.get('numeromarcado')),
                'tipo_telefono': normalizar_texto(row.get('tipo_telefono')),
                'fechapromesa': fechapromesa,
                'valorpromesa': valorpromesa,
                'mejor_gestion_jamar': mejor_gestion,
                'resultado_gestion': resultado,
                'lugar_contacto': normalizar_texto(row.get('lugar_contacto')),
                'tipo_contacto': normalizar_texto(row.get('tipo_contacto')),
                'clave': normalizar_texto(row.get('Clave')),
                'fecha': fecha,
                'min_de_prioridad': min_prioridad,
                'clave_min': normalizar_texto(row.get('ClaveMin')),
                'fecha_carga': ahora,
                'created_at': ahora,
                'updated_at': ahora
            }
            registros.append(registro)
            
        except Exception as e:
            errores += 1
            st.warning(f"Error en fila {idx+2}: {str(e)}")
    
    if not registros:
        st.warning("No hay datos validos para insertar")
        return 0, total, "No hay datos validos"
    
    df_insert = pd.DataFrame(registros)
    
    # ============================================================
    # CONVERTIR FECHAS A TIPOS REALES DE BIGQUERY
    # ============================================================
    
    try:
        if 'fechahoragestion' in df_insert.columns:
            df_insert["fechahoragestion"] = pd.to_datetime(
                df_insert["fechahoragestion"], errors="coerce"
            )
        
        for columna in ["fechapromesa", "fecha"]:
            if columna in df_insert.columns:
                df_insert[columna] = pd.to_datetime(
                    df_insert[columna], errors="coerce"
                ).dt.date
                
        for columna in ["fecha_carga", "created_at", "updated_at"]:
            if columna in df_insert.columns:
                df_insert[columna] = pd.to_datetime(
                    df_insert[columna],
                    errors="coerce",
                    utc=True,
                )
    except Exception as e:
        st.error(f"Error al convertir fechas: {e}")
        return 0, total, f"Error en fechas: {e}"
    
    # ============================================================
    # CONEXIÓN A BIGQUERY
    # ============================================================
    
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        destino = f"{PROYECTO_BQ}.gestiones_jamar"
        
        try:
            tabla_destino = client.get_table(destino)
        except Exception as e:
            st.error(f"La tabla no existe: {e}")
            return 0, total, f"Tabla no existe: {e}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=tabla_destino.schema,
        )
        
        with st.spinner(f"Subiendo {len(df_insert)} registros a BigQuery..."):
            job = client.load_table_from_dataframe(
                df_insert,
                destino,
                job_config=job_config,
            )
            job.result(timeout=180)
            
            rows_loaded = job.output_rows or len(df_insert)
            st.success(f"✅ BigQuery terminó el job. Filas cargadas: {rows_loaded}")
            registros_guardados = rows_loaded
            
            # ============================================================
            # GUARDAR HISTORIAL DE CARGA
            # ============================================================
            try:
                historial_rows = [{
                    "id_carga": id_carga,
                    "id_proyecto": PROYECTO_ID,
                    "archivo_nombre": archivo_nombre,
                    "fecha_carga": datetime.now().isoformat(),
                    "total_registros": total,
                    "registros_guardados": registros_guardados,
                    "errores": errores,
                }]
                
                errores_historial = client.insert_rows_json(
                    f"{PROYECTO_BQ}.historial_cargas_gestiones",
                    historial_rows
                )
                
                if errores_historial:
                    st.warning(f"⚠️ Gestiones cargadas, pero no se pudo guardar historial: {errores_historial}")
                else:
                    st.info(f"📋 Historial actualizado: {archivo_nombre}")
                    
            except Exception as e_historial:
                st.warning(f"⚠️ Gestiones cargadas, pero error al guardar historial: {e_historial}")
            
    except Exception as e:
        st.error(f"Error al insertar en BigQuery: {str(e)}")
        return 0, total, f"Error en BigQuery: {e}"
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

def leer_archivo_gestiones(uploaded_file):
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names
        
        if 'CORREOS & WHATSAPP' in sheet_names and 'LLAMADAS' in sheet_names:
            df_correos = pd.read_excel(uploaded_file, sheet_name='CORREOS & WHATSAPP')
            df_llamadas = pd.read_excel(uploaded_file, sheet_name='LLAMADAS')
            df_combinado = pd.concat([df_correos, df_llamadas], ignore_index=True)
        else:
            st.warning(f"Usando la primera hoja: {sheet_names[0]}")
            df_combinado = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
        
        df_combinado.columns = df_combinado.columns.str.strip()
        return df_combinado
        
    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")
        raise

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    st.markdown("""
    <style>
        .main-header { font-size: 22px; font-weight: 600; color: #1a1a1a; margin-bottom: 4px; }
        .sub-header { font-size: 14px; color: #6b6b6b; margin-bottom: 16px; }
        .card { background-color: #ffffff; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #f0f0f0; margin-bottom: 16px; }
        .card-title { font-size: 15px; font-weight: 500; color: #1a1a1a; margin-bottom: 8px; }
        .selected-file { background-color: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 10px 16px; display: flex; align-items: center; gap: 10px; }
        .selected-file .file-name { font-weight: 500; color: #166534; }
        .status-badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 500; }
        .status-badge.success { background-color: #dcfce7; color: #166534; }
        .status-badge.warning { background-color: #fef3c7; color: #92400e; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Carga de Gestiones - Jamar</div>', unsafe_allow_html=True)
    st.mark

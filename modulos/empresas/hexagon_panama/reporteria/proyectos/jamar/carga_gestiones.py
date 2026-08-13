import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"
TABLA_GESTIONES = f"`{PROYECTO_BQ}.gestiones_jamar`"
TABLA_MAPEO = f"`{PROYECTO_BQ}.mapeo_codigos_gestion`"

# ============================================================
# FUNCIONES DE NORMALIZACIÓN (MEJORADAS)
# ============================================================

def normalizar_texto(valor):
    """Normaliza texto y escapa caracteres especiales para SQL"""
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    if texto == '' or texto == 'nan' or texto == 'None' or texto == 'NULL':
        return None
    # Escapar comillas simples (') y barras invertidas
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

def formatear_valor_sql(valor):
    """Formatea un valor para SQL: si es None devuelve 'NULL', si no devuelve comillas"""
    if valor is None:
        return 'NULL'
    if isinstance(valor, (int, float)):
        return str(valor)
    if isinstance(valor, str):
        return f"'{valor}'"
    return f"'{valor}'"

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
            st.warning("⚠️ No se encontró la tabla de mapeo.")
            return {}
        return dict(zip(df['codigo_gestion'], zip(df['mejor_gestion_jamar'], df['resultado'])))
    except Exception as e:
        st.error(f"❌ Error al obtener mapeo: {e}")
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

def obtener_ultimas_fechas_carga():
    try:
        query = f"""
            SELECT 
                DATE(fechahoragestion) AS fecha,
                COUNT(*) AS total_gestiones,
                COUNT(DISTINCT llave) AS cuentas_gestionadas,
                COUNT(DISTINCT codigo_cliente) AS clientes_gestionados
            FROM {TABLA_GESTIONES}
            WHERE id_proyecto = '{PROYECTO_ID}'
            GROUP BY DATE(fechahoragestion)
            ORDER BY fecha DESC
            LIMIT 10
        """
        df = ejecutar_query(query)
        return df
    except:
        return pd.DataFrame()

def guardar_gestiones_jamar(df, proyecto_id):
    """Guarda las gestiones de Jamar en BigQuery usando el cliente de BigQuery directamente."""
    import time
    start_time = time.time()
    from google.cloud import bigquery
    from google.oauth2 import service_account
    
    total = len(df)
    errores = 0
    registros_guardados = 0
    
    # Obtener mapeo de códigos
    mapeo = obtener_mapeo_codigos()
    if not mapeo:
        st.error("❌ No se pudo obtener el mapeo de códigos.")
        return 0, total, "Error: Tabla de mapeo vacía"

    if df.empty:
        st.warning("⚠️ El archivo no contiene datos.")
        return 0, total, "Archivo vacío"
    
    # Crear lista de diccionarios para insertar
    registros = []
    
    for idx, row in df.iterrows():
        try:
            # Extraer y normalizar campos
            llave_raw = row.get('Llave')
            codigo_agencia = normalizar_texto(row.get('Codigo de la Agencia'))
            numero_cuenta = normalizar_texto(row.get('Número de Cuenta'))
            
            if pd.isna(llave_raw) or not str(llave_raw).strip():
                if codigo_agencia and numero_cuenta:
                    llave = f"{codigo_agencia}{numero_cuenta}"
                else:
                    llave = None
            else:
                llave = normalizar_texto(llave_raw)
            
            codigo_cliente = normalizar_texto(row.get('Codigo del Cliente'))
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
                val = row.get('MínDePrioridad')
                if pd.notna(val):
                    min_prioridad = int(float(val))
            except:
                pass
            
            registro = {
                'id_gestion': str(uuid.uuid4()),
                'id_proyecto': PROYECTO_ID,
                'llave': llave,
                'codigo_agencia': codigo_agencia,
                'numero_cuenta': numero_cuenta,
                'codigo_cliente': codigo_cliente,
                'fechahoragestion': fechahora,
                'codigo_gestion': codigo_gestion,
                'observacion': normalizar_texto(row.get('Observación')),
                'codigo_cobrador': normalizar_texto(row.get('Codigo del cobrador')),
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
                'fecha_carga': datetime.now().isoformat(),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            registros.append(registro)
            
        except Exception as e:
            errores += 1
            st.warning(f"⚠️ Error en fila {idx+2}: {str(e)}")
    
    if not registros:
        st.warning("⚠️ No hay datos válidos para insertar")
        return 0, total, "No hay datos válidos"
    
    # Convertir a DataFrame para subir a BigQuery
    df_insert = pd.DataFrame(registros)
    
    # Conectar a BigQuery con credenciales
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        # Insertar usando load_table_from_dataframe (más robusto)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
            schema=[
                bigquery.SchemaField("id_gestion", "STRING"),
                bigquery.SchemaField("id_proyecto", "STRING"),
                bigquery.SchemaField("llave", "STRING"),
                bigquery.SchemaField("codigo_agencia", "STRING"),
                bigquery.SchemaField("numero_cuenta", "STRING"),
                bigquery.SchemaField("codigo_cliente", "STRING"),
                bigquery.SchemaField("fechahoragestion", "TIMESTAMP"),
                bigquery.SchemaField("codigo_gestion", "STRING"),
                bigquery.SchemaField("observacion", "STRING"),
                bigquery.SchemaField("codigo_cobrador", "STRING"),
                bigquery.SchemaField("area_gestion", "STRING"),
                bigquery.SchemaField("tipo_gestion", "STRING"),
                bigquery.SchemaField("numeromarcado", "STRING"),
                bigquery.SchemaField("tipo_telefono", "STRING"),
                bigquery.SchemaField("fechapromesa", "DATE"),
                bigquery.SchemaField("valorpromesa", "FLOAT64"),
                bigquery.SchemaField("mejor_gestion_jamar", "STRING"),
                bigquery.SchemaField("resultado_gestion", "STRING"),
                bigquery.SchemaField("lugar_contacto", "STRING"),
                bigquery.SchemaField("tipo_contacto", "STRING"),
                bigquery.SchemaField("clave", "STRING"),
                bigquery.SchemaField("fecha", "DATE"),
                bigquery.SchemaField("min_de_prioridad", "INT64"),
                bigquery.SchemaField("clave_min", "STRING"),
                bigquery.SchemaField("fecha_carga", "TIMESTAMP"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ]
        )
        
        with st.spinner(f"📤 Subiendo {len(df_insert)} registros a BigQuery..."):
            job = client.load_table_from_dataframe(
                df_insert,
                f"{PROYECTO_BQ}.gestiones_jamar",
                job_config=job_config
            )
            job.result()  # Esperar a que termine
            
            # Verificar cuántos se insertaron
            table = client.get_table(f"{PROYECTO_BQ}.gestiones_jamar")
            registros_guardados = table.num_rows
            
            # También insertar el conteo actualizado
            st.success(f"✅ {registros_guardados} registros guardados correctamente")
            
    except Exception as e:
        st.error(f"❌ Error al insertar en BigQuery: {str(e)}")
        st.exception(e)
        return 0, total, f"Error en BigQuery: {e}"
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

# ============================================================
# FUNCIÓN PARA LEER AMBAS HOJAS DEL EXCEL
# ============================================================

def leer_archivo_gestiones(uploaded_file):
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names
        
        if 'CORREOS & WHATSAPP' in sheet_names and 'LLAMADAS' in sheet_names:
            df_correos = pd.read_excel(uploaded_file, sheet_name='CORREOS & WHATSAPP')
            df_llamadas = pd.read_excel(uploaded_file, sheet_name='LLAMADAS')
            df_combinado = pd.concat([df_correos, df_llamadas], ignore_index=True)
        else:
            st.warning(f"⚠️ Usando la primera hoja: {sheet_names[0]}")
            df_combinado = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
        
        df_combinado.columns = df_combinado.columns.str.strip()
        return df_combinado
        
    except Exception as e:
        st.error(f"❌ Error al leer el archivo: {e}")
        raise

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para cargar gestiones diarias de Jamar"""
    
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
    
    st.markdown('<div class="main-header">📞 Carga de Gestiones - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el reporte de gestiones diarias. El sistema procesará ambas hojas (CORREOS & WHATSAPP y LLAMADAS).</div>', unsafe_allow_html=True)
    
    # Estado del sistema
    st.markdown('<div class="card">', unsafe_allow_html=True)
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<div class="card-title">📋 Estado del sistema</div>', unsafe_allow_html=True)
    with col2:
        cartera_cargada = verificar_cartera_cargada()
        if cartera_cargada:
            st.markdown('<span class="status-badge success">✅ Cartera cargada</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-badge warning">⚠️ Sin cartera</span>', unsafe_allow_html=True)
    
    # Últimas cargas
    df_resumen = obtener_ultimas_fechas_carga()
    if not df_resumen.empty:
        st.markdown("#### 📊 Últimas cargas de gestiones")
        for _, row in df_resumen.iterrows():
            fecha = row['fecha'].strftime('%d/%m/%Y') if hasattr(row['fecha'], 'strftime') else str(row['fecha'])
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.markdown(f"📅 **{fecha}**")
            with col2:
                st.markdown(f"📞 {row['total_gestiones']:,} gestiones")
            with col3:
                st.markdown(f"👤 {row['clientes_gestionados']:,} clientes")
            st.markdown("---")
    else:
        st.info("ℹ️ No hay cargas de gestiones registradas aún.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Área de carga
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">📤 Subir archivo de gestiones</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo Excel",
        type=["xlsx", "xls"],
        key="gestiones_uploader",
        label_visibility="collapsed"
    )
    
    if uploaded_file is not None:
        size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        st.markdown(f"""
        <div class="selected-file">
            <span>📄</span>
            <span class="file-name">{uploaded_file.name}</span>
            <span class="file-size">({size_mb:.1f} MB)</span>
        </div>
        """, unsafe_allow_html=True)
        
        with st.spinner("📊 Procesando archivo..."):
            try:
                df = leer_archivo_gestiones(uploaded_file)
                
                st.markdown("---")
                st.markdown("#### 📊 Vista previa del archivo")
                st.dataframe(df.head(5), use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    codigos = df['codigo_gestion'].nunique() if 'codigo_gestion' in df.columns else 0
                    st.metric("Códigos únicos", f"{codigos:,}")
                with col3:
                    if 'fechahoragestion' in df.columns and not df.empty:
                        try:
                            fecha = pd.to_datetime(df['fechahoragestion'].iloc[0])
                            st.metric("Fecha del archivo", fecha.strftime('%d/%m/%Y'))
                        except:
                            st.metric("Fecha del archivo", "No disponible")
                
                if st.button("🚀 Guardar en BigQuery", type="primary", use_container_width=True):
                    guardados, errores, detalle = guardar_gestiones_jamar(df, PROYECTO_ID)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Total", f"{len(df):,}")
                    with col2:
                        st.metric("✅ Guardados", f"{guardados:,}")
                    with col3:
                        st.metric("❌ Errores", f"{errores:,}")
                    
                    if guardados > 0:
                        st.success(f"🎉 {detalle}")
                        st.balloons()
                    elif errores > 0:
                        st.warning(f"⚠️ {detalle}")
                    else:
                        st.warning("⚠️ No se guardaron registros. Verifica el archivo.")
                    
                    st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)
    
    st.markdown('</div>', unsafe_allow_html=True)

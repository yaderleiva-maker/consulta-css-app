import streamlit as st
import pandas as pd
import uuid
import re
import unicodedata
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACION
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"
TABLA_PAGOS = f"{PROYECTO_BQ}.pagos_jamar"

# Columnas requeridas
COLUMNAS_REQUERIDAS = [
    "Estado inicial",
    "Tramo inicial",
    "Tramo Nuevo",
    "Número de Cuenta",
    "Codigo de la Agencia",
    "Número cuota vencidas",
    "Saldo",
    "Saldo vencido",
    "Intereses",
    "Gasto Cobranza",
    "Saldo Honorarios",
    "Recaudo Periodo",
    "FECHA UP",
    "Codigo del Cliente",
    "Nombre del Cliente",
    "Cobrador",
    "Nombre del cobrador",
]

# ============================================================
# FUNCIONES DE NORMALIZACION
# ============================================================

def normalizar_encabezado(nombre):
    nombre = unicodedata.normalize("NFKD", str(nombre))
    nombre = nombre.encode("ascii", "ignore").decode("ascii")
    nombre = nombre.strip().lower()
    nombre = re.sub(r"\s+", "_", nombre)
    return nombre

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

def normalizar_fecha(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.date()
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, str):
        for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']:
            try:
                return datetime.strptime(valor.strip(), fmt).date()
            except:
                continue
    return None

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

def obtener_ultima_carga_pagos():
    try:
        query = f"""
            SELECT 
                MAX(fecha_carga) AS ultima_carga,
                COUNT(*) AS total_registros,
                MAX(fecha_up) AS ultima_fecha_corte
            FROM `{TABLA_PAGOS}`
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        if df.empty:
            return None
        if df['ultima_carga'].iloc[0] is None:
            return None
        return {
            'fecha_carga': df['ultima_carga'].iloc[0],
            'total': df['total_registros'].iloc[0],
            'fecha_corte': df['ultima_fecha_corte'].iloc[0]
        }
    except:
        return None

def guardar_pagos_jamar(df, proyecto_id):
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    registros_guardados = 0
    detalles = []
    
    id_carga = str(uuid.uuid4())
    ahora = datetime.now()
    
    registros = []
    
    for idx, row in df.iterrows():
        try:
            estado_inicial = normalizar_texto(row.get('Estado inicial'))
            tramo_inicial = normalizar_texto(row.get('Tramo inicial'))
            tramo_nuevo = normalizar_texto(row.get('Tramo Nuevo'))
            numero_cuenta = normalizar_texto(row.get('Número de Cuenta'))
            codigo_agencia = normalizar_texto(row.get('Codigo de la Agencia'))
            numero_cuotas_vencidas = normalizar_numero(row.get('Número cuota vencidas'))
            saldo = normalizar_numero(row.get('Saldo'))
            saldo_vencido = normalizar_numero(row.get('Saldo vencido'))
            intereses = normalizar_numero(row.get('Intereses'))
            gasto_cobranza = normalizar_numero(row.get('Gasto Cobranza'))
            saldo_honorarios = normalizar_numero(row.get('Saldo Honorarios'))
            recaudo_periodo = normalizar_numero(row.get('Recaudo Periodo'))
            codigo_cliente = normalizar_texto(row.get('Codigo del Cliente'))
            nombre_cliente = normalizar_texto(row.get('Nombre del Cliente'))
            cobrador = normalizar_texto(row.get('Cobrador'))
            nombre_cobrador = normalizar_texto(row.get('Nombre del cobrador'))
            
            fecha_up = normalizar_fecha(row.get('FECHA UP'))
            if fecha_up is None:
                errores += 1
                detalles.append(f"Fila {idx+2}: Fecha invalida")
                continue
            
            llave = None
            if codigo_agencia and numero_cuenta:
                llave = f"{codigo_agencia}{numero_cuenta}"
            
            id_pago = str(uuid.uuid4())
            
            registro = {
                'id_pago': id_pago,
                'id_carga': id_carga,
                'id_proyecto': PROYECTO_ID,
                'llave': llave,
                'estado_inicial': estado_inicial,
                'tramo_inicial': tramo_inicial,
                'tramo_nuevo': tramo_nuevo,
                'numero_cuenta': numero_cuenta,
                'codigo_agencia': codigo_agencia,
                'numero_cuotas_vencidas': numero_cuotas_vencidas,
                'saldo': saldo,
                'saldo_vencido': saldo_vencido,
                'intereses': intereses,
                'gasto_cobranza': gasto_cobranza,
                'saldo_honorarios': saldo_honorarios,
                'recaudo_periodo': recaudo_periodo,
                'fecha_up': fecha_up,
                'codigo_cliente': codigo_cliente,
                'nombre_cliente': nombre_cliente,
                'cobrador': cobrador,
                'nombre_cobrador': nombre_cobrador,
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
    
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        try:
            tabla_destino = client.get_table(TABLA_PAGOS)
        except Exception as e:
            st.error(f"La tabla no existe: {e}")
            return 0, total, f"Tabla no existe: {e}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=tabla_destino.schema,
        )
        
        with st.spinner(f"Subiendo {len(df_insert)} registros a BigQuery..."):
            job = client.load_table_from_dataframe(
                df_insert,
                TABLA_PAGOS,
                job_config=job_config,
            )
            job.result(timeout=180)
            
            rows_loaded = job.output_rows or len(df_insert)
            st.success(f"✅ BigQuery terminó el job. Filas cargadas: {rows_loaded}")
            registros_guardados = rows_loaded
            
    except Exception as e:
        st.error(f"Error al insertar en BigQuery: {str(e)}")
        return 0, total, f"Error en BigQuery: {e}"
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

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
    
    st.markdown('<div class="main-header">Carga de Pagos - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el archivo de pagos diario. El sistema reemplazará completamente los datos anteriores (foto diaria).</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">Estado de pagos</div>', unsafe_allow_html=True)
    
    ultima_carga = obtener_ultima_carga_pagos()
    
    if ultima_carga:
        fecha_carga = ultima_carga['fecha_carga']
        fecha_corte = ultima_carga['fecha_corte']
        total = ultima_carga['total']
        
        # 🔥 Manejar NaT correctamente
        if pd.notna(fecha_carga):
            fecha_carga_str = pd.Timestamp(fecha_carga).strftime("%d/%m/%Y %H:%M")
        else:
            fecha_carga_str = "Sin cargas registradas"
        
        if pd.notna(fecha_corte):
            fecha_corte_str = pd.Timestamp(fecha_corte).strftime("%d/%m/%Y")
        else:
            fecha_corte_str = "Sin fecha de corte"
        
        st.success(f"📊 **Pagos cargados** · {total:,} registros · Corte: {fecha_corte_str} · Última carga: {fecha_carga_str}")
    else:
        st.warning("⚠️ **No hay pagos cargados.** Sube un archivo para comenzar.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="card">
        <div class="card-title">Instrucciones</div>
        <ul style="margin: 0; padding-left: 20px; color: #4b5563; font-size: 14px; line-height: 1.8;">
            <li>El archivo debe tener las columnas del formato de pagos de Jamar.</li>
            <li><strong>IMPORTANTE:</strong> Esta carga reemplazará TODOS los datos anteriores de pagos (foto diaria).</li>
            <li>La fecha oficial de corte es la columna <strong>FECHA UP</strong>.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">Subir archivo de pagos</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo Excel",
        type=["xlsx", "xls"],
        key="pagos_uploader",
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
        
        with st.spinner("Procesando archivo..."):
            try:
                df = pd.read_excel(uploaded_file)
                
                df.columns = (
                    df.columns.astype(str)
                    .str.replace("\ufffd", "ú", regex=False)
                    .str.strip()
                )
                
                faltantes = [col for col in COLUMNAS_REQUERIDAS if col not in df.columns]
                if faltantes:
                    st.error(f"Faltan columnas obligatorias: {', '.join(faltantes)}")
                    st.stop()
                
                st.markdown("---")
                st.markdown("#### Vista previa del archivo")
                st.dataframe(df.head(5), use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    st.metric("Columnas", f"{len(df.columns)}")
                with col3:
                    if 'FECHA UP' in df.columns:
                        try:
                            fecha = pd.to_datetime(df['FECHA UP'].iloc[0])
                            st.metric("Fecha de corte", fecha.strftime('%d/%m/%Y'))
                        except:
                            st.metric("Fecha de corte", "No disponible")
                
                if st.button("Guardar en BigQuery", type="primary", use_container_width=True):
                    guardados, errores, detalle = guardar_pagos_jamar(df, PROYECTO_ID)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total", f"{len(df):,}")
                    with col2:
                        st.metric("Guardados", f"{guardados:,}")
                    with col3:
                        st.metric("Errores", f"{errores:,}")
                    
                    if guardados > 0:
                        st.success(f"🎉 {detalle}")
                        st.balloons()
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"❌ {detalle}")
                        st.stop()
                
            except Exception as e:
                st.error(f"Error al procesar el archivo: {str(e)}")
                st.exception(e)
    
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    render()

import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"
TABLA_DESTINO = f"{PROYECTO_BQ}.cartera_predemanda_jamar"

COLUMNAS_REQUERIDAS = [
    'Estado inicial', 'Tramo inicial', 'Codigo de la Agencia', 
    'Número de Cuenta', 'Tipo credito', 'Saldo Total adeudado',
    'Codigo del Cliente', 'Nombre del Cliente', 'Rank'
]

# ============================================================
# FUNCIONES
# ============================================================

def obtener_ultima_carga_cartera():
    try:
        query = f"""
            SELECT 
                MAX(fecha_carga) AS ultima_carga,
                COUNT(*) AS total_registros
            FROM `{TABLA_DESTINO}`
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        if df.empty:
            return None
        if df['ultima_carga'].iloc[0] is None:
            return None
        if pd.isna(df['ultima_carga'].iloc[0]):
            return None
        return {
            'fecha': df['ultima_carga'].iloc[0],
            'total': df['total_registros'].iloc[0]
        }
    except Exception as e:
        st.warning(f"⚠️ No se pudo obtener información de la cartera: {e}")
        return None

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

def generar_llave(agencia, cuenta):
    if pd.isna(agencia) or pd.isna(cuenta):
        return None
    return f"{str(agencia).strip()}{str(cuenta).strip()}"

def guardar_cartera_jamar(df, proyecto_id):
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    registros_guardados = 0
    detalles = []
    
    id_carga = str(uuid.uuid4())
    
    registros = []
    
    for idx, row in df.iterrows():
        try:
            codigo_agencia = normalizar_texto(row.get('Codigo de la Agencia'))
            numero_cuenta = normalizar_texto(row.get('Número de Cuenta'))
            
            if not codigo_agencia or not numero_cuenta:
                errores += 1
                detalles.append(f"Fila {idx+2}: Falta código de agencia o número de cuenta")
                continue
            
            llave = generar_llave(codigo_agencia, numero_cuenta)
            if not llave:
                errores += 1
                detalles.append(f"Fila {idx+2}: No se pudo generar llave")
                continue
            
            estado_inicial = normalizar_texto(row.get('Estado inicial'))
            tramo_inicial = normalizar_texto(row.get('Tramo inicial'))
            tipo_credito = normalizar_texto(row.get('Tipo credito'))
            codigo_cliente = normalizar_texto(row.get('Codigo del Cliente'))
            nombre_cliente = normalizar_texto(row.get('Nombre del Cliente'))
            rank = normalizar_texto(row.get('Rank'))
            entidad = normalizar_texto(row.get('ENTIDAD', 'HEXAGON'))
            
            saldo_total = normalizar_numero(row.get('Saldo Total adeudado'))
            saldo_vencido = normalizar_numero(row.get('Saldo Total vencido'))
            vr_pagar_dcto_1 = normalizar_numero(row.get('VR A PAGAR DCTO 1'))
            vr_pagar_dcto_2 = normalizar_numero(row.get('VR A PAGAR DCTO 2'))
            vr_pagar_plan_al_dia = normalizar_numero(row.get('Vr a pagar PLAN AL DIA'))
            cuota_inicial = normalizar_numero(row.get('CUOTA INICIAL ARREGLO'))
            saldo_diferir = normalizar_numero(row.get('Saldo a diferir por cuotas'))
            
            fecha_ultimo_pago = None
            if pd.notna(row.get('Fecha ultimo pago')):
                try:
                    fecha_ultimo_pago = pd.to_datetime(row.get('Fecha ultimo pago')).date().isoformat()
                except:
                    pass
            
            plazo_dcto_1 = normalizar_texto(row.get('PLAZO DCTO 1'))
            plazo_dcto_2 = normalizar_texto(row.get('PLAZO DCTO 2'))
            
            id_registro = str(uuid.uuid4())
            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            registro = {
                'id_registro': id_registro,
                'id_carga': id_carga,
                'id_proyecto': PROYECTO_ID,
                'llave': llave,
                'estado_inicial': estado_inicial,
                'tramo_inicial': tramo_inicial,
                'codigo_agencia': codigo_agencia,
                'numero_cuenta': numero_cuenta,
                'tipo_credito': tipo_credito,
                'saldo_total_vencido': saldo_vencido,
                'saldo_total_adeudado': saldo_total,
                'fecha_ultimo_pago': fecha_ultimo_pago,
                'codigo_cliente': codigo_cliente,
                'nombre_cliente': nombre_cliente,
                'entidad': entidad,
                'rank': rank,
                'vr_pagar_dcto_1': vr_pagar_dcto_1,
                'vr_pagar_dcto_2': vr_pagar_dcto_2,
                'plazo_dcto_1': plazo_dcto_1,
                'plazo_dcto_2': plazo_dcto_2,
                'vr_pagar_plan_al_dia': vr_pagar_plan_al_dia,
                'cuota_inicial_arreglo': cuota_inicial,
                'saldo_diferir_cuotas': saldo_diferir,
                'fecha_carga': ahora,
                'created_at': ahora,
                'updated_at': ahora
            }
            registros.append(registro)
            
        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")
    
    if not registros:
        st.warning("No hay datos válidos para insertar")
        return 0, total, "No hay datos válidos"
    
    df_insert = pd.DataFrame(registros)
    
    # TIMESTAMP: convertir a fechas UTC reales
    for columna in ["fecha_carga", "created_at", "updated_at"]:
        if columna in df_insert.columns:
            df_insert[columna] = pd.to_datetime(
                df_insert[columna],
                errors="coerce",
                utc=True,
            )
    
    # DATE: convertir a date real
    if "fecha_ultimo_pago" in df_insert.columns:
        df_insert["fecha_ultimo_pago"] = pd.to_datetime(
            df_insert["fecha_ultimo_pago"],
            errors="coerce",
        ).dt.date
    
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        try:
            client.get_table(TABLA_DESTINO)
        except Exception as e:
            st.error(f"La tabla no existe: {e}")
            return 0, total, f"Tabla no existe: {e}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        with st.spinner(f"Subiendo {len(df_insert)} registros a BigQuery..."):
            job = client.load_table_from_dataframe(
                df_insert,
                TABLA_DESTINO,
                job_config=job_config
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
        .selected-file { background-color: #f0fdf4; border: 1px solid #86efac; border-radius: 8

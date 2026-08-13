import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime

from services.bigquery import ejecutar_query
from services.archivos import leer_excel, validar_columnas

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"
TABLA_DESTINO = f"`{PROYECTO_BQ}.cartera_predemanda_jamar`"

# Columnas requeridas para la carga de Jamar
COLUMNAS_REQUERIDAS = [
    'Estado inicial', 'Tramo inicial', 'Codigo de la Agencia', 
    'Número de Cuenta', 'Tipo credito', 'Saldo Total adeudado',
    'Codigo del Cliente', 'Nombre del Cliente', 'Rank'
]

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

def obtener_ultima_carga_cartera():
    """Obtiene la fecha de la última carga de cartera"""
    try:
        query = f"""
            SELECT 
                MAX(fecha_carga) AS ultima_carga,
                COUNT(*) AS total_registros
            FROM {TABLA_DESTINO}
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        
        if df.empty:
            return None
            
        # Verificar si hay datos válidos
        if df['ultima_carga'].iloc[0] is None:
            return None
            
        # Verificar si es NaT (Not a Time)
        if pd.isna(df['ultima_carga'].iloc[0]):
            return None
            
        return {
            'fecha': df['ultima_carga'].iloc[0],
            'total': df['total_registros'].iloc[0]
        }
    except Exception as e:
        st.warning(f"⚠️ No se pudo obtener información de la cartera: {e}")
        return None

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_texto(valor):
    if pd.isna(valor):
        return None
    return str(valor).strip()

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
    detalles = []
    registros_guardados = 0

    # Eliminar datos anteriores
    with st.spinner("Eliminando datos anteriores de Jamar..."):
        delete_query = f"""
            DELETE FROM {TABLA_DESTINO}
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        try:
            ejecutar_query(delete_query)
        except Exception as e:
            return 0, total, f"Error en eliminación: {e}"

    # Preparar nuevos registros
    valores = []
    
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
            
            valores.append(f"""(
                '{id_registro}',
                '{PROYECTO_ID}',
                '{llave}',
                {f"'{estado_inicial}'" if estado_inicial else 'NULL'},
                {f"'{tramo_inicial}'" if tramo_inicial else 'NULL'},
                '{codigo_agencia}',
                '{numero_cuenta}',
                {f"'{tipo_credito}'" if tipo_credito else 'NULL'},
                {saldo_vencido if saldo_vencido is not None else 'NULL'},
                {saldo_total if saldo_total is not None else 'NULL'},
                {f"'{fecha_ultimo_pago}'" if fecha_ultimo_pago else 'NULL'},
                {f"'{codigo_cliente}'" if codigo_cliente else 'NULL'},
                {f"'{nombre_cliente}'" if nombre_cliente else 'NULL'},
                {f"'{entidad}'" if entidad else 'NULL'},
                {f"'{rank}'" if rank else 'NULL'},
                {vr_pagar_dcto_1 if vr_pagar_dcto_1 is not None else 'NULL'},
                {vr_pagar_dcto_2 if vr_pagar_dcto_2 is not None else 'NULL'},
                {f"'{plazo_dcto_1}'" if plazo_dcto_1 else 'NULL'},
                {f"'{plazo_dcto_2}'" if plazo_dcto_2 else 'NULL'},
                {vr_pagar_plan_al_dia if vr_pagar_plan_al_dia is not None else 'NULL'},
                {cuota_inicial if cuota_inicial is not None else 'NULL'},
                {saldo_diferir if saldo_diferir is not None else 'NULL'},
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )""")
            
        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")
    
    if valores:
        with st.spinner(f"Insertando {len(valores)} registros en BigQuery..."):
            insert_query = f"""
                INSERT INTO {TABLA_DESTINO}
                (id_registro, id_proyecto, llave, estado_inicial, tramo_inicial,
                 codigo_agencia, numero_cuenta, tipo_credito, saldo_total_vencido,
                 saldo_total_adeudado, fecha_ultimo_pago, codigo_cliente, nombre_cliente,
                 entidad, rank, vr_pagar_dcto_1, vr_pagar_dcto_2, plazo_dcto_1,
                 plazo_dcto_2, vr_pagar_plan_al_dia, cuota_inicial_arreglo,
                 saldo_diferir_cuotas, fecha_carga, created_at, updated_at)
                VALUES {', '.join(valores)}
            """
            try:
                ejecutar_query(insert_query)
                registros_guardados = len(valores)
            except Exception as e:
                return registros_guardados, errores, f"Error en inserción: {e}"
    
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
    
    st.markdown('<div class="main-header">Carga de Cartera Predemanda - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el archivo de cartera pre-demanda de Jamar. El sistema reemplazará completamente los datos anteriores.</div>', unsafe_allow_html=True)
    
    # ---- Verificar si ya existe cartera cargada ----
    ultima_carga = obtener_ultima_carga_cartera()
    
    if ultima_carga:
        fecha = ultima_carga['fecha']
        total = ultima_carga['total']
        
        # Verificar si la fecha es válida
        if pd.isna(fecha):
            st.success(f"📊 **Cartera ya cargada** · {total:,} registros")
        else:
            fecha_str = fecha.strftime('%d/%m/%Y %H:%M')
            st.success(f"📊 **Cartera ya cargada** · {total:,} registros · Última carga: {fecha_str}")
    else:
        st.warning("⚠️ **No hay cartera cargada.** Sube un archivo para comenzar.")
    
    # ---- Instrucciones ----
    st.markdown("""
    <div class="card">
        <div class="card-title">Instrucciones</div>
        <ul style="margin: 0; padding-left: 20px; color: #4b5563; font-size: 14px; line-height: 1.8;">
            <li>El archivo debe tener las columnas del formato de Jamar.</li>
            <li>Columnas obligatorias: <strong>Estado inicial, Tramo inicial, Codigo de la Agencia, Número de Cuenta, Saldo Total adeudado, Codigo del Cliente, Nombre del Cliente, Rank</strong></li>
            <li>Se generará automáticamente una <strong>clave compuesta</strong> (Agencia + Cuenta) para identificar cada registro.</li>
            <li><strong>IMPORTANTE:</strong> Esta carga reemplazará TODOS los datos anteriores de Jamar.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # ---- Subida de archivo ----
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">Subir archivo</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo Excel",
        type=["xlsx", "xls"],
        key="jamar_uploader",
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
                
                faltantes = [col for col in COLUMNAS_REQUERIDAS if col not in df.columns]
                if faltantes:
                    st.error(f"Faltan columnas obligatorias: {', '.join(faltantes)}")
                    st.stop()
                
                st.markdown("---")
                st.markdown("#### Vista previa del archivo")
                st.dataframe(df.head(10), use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    st.metric("Columnas", f"{len(df.columns)}")
                with col3:
                    llaves = df.apply(lambda row: generar_llave(row.get('Codigo de la Agencia'), row.get('Número de Cuenta')), axis=1)
                    st.metric("Llaves únicas", f"{llaves.nunique():,}")
                
                if st.button("Guardar en BigQuery", type="primary", use_container_width=True):
                    guardados, errores, detalle = guardar_cartera_jamar(df, PROYECTO_ID)
                    
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
                        st.rerun()
                    else:
                        st.error(f"❌ {detalle}")
                
            except Exception as e:
                st.error(f"Error al procesar el archivo: {str(e)}")
                st.exception(e)
    
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    render()

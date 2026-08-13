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
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_texto(valor):
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    # Escapar comillas simples para SQL
    texto = texto.replace("'", "''")
    return texto

def normalizar_numero(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        limpio = re.sub(r'[^\d.,-]', '', valor)
        limpio = limpio.replace(',', '.')
        try:
            return float(limpio)
        except:
            return None
    return None

def normalizar_fecha_hora(valor):
    """Convierte a TIMESTAMP. Formato esperado: DD-MM-YYYY HH:MM:SS"""
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()
    if isinstance(valor, str):
        for fmt in ['%d-%m-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']:
            try:
                return datetime.strptime(valor.strip(), fmt).isoformat()
            except:
                continue
    return None

def normalizar_fecha(valor):
    """Convierte a DATE"""
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.date().isoformat()
    if isinstance(valor, str):
        for fmt in ['%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y']:
            try:
                return datetime.strptime(valor.strip(), fmt).date().isoformat()
            except:
                continue
    return None

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_mapeo_codigos():
    """Obtiene el mapeo de códigos de gestión desde BigQuery"""
    query = f"""
        SELECT codigo_gestion, mejor_gestion_jamar, resultado
        FROM {TABLA_MAPEO}
    """
    df = ejecutar_query(query)
    if df.empty:
        return {}
    return dict(zip(df['codigo_gestion'], zip(df['mejor_gestion_jamar'], df['resultado'])))

def obtener_ultimas_fechas_carga():
    """Obtiene las fechas de las últimas cargas de gestiones para mostrar resumen"""
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
    try:
        df = ejecutar_query(query)
        return df
    except:
        return pd.DataFrame()

def guardar_gestiones_jamar(df, proyecto_id):
    """
    Guarda las gestiones de Jamar en BigQuery.
    """
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    detalles = []
    registros_guardados = 0
    
    # Obtener mapeo de códigos
    mapeo = obtener_mapeo_codigos()
    
    valores = []
    
    for idx, row in df.iterrows():
        try:
            # Extraer campos clave
            llave = normalizar_texto(row.get('Llave'))
            codigo_agencia = normalizar_texto(row.get('Codigo de la Agencia'))
            numero_cuenta = normalizar_texto(row.get('Número de Cuenta'))
            codigo_cliente = normalizar_texto(row.get('Codigo del Cliente'))
            codigo_gestion = normalizar_texto(row.get('codigo_gestion'))
            
            if not llave:
                if codigo_agencia and numero_cuenta:
                    llave = f"{codigo_agencia}{numero_cuenta}"
            
            # Obtener clasificación del código de gestión
            mejor_gestion = None
            resultado = None
            if codigo_gestion and codigo_gestion in mapeo:
                mejor_gestion, resultado = mapeo[codigo_gestion]
            
            # Fechas
            fechahora = normalizar_fecha_hora(row.get('fechahoragestion'))
            fechapromesa = normalizar_fecha(row.get('fechapromesa'))
            fecha = normalizar_fecha(row.get('Fecha'))
            
            # Números
            valorpromesa = normalizar_numero(row.get('valorpromesa'))
            min_prioridad = None
            try:
                if pd.notna(row.get('MínDePrioridad')):
                    min_prioridad = int(row.get('MínDePrioridad'))
            except:
                pass
            
            id_gestion = str(uuid.uuid4())
            
            # Escapar todos los textos
            observacion = normalizar_texto(row.get('Observación'))
            cod_cobrador = normalizar_texto(row.get('Codigo del cobrador'))
            area_gestion = normalizar_texto(row.get('area_gestion'))
            tipo_gestion = normalizar_texto(row.get('tipo_gestion'))
            numeromarcado = normalizar_texto(row.get('numeromarcado'))
            tipo_telefono = normalizar_texto(row.get('tipo_telefono'))
            lugar_contacto = normalizar_texto(row.get('lugar_contacto'))
            tipo_contacto = normalizar_texto(row.get('tipo_contacto'))
            clave = normalizar_texto(row.get('Clave'))
            clave_min = normalizar_texto(row.get('ClaveMin'))
            
            valores.append(f"""(
                '{id_gestion}',
                '{PROYECTO_ID}',
                {f"'{llave}'" if llave else 'NULL'},
                {f"'{codigo_agencia}'" if codigo_agencia else 'NULL'},
                {f"'{numero_cuenta}'" if numero_cuenta else 'NULL'},
                {f"'{codigo_cliente}'" if codigo_cliente else 'NULL'},
                {f"'{fechahora}'" if fechahora else 'NULL'},
                {f"'{codigo_gestion}'" if codigo_gestion else 'NULL'},
                {f"'{observacion}'" if observacion else 'NULL'},
                {f"'{cod_cobrador}'" if cod_cobrador else 'NULL'},
                {f"'{area_gestion}'" if area_gestion else 'NULL'},
                {f"'{tipo_gestion}'" if tipo_gestion else 'NULL'},
                {f"'{numeromarcado}'" if numeromarcado else 'NULL'},
                {f"'{tipo_telefono}'" if tipo_telefono else 'NULL'},
                {f"'{fechapromesa}'" if fechapromesa else 'NULL'},
                {valorpromesa if valorpromesa is not None else 'NULL'},
                {f"'{mejor_gestion}'" if mejor_gestion else 'NULL'},
                {f"'{resultado}'" if resultado else 'NULL'},
                {f"'{lugar_contacto}'" if lugar_contacto else 'NULL'},
                {f"'{tipo_contacto}'" if tipo_contacto else 'NULL'},
                {f"'{clave}'" if clave else 'NULL'},
                {f"'{fecha}'" if fecha else 'NULL'},
                {min_prioridad if min_prioridad is not None else 'NULL'},
                {f"'{clave_min}'" if clave_min else 'NULL'},
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )""")
            
        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")
    
    if valores:
        with st.spinner(f"📥 Insertando {len(valores)} registros en BigQuery..."):
            insert_query = f"""
                INSERT INTO {TABLA_GESTIONES}
                (id_gestion, id_proyecto, llave, codigo_agencia, numero_cuenta, codigo_cliente,
                 fechahoragestion, codigo_gestion, observacion, codigo_cobrador, area_gestion,
                 tipo_gestion, numeromarcado, tipo_telefono, fechapromesa, valorpromesa,
                 mejor_gestion_jamar, resultado_gestion, lugar_contacto, tipo_contacto,
                 clave, fecha, min_de_prioridad, clave_min, fecha_carga, created_at, updated_at)
                VALUES {', '.join(valores)}
            """
            try:
                ejecutar_query(insert_query)
                registros_guardados = len(valores)
            except Exception as e:
                st.error(f"❌ Error al insertar datos: {e}")
                return registros_guardados, errores, f"Error en inserción: {e}"
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

# ============================================================
# FUNCIÓN PARA LEER AMBAS HOJAS DEL EXCEL
# ============================================================

def leer_archivo_gestiones(uploaded_file):
    """Lee un archivo Excel con dos hojas: CORREOS & WHATSAPP y LLAMADAS."""
    try:
        df_correos = pd.read_excel(uploaded_file, sheet_name='CORREOS & WHATSAPP')
        df_llamadas = pd.read_excel(uploaded_file, sheet_name='LLAMADAS')
    except Exception as e:
        st.error(f"❌ Error al leer el archivo. Asegúrate de que tiene las hojas 'CORREOS & WHATSAPP' y 'LLAMADAS'. Error: {e}")
        raise
    
    df_combinado = pd.concat([df_correos, df_llamadas], ignore_index=True)
    
    # Normalizar nombres de columnas
    df_combinado.columns = df_combinado.columns.str.strip()
    
    return df_combinado

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
        .resumen-card { background-color: #f8fafc; border-radius: 8px; padding: 12px 16px; border: 1px solid #e5e7eb; }
        .resumen-fecha { font-weight: 600; color: #1a1a1a; }
        .resumen-numero { font-weight: 500; color: #dc2626; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">📞 Carga de Gestiones - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el reporte de gestiones diarias. El sistema procesará ambas hojas (CORREOS & WHATSAPP y LLAMADAS).</div>', unsafe_allow_html=True)
    
    # ============================================================
    # RESUMEN DE ÚLTIMAS CARGAS
    # ============================================================
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">📋 Últimas cargas de gestiones</div>', unsafe_allow_html=True)
    
    df_resumen = obtener_ultimas_fechas_carga()
    
    if not df_resumen.empty:
        # Mostrar tabla compacta
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
    
    # ============================================================
    # ÁREA DE CARGA
    # ============================================================
    
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
                    codigos_unicos = df['codigo_gestion'].nunique() if 'codigo_gestion' in df.columns else 0
                    st.metric("Códigos únicos", f"{codigos_unicos:,}")
                with col3:
                    # Fecha de la carga (tomar la primera fecha del archivo)
                    if 'fechahoragestion' in df.columns:
                        try:
                            primera_fecha = pd.to_datetime(df['fechahoragestion'].iloc[0])
                            st.metric("Fecha del archivo", primera_fecha.strftime('%d/%m/%Y'))
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
                    
                    if errores == 0:
                        st.success(f"🎉 {detalle}")
                    else:
                        st.warning(f"⚠️ {detalle}")
                    
                    st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
    
    st.markdown('</div>', unsafe_allow_html=True)

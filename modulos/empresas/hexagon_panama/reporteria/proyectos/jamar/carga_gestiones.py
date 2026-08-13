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
    if texto == '' or texto == 'nan' or texto == 'None' or texto == 'NULL':
        return None
    # Escapar comillas simples (') para SQL
    texto = texto.replace("'", "''")
    # Escapar caracteres especiales
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
    """Convierte a TIMESTAMP"""
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()
    if isinstance(valor, str):
        valor = str(valor).strip()
        # Si está vacío
        if not valor or valor == 'nan' or valor == 'None':
            return None
        for fmt in ['%d-%m-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']:
            try:
                return datetime.strptime(valor, fmt).isoformat()
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
    """Obtiene el mapeo de códigos de gestión desde BigQuery"""
    try:
        query = f"""
            SELECT codigo_gestion, mejor_gestion_jamar, resultado
            FROM {TABLA_MAPEO}
        """
        df = ejecutar_query(query)
        if df.empty:
            st.warning("⚠️ No se encontró la tabla de mapeo. Ejecuta primero el SQL de creación.")
            return {}
        return dict(zip(df['codigo_gestion'], zip(df['mejor_gestion_jamar'], df['resultado'])))
    except Exception as e:
        st.error(f"❌ Error al obtener mapeo: {e}")
        return {}

def obtener_cartera_llaves():
    """Obtiene las llaves existentes en la cartera para validación"""
    try:
        query = f"""
            SELECT llave
            FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
            WHERE id_proyecto = '{PROYECTO_ID}'
        """
        df = ejecutar_query(query)
        return set(df['llave'].tolist()) if not df.empty else set()
    except:
        return set()

def verificar_cartera_cargada():
    """Verifica si la cartera predemanda tiene datos"""
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
    """Obtiene las fechas de las últimas cargas de gestiones"""
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
    """
    Guarda las gestiones de Jamar en BigQuery.
    """
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    detalles = []
    registros_guardados = 0
    
    # 1. Obtener mapeo de códigos
    mapeo = obtener_mapeo_codigos()
    if not mapeo:
        st.error("❌ No se pudo obtener el mapeo de códigos. Verifica la tabla mapeo_codigos_gestion.")
        return 0, total, "Error: Tabla de mapeo vacía o no existe"
    
    # 2. Validar que hay datos para procesar
    if df.empty:
        st.warning("⚠️ El archivo no contiene datos.")
        return 0, total, "Archivo vacío"
    
    st.info(f"📊 Procesando {len(df)} registros...")
    
    # 3. Preparar valores
    valores = []
    registros_sin_llave = 0
    
    for idx, row in df.iterrows():
        try:
            # Extraer campos clave
            llave_raw = row.get('Llave')
            codigo_agencia = normalizar_texto(row.get('Codigo de la Agencia'))
            numero_cuenta = normalizar_texto(row.get('Número de Cuenta'))
            
            # Si no tiene llave, intentar generarla
            if pd.isna(llave_raw) or not str(llave_raw).strip():
                if codigo_agencia and numero_cuenta:
                    llave = f"{codigo_agencia}{numero_cuenta}"
                    registros_sin_llave += 1
                else:
                    llave = None
            else:
                llave = normalizar_texto(llave_raw)
            
            codigo_cliente = normalizar_texto(row.get('Codigo del Cliente'))
            codigo_gestion = normalizar_texto(row.get('codigo_gestion'))
            
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
                val = row.get('MínDePrioridad')
                if pd.notna(val):
                    min_prioridad = int(float(val))
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
            pais = normalizar_texto(row.get('Pais'))
            tipo_credito = normalizar_texto(row.get('Tipo credito'))
            
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
    
    # Mostrar estadísticas de procesamiento
    st.info(f"📊 Registros procesados: {len(valores)} para insertar, {errores} con errores")
    if registros_sin_llave > 0:
        st.info(f"🔑 {registros_sin_llave} registros no tenían llave y se generaron automáticamente")
    
    # 4. Insertar en BigQuery
    if valores:
        # Dividir en lotes para evitar errores de tamaño
        batch_size = 500
        total_insertados = 0
        
        for i in range(0, len(valores), batch_size):
            batch = valores[i:i+batch_size]
            try:
                insert_query = f"""
                    INSERT INTO {TABLA_GESTIONES}
                    (id_gestion, id_proyecto, llave, codigo_agencia, numero_cuenta, codigo_cliente,
                     fechahoragestion, codigo_gestion, observacion, codigo_cobrador, area_gestion,
                     tipo_gestion, numeromarcado, tipo_telefono, fechapromesa, valorpromesa,
                     mejor_gestion_jamar, resultado_gestion, lugar_contacto, tipo_contacto,
                     clave, fecha, min_de_prioridad, clave_min, fecha_carga, created_at, updated_at)
                    VALUES {', '.join(batch)}
                """
                ejecutar_query(insert_query)
                total_insertados += len(batch)
                st.success(f"✅ Lote {i//batch_size + 1}: {len(batch)} registros insertados")
            except Exception as e:
                st.error(f"❌ Error en lote {i//batch_size + 1}: {e}")
                errores += len(batch)
        
        registros_guardados = total_insertados
    else:
        st.warning("⚠️ No hay datos válidos para insertar")
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

# ============================================================
# FUNCIÓN PARA LEER AMBAS HOJAS DEL EXCEL
# ============================================================

def leer_archivo_gestiones(uploaded_file):
    """Lee un archivo Excel con dos hojas: CORREOS & WHATSAPP y LLAMADAS."""
    try:
        # Intentar leer ambas hojas
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names
        
        st.info(f"📋 Hojas encontradas: {', '.join(sheet_names)}")
        
        if 'CORREOS & WHATSAPP' in sheet_names and 'LLAMADAS' in sheet_names:
            df_correos = pd.read_excel(uploaded_file, sheet_name='CORREOS & WHATSAPP')
            df_llamadas = pd.read_excel(uploaded_file, sheet_name='LLAMADAS')
            df_combinado = pd.concat([df_correos, df_llamadas], ignore_index=True)
            st.info(f"📊 CORREOS: {len(df_correos)} registros, LLAMADAS: {len(df_llamadas)} registros")
        else:
            # Si no tiene las hojas exactas, intentar con la primera hoja
            st.warning(f"⚠️ No se encontraron las hojas 'CORREOS & WHATSAPP' y 'LLAMADAS'. Usando la primera hoja: {sheet_names[0]}")
            df_combinado = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
        
        # Normalizar nombres de columnas
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
        .status-badge.error { background-color: #fee2e2; color: #991b1b; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">📞 Carga de Gestiones - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el reporte de gestiones diarias. El sistema procesará ambas hojas (CORREOS & WHATSAPP y LLAMADAS).</div>', unsafe_allow_html=True)
    
    # ============================================================
    # VERIFICAR CARTERA CARGADA
    # ============================================================
    
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
    
    # Mostrar últimas cargas de gestiones
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
                    if 'fechahoragestion' in df.columns:
                        try:
                            primera_fecha = pd.to_datetime(df['fechahoragestion'].iloc[0])
                            st.metric("Fecha del archivo", primera_fecha.strftime('%d/%m/%Y'))
                        except:
                            st.metric("Fecha del archivo", "No disponible")
                
                # Mostrar columnas disponibles
                st.caption(f"Columnas encontradas: {', '.join(df.columns.tolist()[:10])}...")
                
                if st.button("🚀 Guardar en BigQuery", type="primary", use_container_width=True):
                    guardados, errores, detalle = guardar_gestiones_jamar(df, PROYECTO_ID)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Total", f"{len(df):,}")
                    with col2:
                        st.metric("✅ Guardados", f"{guardados:,}")
                    with col3:
                        st.metric("❌ Errores", f"{errores:,}")
                    
                    if errores == 0 and guardados > 0:
                        st.success(f"🎉 {detalle}")
                        st.balloons()
                    elif guardados == 0 and errores == 0:
                        st.warning("⚠️ No se guardaron registros. Verifica que el archivo tenga datos válidos.")
                    else:
                        st.warning(f"⚠️ {detalle}")
                
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)
    
    st.markdown('</div>', unsafe_allow_html=True)

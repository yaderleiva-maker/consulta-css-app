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
    return str(valor).strip()

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
        # Probar varios formatos
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

def obtener_mapeo_codigos():
    """Obtiene el mapeo de códigos de gestión desde BigQuery"""
    query = f"""
        SELECT codigo_gestion, mejor_gestion_jamar, resultado
        FROM {TABLA_MAPEO}
    """
    df = ejecutar_query(query)
    return dict(zip(df['codigo_gestion'], zip(df['mejor_gestion_jamar'], df['resultado'])))

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
    
    # ============================================================
    # PASO 1: PREPARAR REGISTROS
    # ============================================================
    
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
                # Intentar generar llave si no viene
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
            
            valores.append(f"""(
                '{id_gestion}',
                '{PROYECTO_ID}',
                {f"'{llave}'" if llave else 'NULL'},
                {f"'{codigo_agencia}'" if codigo_agencia else 'NULL'},
                {f"'{numero_cuenta}'" if numero_cuenta else 'NULL'},
                {f"'{codigo_cliente}'" if codigo_cliente else 'NULL'},
                {f"'{fechahora}'" if fechahora else 'NULL'},
                {f"'{codigo_gestion}'" if codigo_gestion else 'NULL'},
                {f"'{normalizar_texto(row.get('Observación'))}'" if pd.notna(row.get('Observación')) else 'NULL'},
                {f"'{normalizar_texto(row.get('Codigo del cobrador'))}'" if pd.notna(row.get('Codigo del cobrador')) else 'NULL'},
                {f"'{normalizar_texto(row.get('area_gestion'))}'" if pd.notna(row.get('area_gestion')) else 'NULL'},
                {f"'{normalizar_texto(row.get('tipo_gestion'))}'" if pd.notna(row.get('tipo_gestion')) else 'NULL'},
                {f"'{normalizar_texto(row.get('numeromarcado'))}'" if pd.notna(row.get('numeromarcado')) else 'NULL'},
                {f"'{normalizar_texto(row.get('tipo_telefono'))}'" if pd.notna(row.get('tipo_telefono')) else 'NULL'},
                {f"'{fechapromesa}'" if fechapromesa else 'NULL'},
                {valorpromesa if valorpromesa is not None else 'NULL'},
                {f"'{mejor_gestion}'" if mejor_gestion else 'NULL'},
                {f"'{resultado}'" if resultado else 'NULL'},
                {f"'{normalizar_texto(row.get('lugar_contacto'))}'" if pd.notna(row.get('lugar_contacto')) else 'NULL'},
                {f"'{normalizar_texto(row.get('tipo_contacto'))}'" if pd.notna(row.get('tipo_contacto')) else 'NULL'},
                {f"'{normalizar_texto(row.get('Clave'))}'" if pd.notna(row.get('Clave')) else 'NULL'},
                {f"'{fecha}'" if fecha else 'NULL'},
                {min_prioridad if min_prioridad is not None else 'NULL'},
                {f"'{normalizar_texto(row.get('ClaveMin'))}'" if pd.notna(row.get('ClaveMin')) else 'NULL'},
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )""")
            
        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")
    
    # ============================================================
    # PASO 2: INSERTAR REGISTROS
    # ============================================================
    
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
                st.success(f"✅ {registros_guardados} registros insertados correctamente")
            except Exception as e:
                st.error(f"❌ Error al insertar datos: {e}")
                return registros_guardados, errores, f"Error en inserción: {e}"
    else:
        st.warning("⚠️ No hay datos válidos para insertar")
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_guardados} registros guardados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return registros_guardados, errores, detalle

# ============================================================
# FUNCIÓN PARA LEER AMBAS HOJAS DEL EXCEL
# ============================================================

def leer_archivo_gestiones(uploaded_file):
    """
    Lee un archivo Excel con dos hojas: CORREOS & WHATSAPP y LLAMADAS.
    Las combina en un solo DataFrame.
    """
    # Leer ambas hojas
    df_correos = pd.read_excel(uploaded_file, sheet_name='CORREOS & WHATSAPP')
    df_llamadas = pd.read_excel(uploaded_file, sheet_name='LLAMADAS')
    
    # Asegurar que las columnas sean iguales (en mayúsculas y sin espacios)
    columnas = [
        'Llave', 'Pais', 'Codigo de la Agencia', 'Tipo credito', 
        'Número de Cuenta', 'Codigo del Cliente', 'fechahoragestion', 
        'codigo_gestion', 'Observación', 'Codigo del cobrador', 
        'area_gestion', 'tipo_gestion', 'numeromarcado', 'tipo_telefono', 
        'extension_', 'fechapromesa', 'valorpromesa', 'codigo_nopago', 
        'lugar_contacto', 'tipo_contacto', 'Clave', 'Fecha', 
        'MínDePrioridad', 'ClaveMin'
    ]
    
    # Normalizar columnas
    for df in [df_correos, df_llamadas]:
        df.columns = df.columns.str.strip()
    
    # Concatenar
    df_combinado = pd.concat([df_correos, df_llamadas], ignore_index=True)
    
    # Asegurar que todas las columnas existan
    for col in columnas:
        if col not in df_combinado.columns:
            df_combinado[col] = None
    
    return df_combinado

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para cargar gestiones diarias de Jamar"""
    
    st.markdown("""
    <style>
        .main-header { font-size: 24px; font-weight: 600; color: #1a1a1a; margin-bottom: 8px; }
        .sub-header { font-size: 14px; color: #6b6b6b; margin-bottom: 24px; }
        .card { background-color: #ffffff; border-radius: 12px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04); border: 1px solid #f0f0f0; margin-bottom: 16px; }
        .card-title { font-size: 16px; font-weight: 500; color: #1a1a1a; margin-bottom: 12px; }
        .selected-file { background-color: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 12px 16px; display: flex; align-items: center; gap: 12px; }
        .selected-file .file-name { font-weight: 500; color: #166534; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">📥 Carga de Gestiones - Jamar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el reporte de gestiones diarias de Jamar. El sistema procesará ambas hojas (CORREOS & WHATSAPP y LLAMADAS) y las guardará en BigQuery.</div>', unsafe_allow_html=True)
    
    # ---- Instrucciones ----
    st.markdown("""
    <div class="card">
        <div class="card-title">📋 Instrucciones</div>
        <ul style="margin: 0; padding-left: 20px; color: #4b5563; font-size: 14px; line-height: 1.8;">
            <li>El archivo debe tener dos hojas: <strong>CORREOS & WHATSAPP</strong> y <strong>LLAMADAS</strong>.</li>
            <li>El sistema combinará ambas hojas en una sola tabla.</li>
            <li>Se cruzará automáticamente el <strong>codigo_gestion</strong> con la tabla de mapeo para obtener:
                <ul>
                    <li><strong>MEJOR GESTIÓN_JAMAR</strong></li>
                    <li><strong>RESULTADO</strong> (COMPROMISO DE PAGO, CONTACTO EFECTIVO, NO CONTACTOS, CONTACTO CON TERCERO)</li>
                </ul>
            </li>
            <li>Los datos se guardan en la tabla <strong>gestiones_jamar</strong> (histórico).</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # ---- Subida de archivo ----
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
        
        # Procesar archivo
        with st.spinner("📊 Procesando archivo..."):
            try:
                df = leer_archivo_gestiones(uploaded_file)
                
                # Mostrar vista previa
                st.markdown("---")
                st.markdown("#### 📊 Vista previa del archivo (combinado)")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Estadísticas
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    st.metric("Columnas", f"{len(df.columns)}")
                with col3:
                    # Contar códigos de gestión únicos
                    codigos_unicos = df['codigo_gestion'].nunique() if 'codigo_gestion' in df.columns else 0
                    st.metric("Códigos únicos", f"{codigos_unicos:,}")
                
                # Botón para procesar
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
                
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)
    
    st.markdown('</div>', unsafe_allow_html=True)

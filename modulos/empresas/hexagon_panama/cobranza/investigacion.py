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
COLUMNAS_REQUERIDAS = ['cedula', 'numero']  # Para teléfonos
COLUMNAS_CORREO = ['cedula', 'correo']      # Para correos

# ============================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================

def validar_telefono(numero):
    """Valida un número de teléfono para Panamá"""
    if not numero or str(numero).strip() in ['', '0', '000', 'nan']:
        return None
    
    # Limpiar: solo dígitos
    limpio = re.sub(r'[^0-9]', '', str(numero))
    
    # Verificar longitud
    if len(limpio) not in [7, 8]:
        return None
    
    # Verificar que no sea todo ceros
    if limpio.count('0') == len(limpio):
        return None
    
    # Si empieza con 6, debe tener 8 dígitos
    if limpio.startswith('6') and len(limpio) != 8:
        return None
    
    # Si empieza con otro dígito, puede tener 7 u 8
    if not limpio.startswith('6') and len(limpio) not in [7, 8]:
        return None
    
    return limpio

def validar_correo(correo):
    """Valida un correo electrónico (formato básico)"""
    if not correo or str(correo).strip() in ['', 'nan']:
        return None
    correo = str(correo).strip().lower()
    if '@' not in correo or '.' not in correo:
        return None
    return correo

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    query = f"""
        SELECT id_proyecto, nombre
        FROM `{PROYECTO_BQ}.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    return ejecutar_query(query)

def obtener_personas_por_cedula(cedulas):
    """Obtiene id_persona para una lista de cédulas"""
    if not cedulas:
        return pd.DataFrame()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT id_persona, identificacion, nombre
        FROM `{PROYECTO_BQ}.personas`
        WHERE identificacion IN ('{cedulas_escapadas}')
    """
    return ejecutar_query(query)

def obtener_telefonos_existentes(proyecto_id, cedulas):
    """Obtiene teléfonos existentes para un proyecto y lista de cédulas"""
    if not cedulas:
        return set()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT 
            p.identificacion,
            t.numero
        FROM `{PROYECTO_BQ}.telefonos_proyecto` tp
        JOIN `{PROYECTO_BQ}.personas` p ON tp.id_persona = p.id_persona
        JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE tp.id_proyecto = '{proyecto_id}'
          AND p.identificacion IN ('{cedulas_escapadas}')
    """
    df = ejecutar_query(query)
    return set(zip(df['identificacion'], df['numero'])) if not df.empty else set()

def obtener_correos_existentes(proyecto_id, cedulas):
    """Obtiene correos existentes para un proyecto y lista de cédulas"""
    if not cedulas:
        return set()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT 
            p.identificacion,
            c.correo
        FROM `{PROYECTO_BQ}.correos_proyecto` cp
        JOIN `{PROYECTO_BQ}.personas` p ON cp.id_persona = p.id_persona
        JOIN `{PROYECTO_BQ}.correos` c ON cp.id_correo = c.id_correo
        WHERE cp.id_proyecto = '{proyecto_id}'
          AND p.identificacion IN ('{cedulas_escapadas}')
    """
    df = ejecutar_query(query)
    return set(zip(df['identificacion'], df['correo'])) if not df.empty else set()

def insertar_telefonos_batch(telefonos_nuevos):
    """Inserta teléfonos nuevos en lote"""
    if not telefonos_nuevos:
        return
    valores = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.telefonos` (id_telefono, numero)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_telefonos_proyecto_batch(relaciones):
    """Inserta relaciones telefonos_proyecto en lote"""
    if not relaciones:
        return
    valores = [f"""(
        '{r['id_telefono']}',
        '{r['id_persona']}',
        '{r['id_proyecto']}',
        '{r['fuente']}',
        {r['prioridad']},
        '{r['estado']}'
    )""" for r in relaciones]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.telefonos_proyecto`
        (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_correos_batch(correos_nuevos):
    """Inserta correos nuevos en lote"""
    if not correos_nuevos:
        return
    valores = [f"('{c['id_correo']}', '{c['correo']}')" for c in correos_nuevos]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.correos` (id_correo, correo)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_correos_proyecto_batch(relaciones):
    """Inserta relaciones correos_proyecto en lote"""
    if not relaciones:
        return
    valores = [f"""(
        '{r['id_correo']}',
        '{r['id_persona']}',
        '{r['id_proyecto']}',
        '{r['fuente']}',
        {r['prioridad']},
        '{r['estado']}'
    )""" for r in relaciones]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.correos_proyecto`
        (id_correo, id_persona, id_proyecto, fuente, prioridad, estado)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

# ============================================================
# PROCESO DE ANEXADO
# ============================================================

def anexar_investigacion(df, proyecto_id, tipo):
    """
    Anexa teléfonos o correos de investigación a Cobranza.
    
    Args:
        df: DataFrame con columnas 'cedula' y 'numero' o 'correo'
        proyecto_id: ID del proyecto
        tipo: 'telefonos' o 'correos'
    """
    import time
    start_time = time.time()
    
    total = len(df)
    anexados = 0
    errores = 0
    detalles = []
    
    # Determinar columna de valor
    col_valor = 'numero' if tipo == 'telefonos' else 'correo'
    
    # Validar columnas
    if 'cedula' not in df.columns or col_valor not in df.columns:
        return total, 0, total, f"Faltan columnas: 'cedula' y '{col_valor}'"
    
    # Limpiar datos
    cedulas = df['cedula'].astype(str).str.strip().dropna().unique().tolist()
    
    # Obtener personas existentes
    df_personas = obtener_personas_por_cedula(cedulas)
    map_cedula_a_id = dict(zip(df_personas['identificacion'], df_personas['id_persona']))
    
    # Obtener existentes en el proyecto
    if tipo == 'telefonos':
        existentes_set = obtener_telefonos_existentes(proyecto_id, cedulas)
        validar_valor = validar_telefono
        fuente = 'INVESTIGACION'
    else:
        existentes_set = obtener_correos_existentes(proyecto_id, cedulas)
        validar_valor = validar_correo
        fuente = 'INVESTIGACION'
    
    # Preparar datos para insertar
    nuevos_catalogos = []
    nuevas_relaciones = []
    
    # Cache para IDs ya creados en esta carga
    cache_ids = {}
    
    for _, row in df.iterrows():
        cedula = str(row['cedula']).strip()
        valor_raw = str(row[col_valor]).strip()
        
        # Validar valor
        valor_limpio = validar_valor(valor_raw)
        if not valor_limpio:
            errores += 1
            detalles.append(f"Cédula {cedula}: {col_valor} inválido '{valor_raw}'")
            continue
        
        # Verificar si la persona existe
        if cedula not in map_cedula_a_id:
            errores += 1
            detalles.append(f"Cédula {cedula}: persona no encontrada en Cobranza")
            continue
        
        id_persona = map_cedula_a_id[cedula]
        
        # Verificar si ya existe para esta persona en el proyecto
        if (cedula, valor_limpio) in existentes_set:
            continue  # Ya existe, no hacer nada
        
        # Verificar si ya se creó en esta misma carga
        if valor_limpio in cache_ids:
            id_valor = cache_ids[valor_limpio]
        else:
            # Crear nuevo en catálogo
            id_valor = str(uuid.uuid4())
            cache_ids[valor_limpio] = id_valor
            nuevos_catalogos.append({
                'id': id_valor,
                'valor': valor_limpio
            })
        
        # Crear relación con el proyecto
        nuevas_relaciones.append({
            'id_valor': id_valor,
            'id_persona': id_persona,
            'id_proyecto': proyecto_id,
            'fuente': fuente,
            'prioridad': 10,  # Prioridad más baja que BASE
            'estado': 'ACTIVO'
        })
    
    # Insertar en BigQuery
    if nuevos_catalogos:
        if tipo == 'telefonos':
            insertar_telefonos_batch([{'id_telefono': c['id'], 'numero': c['valor']} for c in nuevos_catalogos])
        else:
            insertar_correos_batch([{'id_correo': c['id'], 'correo': c['valor']} for c in nuevos_catalogos])
    
    if nuevas_relaciones:
        if tipo == 'telefonos':
            relaciones = [{
                'id_telefono': r['id_valor'],
                'id_persona': r['id_persona'],
                'id_proyecto': r['id_proyecto'],
                'fuente': r['fuente'],
                'prioridad': r['prioridad'],
                'estado': r['estado']
            } for r in nuevas_relaciones]
            insertar_telefonos_proyecto_batch(relaciones)
        else:
            relaciones = [{
                'id_correo': r['id_valor'],
                'id_persona': r['id_persona'],
                'id_proyecto': r['id_proyecto'],
                'fuente': r['fuente'],
                'prioridad': r['prioridad'],
                'estado': r['estado']
            } for r in nuevas_relaciones]
            insertar_correos_proyecto_batch(relaciones)
    
    anexados = len(nuevas_relaciones)
    elapsed_time = time.time() - start_time
    detalle = f"{anexados} {tipo} anexados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    
    return total, anexados, errores, detalle

# ============================================================
# GENERAR REPORTE DE INVESTIGACIÓN (EXCEL)
# ============================================================

def generar_reporte_investigacion(proyecto_id):
    """Genera Excel con todas las investigaciones del proyecto"""
    # 1. Personas + Cuentas
    query_personas = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            c.cuenta,
            c.empresa,
            c.ocupacion,
            c.direccion,
            c.saldo,
            c.fecha_ultimo_pago,
            c.dias_mora,
            c.cartera
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.cuentas` c ON p.id_persona = c.id_persona
        WHERE c.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre
    """
    df_personas = ejecutar_query(query_personas)
    
    # 2. Teléfonos
    query_telefonos = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            t.numero,
            t.tipo,
            tp.fuente AS origen,
            tp.prioridad,
            tp.cant_toques,
            tp.cant_contactos,
            tp.cant_no_contactos,
            tp.estado
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.telefonos_proyecto` tp ON p.id_persona = tp.id_persona
        JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE tp.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre, tp.prioridad
    """
    df_telefonos = ejecutar_query(query_telefonos)
    
    # 3. Correos
    query_correos = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            c.correo,
            cp.fuente AS origen,
            cp.prioridad,
            cp.estado
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.correos_proyecto` cp ON p.id_persona = cp.id_persona
        JOIN `{PROYECTO_BQ}.correos` c ON cp.id_correo = c.id_correo
        WHERE cp.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre, cp.prioridad
    """
    df_correos = ejecutar_query(query_correos)
    
    # 4. Resumen
    resumen = {
        "Proyecto": proyecto_id,
        "Fecha generación": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Total clientes": len(df_personas),
        "Total teléfonos": len(df_telefonos),
        "Total correos": len(df_correos),
        "Teléfonos BASE": len(df_telefonos[df_telefonos['origen'] == 'BASE']) if not df_telefonos.empty else 0,
        "Teléfonos INVESTIGACION": len(df_telefonos[df_telefonos['origen'] == 'INVESTIGACION']) if not df_telefonos.empty else 0,
        "Teléfonos INACTIVOS": len(df_telefonos[df_telefonos['estado'] == 'INACTIVO']) if not df_telefonos.empty else 0,
    }
    df_resumen = pd.DataFrame([resumen])
    
    return {
        'resumen': df_resumen,
        'personas': df_personas,
        'telefonos': df_telefonos,
        'correos': df_correos
    }

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    st.markdown("""
    <style>
        .main-header { font-size: 24px; font-weight: 600; color: #1a1a1a; margin-bottom: 8px; }
        .sub-header { font-size: 14px; color: #6b6b6b; margin-bottom: 24px; }
        .card {
            background-color: #ffffff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
            border: 1px solid #f0f0f0;
            margin-bottom: 16px;
        }
        .card-title { font-size: 16px; font-weight: 500; color: #1a1a1a; margin-bottom: 12px; }
        .btn-primary {
            background-color: #dc2626;
            color: white;
            border: none;
            padding: 12px 32px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 16px;
            cursor: pointer;
            width: 100%;
            transition: background-color 0.2s;
        }
        .btn-primary:hover { background-color: #b91c1c; }
        .btn-primary:disabled { background-color: #9ca3af; cursor: not-allowed; }
        .btn-outline {
            background-color: transparent;
            color: #dc2626;
            border: 1px solid #dc2626;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
        }
        .btn-outline:hover { background-color: #fef2f2; }
        .metric-card {
            background-color: #f8fafc;
            border-radius: 8px;
            padding: 16px;
            text-align: center;
            border: 1px solid #e5e7eb;
        }
        .metric-value { font-size: 24px; font-weight: 600; color: #1a1a1a; }
        .metric-label { font-size: 13px; color: #6b6b6b; }
        .helper-text { font-size: 13px; color: #6b6b6b; margin-top: 4px; }
        .project-selector { margin-bottom: 16px; }
        .project-selector label { font-weight: 500; color: #1a1a1a; font-size: 14px; }
        .upload-area {
            border: 2px dashed #d1d5db;
            border-radius: 12px;
            padding: 32px 24px;
            text-align: center;
            background-color: #fafafa;
            margin: 16px 0;
            transition: border-color 0.2s;
        }
        .upload-area:hover { border-color: #dc2626; background-color: #fef2f2; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">🔍 Investigación y Anexado</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube un archivo con resultados de investigación (teléfonos o correos nuevos) para anexarlos a Cobranza.</div>', unsafe_allow_html=True)
    
    # ============================================================
    # CARD PRINCIPAL
    # ============================================================
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # ---- Selector de Proyecto ----
    proyectos_df = obtener_proyectos_activos()
    
    if len(proyectos_df) == 0:
        st.warning("⚠️ No hay proyectos activos en el sistema.")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    opciones_proyectos = {row['nombre']: row['id_proyecto'] for _, row in proyectos_df.iterrows()}
    nombres_proyectos = list(opciones_proyectos.keys())
    
    proyecto_seleccionado_nombre = st.selectbox(
        "🏢 Proyecto",
        nombres_proyectos,
        index=0 if nombres_proyectos else None,
        help="Selecciona el proyecto al que pertenecen los clientes"
    )
    proyecto_seleccionado = opciones_proyectos.get(proyecto_seleccionado_nombre)
    
    st.markdown('<div class="helper-text">Los datos investigados se anexarán a este proyecto con origen "INVESTIGACION".</div>', unsafe_allow_html=True)
    
    # ---- Tipo de anexado ----
    tipo_anexado = st.radio(
        "📌 Selecciona el tipo de dato a anexar",
        ["📱 Teléfonos", "📧 Correos"],
        horizontal=True,
        help="Elige si estás subiendo teléfonos o correos"
    )
    tipo = "telefonos" if "Teléfonos" in tipo_anexado else "correos"
    
    # ---- Área de Subida ----
    uploaded_file = st.file_uploader(
        f"📤 Sube archivo con {tipo} nuevos",
        type=["xlsx", "xls", "csv"],
        help=f"El archivo debe tener columnas 'cedula' y '{'numero' if tipo == 'telefonos' else 'correo'}'",
        key="investigacion_uploader"
    )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ============================================================
    # PROCESAMIENTO
    # ============================================================
    
    if uploaded_file is not None:
        with st.spinner("📊 Procesando archivo..."):
            try:
                # Leer archivo
                if uploaded_file.name.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(uploaded_file)
                else:
                    import io
                    from io import StringIO
                    contenido = uploaded_file.getvalue().decode('utf-8-sig')
                    for sep in [',', ';']:
                        try:
                            df = pd.read_csv(StringIO(contenido), sep=sep, dtype=str)
                            if len(df.columns) > 1:
                                break
                        except:
                            continue
                
                # Mostrar vista previa
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown('<div class="card-title">📊 Vista previa del archivo</div>', unsafe_allow_html=True)
                st.dataframe(df.head(10), use_container_width=True)
                st.markdown(f'**Total:** {len(df)} filas')
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Botón para anexar
                if st.button(f"🚀 Anexar {tipo} a Cobranza", type="primary", use_container_width=True):
                    with st.spinner(f"🔄 Anexando {tipo}..."):
                        total, anexados, errores, detalle = anexar_investigacion(df, proyecto_seleccionado, tipo)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📊 Total", f"{total:,}")
                        with col2:
                            st.metric("✅ Anexados", f"{anexados:,}")
                        with col3:
                            st.metric("❌ Errores", f"{errores:,}")
                        
                        if errores == 0:
                            st.success(f"✅ {detalle}")
                        else:
                            st.warning(f"⚠️ {detalle}")
                        
                        # Botón para descargar reporte actualizado
                        st.markdown("---")
                        st.markdown("### 📥 Descargar reporte completo")
                        
                        with st.spinner("📊 Generando reporte..."):
                            data_reporte = generar_reporte_investigacion(proyecto_seleccionado)
                            excel_bytes = generar_excel_reporte(data_reporte)
                            st.download_button(
                                label="📥 Descargar Reporte de Investigación",
                                data=excel_bytes,
                                file_name=f"INVESTIGACION_{proyecto_seleccionado}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)

    # ============================================================
    # DESCARGA DIRECTA (sin subir)
    # ============================================================
    
    if proyecto_seleccionado:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📥 Descargar reporte de investigación</div>', unsafe_allow_html=True)
        st.markdown('<div class="helper-text">Descarga el reporte completo del proyecto seleccionado (clientes, teléfonos, correos).</div>', unsafe_allow_html=True)
        
        with st.spinner(f"📊 Generando reporte para {proyecto_seleccionado_nombre}..."):
            try:
                data_reporte = generar_reporte_investigacion(proyecto_seleccionado)
                
                # Mostrar métricas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("👥 Clientes", f"{len(data_reporte['personas']):,}")
                with col2:
                    st.metric("📱 Teléfonos", f"{len(data_reporte['telefonos']):,}")
                with col3:
                    st.metric("📧 Correos", f"{len(data_reporte['correos']):,}")
                with col4:
                    total_inv = len(data_reporte['telefonos'][data_reporte['telefonos']['origen'] == 'INVESTIGACION']) if not data_reporte['telefonos'].empty else 0
                    st.metric("🔍 Investigados", f"{total_inv:,}")
                
                excel_bytes = generar_excel_reporte(data_reporte)
                st.download_button(
                    label="📥 Descargar Reporte Completo",
                    data=excel_bytes,
                    file_name=f"INVESTIGACION_{proyecto_seleccionado}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"❌ Error al generar el reporte: {str(e)}")
                st.exception(e)
        
        st.markdown('</div>', unsafe_allow_html=True)

def generar_excel_reporte(data):
    """Genera un archivo Excel con múltiples hojas"""
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Hoja 1: Resumen
        data['resumen'].to_excel(writer, sheet_name='Resumen', index=False)
        
        # Hoja 2: Personas
        if not data['personas'].empty:
            data['personas'].to_excel(writer, sheet_name='Clientes', index=False)
        
        # Hoja 3: Teléfonos
        if not data['telefonos'].empty:
            data['telefonos'].to_excel(writer, sheet_name='Teléfonos', index=False)
        
        # Hoja 4: Correos
        if not data['correos'].empty:
            data['correos'].to_excel(writer, sheet_name='Correos', index=False)
        
        # Ajustar anchos
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue()

# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    render()

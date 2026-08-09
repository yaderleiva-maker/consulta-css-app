import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Importar servicios de Hexagon
from services.bigquery import ejecutar_query
from services.archivos import leer_excel, validar_columnas

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_COBRANZA = "proyecto-css-panama.cobranza"
PROYECTO_CONSULTAS = "proyecto-css-panama.consultas"
PROYECTO_CSS = "proyecto-css-panama.css_data"

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    """Obtiene lista de proyectos activos desde BigQuery"""
    query = f"""
        SELECT 
            id_proyecto,
            nombre
        FROM `{PROYECTO_COBRANZA}.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.warning(f"⚠️ No se pudieron cargar los proyectos: {str(e)}")
        return pd.DataFrame()

def consultar_css(cedulas):
    """
    Consulta la base de datos CSS para obtener información de las cédulas.
    Retorna: DataFrame con cedula, nombre, patrono, razon_social, tel1, fecha, salario
    """
    if not cedulas:
        return pd.DataFrame()
    
    # Escapar las cédulas para la consulta SQL
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    
    query = f"""
        SELECT 
            cedula,
            NOMBRE,
            PATRONO,
            RAZON_SO,
            TEL1,
            FECHA,
            SALARIO
        FROM `{PROYECTO_CSS}.css-actual`
        WHERE cedula IN ('{cedulas_escapadas}')
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.error(f"❌ Error al consultar CSS: {str(e)}")
        return pd.DataFrame()

def obtener_telefonos_existentes(cedulas):
    """
    Obtiene los teléfonos actuales en la base de Cobranza para las cédulas dadas
    """
    if not cedulas:
        return pd.DataFrame()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    
    query = f"""
        SELECT 
            p.identificacion,
            t.numero,
            tp.fuente
        FROM `{PROYECTO_COBRANZA}.telefonos_proyecto` tp
        JOIN `{PROYECTO_COBRANZA}.personas` p ON tp.id_persona = p.id_persona
        JOIN `{PROYECTO_COBRANZA}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE p.identificacion IN ('{cedulas_escapadas}')
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.error(f"❌ Error al obtener teléfonos existentes: {str(e)}")
        return pd.DataFrame()

def obtener_correos_existentes(cedulas):
    """
    Obtiene los correos actuales en la base de Cobranza para las cédulas dadas
    """
    if not cedulas:
        return pd.DataFrame()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    
    query = f"""
        SELECT 
            p.identificacion,
            c.correo,
            cp.fuente
        FROM `{PROYECTO_COBRANZA}.correos_proyecto` cp
        JOIN `{PROYECTO_COBRANZA}.personas` p ON cp.id_persona = p.id_persona
        JOIN `{PROYECTO_COBRANZA}.correos` c ON cp.id_correo = c.id_correo
        WHERE p.identificacion IN ('{cedulas_escapadas}')
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.error(f"❌ Error al obtener correos existentes: {str(e)}")
        return pd.DataFrame()

def obtener_personas_por_cedula(cedulas):
    """
    Obtiene id_persona y nombre para las cédulas dadas
    """
    if not cedulas:
        return pd.DataFrame()
    
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    
    query = f"""
        SELECT 
            id_persona,
            identificacion,
            nombre
        FROM `{PROYECTO_COBRANZA}.personas`
        WHERE identificacion IN ('{cedulas_escapadas}')
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.error(f"❌ Error al obtener personas: {str(e)}")
        return pd.DataFrame()

def normalizar_telefono(valor):
    """Limpia y normaliza un número de teléfono"""
    if pd.isna(valor) or not str(valor).strip():
        return None
    # Eliminar espacios, guiones, paréntesis
    valor = str(valor).strip()
    valor = valor.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    # Si tiene .0 al final, quitarlo
    if valor.endswith('.0'):
        valor = valor[:-2]
    # Verificar que solo tenga dígitos
    if not valor.isdigit():
        return None
    # Verificar longitud (7 u 8 dígitos para Panamá)
    if len(valor) not in [7, 8]:
        return None
    return valor

def normalizar_correo(valor):
    """Limpia y normaliza un correo electrónico"""
    if pd.isna(valor) or not str(valor).strip():
        return None
    valor = str(valor).strip().lower()
    # Verificar formato básico
    if '@' not in valor or '.' not in valor:
        return None
    return valor

# ============================================================
# PROCESO DE INVESTIGACIÓN Y ANEXADO
# ============================================================

def procesar_investigacion(df, proyecto_seleccionado):
    """
    Procesa el archivo de cédulas, consulta CSS y anexa los datos a Cobranza
    """
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    detalles = []
    
    # ============================================================
    # PASO 1: Validar y extraer cédulas
    # ============================================================
    
    # Buscar columna de cédula
    columna_cedula = None
    for col in df.columns:
        if col.lower() in ['cedula', 'cédula', 'identificacion', 'id']:
            columna_cedula = col
            break
    
    if not columna_cedula:
        return total, 0, total, "No se encontró columna de cédula. Busca: 'cedula', 'cédula', 'identificacion' o 'id'"
    
    # Limpiar cédulas
    cedulas = df[columna_cedula].astype(str).str.strip()
    cedulas = cedulas[cedulas != '']
    cedulas = cedulas.dropna().unique().tolist()
    
    if not cedulas:
        return total, 0, total, "No se encontraron cédulas válidas en el archivo"
    
    st.info(f"📌 {len(cedulas)} cédulas únicas encontradas")
    
    # ============================================================
    # PASO 2: Consultar CSS
    # ============================================================
    
    with st.spinner(f"🔍 Consultando CSS para {len(cedulas)} cédulas..."):
        df_css = consultar_css(cedulas)
    
    if df_css.empty:
        return total, 0, total, "No se encontraron datos en CSS para las cédulas proporcionadas"
    
    st.success(f"✅ {len(df_css)} registros encontrados en CSS")
    
    # ============================================================
    # PASO 3: Obtener personas existentes en Cobranza
    # ============================================================
    
    with st.spinner("👤 Buscando personas en Cobranza..."):
        df_personas = obtener_personas_por_cedula(cedulas)
        map_cedula_a_id_persona = dict(zip(df_personas['identificacion'], df_personas['id_persona']))
    
    # ============================================================
    # PASO 4: Obtener teléfonos y correos existentes
    # ============================================================
    
    with st.spinner("📱 Buscando teléfonos existentes..."):
        df_telefonos_existentes = obtener_telefonos_existentes(cedulas)
        # Crear set de teléfonos existentes por cédula
        telefonos_existentes_set = set()
        for _, row in df_telefonos_existentes.iterrows():
            telefono = normalizar_telefono(row['numero'])
            if telefono:
                telefonos_existentes_set.add((row['identificacion'], telefono))
    
    with st.spinner("📧 Buscando correos existentes..."):
        df_correos_existentes = obtener_correos_existentes(cedulas)
        correos_existentes_set = set()
        for _, row in df_correos_existentes.iterrows():
            correo = normalizar_correo(row['correo'])
            if correo:
                correos_existentes_set.add((row['identificacion'], correo))
    
    # ============================================================
    # PASO 5: Preparar datos para insertar
    # ============================================================
    
    telefonos_nuevos = []
    telefonos_proyecto_nuevos = []
    correos_nuevos = []
    correos_proyecto_nuevos = []
    empresas_actualizadas = []
    
    # Diccionario para cachear teléfonos ya creados en esta carga
    cache_telefonos = {}
    cache_correos = {}
    
    # También necesitamos los IDs de teléfonos y correos existentes
    # Obtener todos los teléfonos y correos en la base (para asignar IDs)
    if telefonos_existentes_set:
        telefonos_numeros = [t[1] for t in telefonos_existentes_set]
        tel_list = "', '".join(telefonos_numeros)
        query_telefonos_ids = f"""
            SELECT numero, id_telefono
            FROM `{PROYECTO_COBRANZA}.telefonos`
            WHERE numero IN ('{tel_list}')
        """
        df_telefonos_ids = ejecutar_query(query_telefonos_ids)
        map_telefono_a_id = dict(zip(df_telefonos_ids['numero'], df_telefonos_ids['id_telefono']))
    else:
        map_telefono_a_id = {}
    
    if correos_existentes_set:
        correos_lista = [c[1] for c in correos_existentes_set]
        corr_list = "', '".join(correos_lista)
        query_correos_ids = f"""
            SELECT correo, id_correo
            FROM `{PROYECTO_COBRANZA}.correos`
            WHERE correo IN ('{corr_list}')
        """
        df_correos_ids = ejecutar_query(query_correos_ids)
        map_correo_a_id = dict(zip(df_correos_ids['correo'], df_correos_ids['id_correo']))
    else:
        map_correo_a_id = {}
    
    # Procesar cada registro de CSS
    for _, row in df_css.iterrows():
        cedula = str(row['cedula']).strip()
        
        if cedula not in map_cedula_a_id_persona:
            # Esta persona no existe en Cobranza, la saltamos (debería existir para anexar)
            continue
        
        id_persona = map_cedula_a_id_persona[cedula]
        
        # ---- TELÉFONOS ----
        telefono_css = normalizar_telefono(row.get('TEL1'))
        if telefono_css:
            # Verificar si ya existe para esta persona
            if (cedula, telefono_css) not in telefonos_existentes_set:
                # Verificar si el teléfono existe en el catálogo
                if telefono_css not in map_telefono_a_id:
                    # Teléfono nuevo en el catálogo
                    id_telefono = str(uuid.uuid4())
                    map_telefono_a_id[telefono_css] = id_telefono
                    telefonos_nuevos.append({
                        'id_telefono': id_telefono,
                        'numero': telefono_css
                    })
                else:
                    id_telefono = map_telefono_a_id[telefono_css]
                
                # Agregar relación con el proyecto
                telefonos_proyecto_nuevos.append({
                    'id_telefono': id_telefono,
                    'id_persona': id_persona,
                    'id_proyecto': proyecto_seleccionado,
                    'fuente': 'INVESTIGACION_CSS',
                    'prioridad': 10,  # Prioridad más baja que los de carga inicial
                    'estado': 'ACTIVO'
                })
        
        # ---- CORREOS ----
        # En el CSS actual solo tenemos un teléfono, no correos.
        # Pero si en el futuro se agregan correos, aquí va la lógica.
        # Por ahora dejamos la estructura preparada.
        
        # ---- EMPRESA ----
        empresa_css = str(row.get('RAZON_SO', '')).strip()
        if empresa_css and empresa_css != 'nan':
            empresas_actualizadas.append({
                'cedula': cedula,
                'id_persona': id_persona,
                'empresa_nueva': empresa_css,
                'patrono': str(row.get('PATRONO', '')).strip(),
                'fecha': str(row.get('FECHA', '')).strip(),
                'salario': row.get('SALARIO')
            })
    
    # ============================================================
    # PASO 6: Insertar en BigQuery
    # ============================================================
    
    registros_insertados = 0
    
    # 6.1 Insertar teléfonos nuevos
    if telefonos_nuevos:
        valores_telefonos = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
        insert_telefonos = f"""
            INSERT INTO `{PROYECTO_COBRANZA}.telefonos`
            (id_telefono, numero)
            VALUES {', '.join(valores_telefonos)}
        """
        ejecutar_query(insert_telefonos)
        registros_insertados += len(telefonos_nuevos)
    
    # 6.2 Insertar relaciones telefonos_proyecto
    if telefonos_proyecto_nuevos:
        valores_rel_tel = []
        for t in telefonos_proyecto_nuevos:
            valores_rel_tel.append(f"""(
                '{t['id_telefono']}',
                '{t['id_persona']}',
                '{t['id_proyecto']}',
                '{t['fuente']}',
                {t['prioridad']},
                '{t['estado']}'
            )""")
        
        insert_rel_tel = f"""
            INSERT INTO `{PROYECTO_COBRANZA}.telefonos_proyecto`
            (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
            VALUES {', '.join(valores_rel_tel)}
        """
        ejecutar_query(insert_rel_tel)
        registros_insertados += len(telefonos_proyecto_nuevos)
    
    # 6.3 Actualizar empresas en cuentas
    if empresas_actualizadas:
        for emp in empresas_actualizadas:
            # Buscar la cuenta más reciente de esta persona
            query_cuenta = f"""
                SELECT id_cuenta
                FROM `{PROYECTO_COBRANZA}.cuentas`
                WHERE id_persona = '{emp['id_persona']}'
                  AND id_proyecto = '{proyecto_seleccionado}'
                ORDER BY created_at DESC
                LIMIT 1
            """
            df_cuenta = ejecutar_query(query_cuenta)
            
            if not df_cuenta.empty():
                id_cuenta = df_cuenta['id_cuenta'].iloc[0]
                update_empresa = f"""
                    UPDATE `{PROYECTO_COBRANZA}.cuentas`
                    SET empresa = '{emp['empresa_nueva']}',
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE id_cuenta = '{id_cuenta}'
                """
                ejecutar_query(update_empresa)
                registros_insertados += 1
    
    # ============================================================
    # PASO 7: Generar reportes
    # ============================================================
    
    # Reporte de teléfonos nuevos
    df_telefonos_nuevos = pd.DataFrame(telefonos_proyecto_nuevos)
    if not df_telefonos_nuevos.empty:
        # Unir con personas para mostrar cédula y nombre
        df_personas_report = df_personas.set_index('id_persona')
        df_telefonos_report = df_telefonos_nuevos.copy()
        df_telefonos_report['cedula'] = df_telefonos_report['id_persona'].map(df_personas_report['identificacion'])
        df_telefonos_report['nombre'] = df_telefonos_report['id_persona'].map(df_personas_report['nombre'])
        df_telefonos_report = df_telefonos_report[['cedula', 'nombre', 'id_telefono', 'fuente']]
    else:
        df_telefonos_report = pd.DataFrame()
    
    # Reporte de empresas actualizadas
    df_empresas_report = pd.DataFrame(empresas_actualizadas)
    if not df_empresas_report.empty:
        df_empresas_report = df_empresas_report[['cedula', 'empresa_nueva', 'patrono', 'fecha', 'salario']]
        df_empresas_report.columns = ['cedula', 'empresa', 'patrono', 'fecha', 'salario']
    
    elapsed_time = time.time() - start_time
    detalle = f"{registros_insertados} registros anexados. Tiempo: {elapsed_time:.2f}s"
    
    return {
        'total': total,
        'procesados': len(df_css),
        'errores': errores,
        'detalle': detalle,
        'telefonos_nuevos': df_telefonos_report,
        'empresas_actualizadas': df_empresas_report,
        'registros_insertados': registros_insertados
    }

# ============================================================
# GENERAR PLANTILLA
# ============================================================

def generar_plantilla_investigacion():
    """Genera un archivo Excel con el formato para investigación"""
    data = {
        'cedula': ['8-123-456', '8-789-012', '1-234-567'],
        'nombre': ['JUAN PEREZ', 'MARIA LOPEZ', 'CARLOS RUIZ']
    }
    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Investigacion', index=False)
        
        instrucciones = pd.DataFrame({
            'Instrucciones': [
                'FORMATO DE INVESTIGACIÓN HEXAGON - COBRANZA',
                '',
                '📌 COLUMNAS OBLIGATORIAS:',
                '  • cedula: Cédula del cliente a investigar',
                '  • nombre: Nombre del cliente (opcional pero recomendado)',
                '',
                '⚠️ El sistema consultará automáticamente:',
                '  • CSS para obtener empresa actual y teléfono',
                '  • Directorio para obtener correos y teléfonos adicionales',
                '',
                '📌 Los datos encontrados se ANEXARÁN a:',
                '  • Teléfonos (si no existen)',
                '  • Correos (si no existen)',
                '  • Empresa (se actualiza en la cuenta)'
            ]
        })
        instrucciones.to_excel(writer, sheet_name='Instrucciones', index=False, header=False)
    
    return output.getvalue()

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para la vista de Investigación"""
    
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
        .upload-area {
            border: 2px dashed #d1d5db;
            border-radius: 12px;
            padding: 32px 24px;
            text-align: center;
            background-color: #fafafa;
            margin: 16px 0;
        }
        .upload-area:hover { border-color: #dc2626; background-color: #fef2f2; }
        .status-success { color: #16a34a; font-weight: 500; }
        .status-warning { color: #ea580c; font-weight: 500; }
        .status-error { color: #dc2626; font-weight: 500; }
        .btn-primary {
            background-color: #dc2626;
            color: white;
            border: none;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            width: 100%;
        }
        .btn-primary:hover { background-color: #b91c1c; }
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
    </style>
    """, unsafe_allow_html=True)
    
    # ============================================================
    # HEADER
    # ============================================================
    
    st.markdown('<div class="main-header">🔍 Investigación y Anexado</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube un archivo con cédulas para consultar fuentes externas y anexar los datos encontrados a Cobranza.</div>', unsafe_allow_html=True)
    
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
    proyecto_seleccionado = opciones_proyectos.get(proyecto_selecciono_nombre)
    
    st.markdown('<div class="helper-text">Los datos investigados se anexarán a este proyecto.</div>', unsafe_allow_html=True)
    
    # ---- Botón Descargar Plantilla ----
    col1, col2 = st.columns([4, 1])
    with col2:
        plantilla_bytes = generar_plantilla_investigacion()
        st.download_button(
            label="📄 Plantilla",
            data=plantilla_bytes,
            file_name="FORMATO_INVESTIGACION_HEXAGON.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    # ---- Área de Subida ----
    uploaded_file = st.file_uploader(
        "Sube tu archivo con cédulas",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
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
                    # Detectar separador para CSV
                    contenido = uploaded_file.getvalue().decode('utf-8-sig')
                    import io
                    from io import StringIO
                    
                    # Probar con coma y punto y coma
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
                
                # Botón para procesar
                if st.button("🚀 Procesar investigación", type="primary", use_container_width=True):
                    with st.spinner("🔍 Procesando investigación..."):
                        resultado = procesar_investigacion(df, proyecto_seleccionado)
                        
                        # Mostrar resultados
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📊 Total cédulas", f"{resultado['total']:,}")
                        with col2:
                            st.metric("🔍 Encontrados en CSS", f"{resultado['procesados']:,}")
                        with col3:
                            st.metric("📱 Teléfonos anexados", f"{len(resultado['telefonos_nuevos']):,}")
                        with col4:
                            st.metric("🏢 Empresas actualizadas", f"{len(resultado['empresas_actualizadas']):,}")
                        
                        if resultado['errores'] == 0:
                            st.success(f"✅ {resultado['detalle']}")
                        else:
                            st.warning(f"⚠️ {resultado['detalle']}")
                        
                        # ---- Descargar reportes ----
                        st.markdown("---")
                        st.markdown("### 📥 Descargar resultados")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if not resultado['telefonos_nuevos'].empty:
                                csv_telefonos = resultado['telefonos_nuevos'].to_csv(index=False)
                                st.download_button(
                                    label="📱 Teléfonos anexados",
                                    data=csv_telefonos,
                                    file_name="telefonos_anexados.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            else:
                                st.info("No se encontraron teléfonos nuevos para anexar.")
                        
                        with col2:
                            if not resultado['empresas_actualizadas'].empty:
                                csv_empresas = resultado['empresas_actualizadas'].to_csv(index=False)
                                st.download_button(
                                    label="🏢 Empresas actualizadas",
                                    data=csv_empresas,
                                    file_name="empresas_actualizadas.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            else:
                                st.info("No se encontraron empresas para actualizar.")
                        
                        # Reporte completo (unir todo)
                        st.markdown("---")
                        st.markdown("### 📋 Reporte completo")
                        
                        # Crear reporte combinado
                        reportes = []
                        if not resultado['telefonos_nuevos'].empty:
                            reportes.append(resultado['telefonos_nuevos'])
                        if not resultado['empresas_actualizadas'].empty:
                            reportes.append(resultado['empresas_actualizadas'])
                        
                        if reportes:
                            # No combinamos porque tienen columnas diferentes
                            st.info("Los reportes individuales están disponibles arriba.")
                        else:
                            st.info("No se generaron reportes. Verifica que el archivo tenga cédulas válidas.")
                        
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)

# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    render()

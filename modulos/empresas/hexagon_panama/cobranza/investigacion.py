import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
import io
import time

from services.bigquery import ejecutar_query
from services.archivos import leer_excel

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_COBRANZA = "proyecto-css-panama.cobranza"
PROYECTO_CSS = "proyecto-css-panama.css_data"

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    query = f"""
        SELECT id_proyecto, nombre
        FROM `{PROYECTO_COBRANZA}.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    try:
        return ejecutar_query(query)
    except:
        return pd.DataFrame()

def consultar_css(cedulas):
    if not cedulas:
        return pd.DataFrame()
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT 
            cedula, NOMBRE, PATRONO, RAZON_SO, TEL1, FECHA, SALARIO
        FROM `{PROYECTO_CSS}.css-actual`
        WHERE cedula IN ('{cedulas_escapadas}')
    """
    try:
        return ejecutar_query(query)
    except:
        return pd.DataFrame()

def obtener_datos_completos_proyecto(proyecto):
    """Obtiene todos los datos de un proyecto para el reporte consolidado"""
    query = f"""
        WITH personas_cobranza AS (
            SELECT 
                p.id_persona,
                p.identificacion,
                p.nombre
            FROM `{PROYECTO_COBRANZA}.personas` p
            JOIN `{PROYECTO_COBRANZA}.cuentas` c ON p.id_persona = c.id_persona
            WHERE c.id_proyecto = '{proyecto}'
        ),
        telefonos_proyecto AS (
            SELECT 
                pc.identificacion,
                pc.nombre,
                t.numero,
                tp.fuente,
                tp.estado,
                tp.prioridad,
                'TELEFONO' AS tipo_contacto
            FROM personas_cobranza pc
            JOIN `{PROYECTO_COBRANZA}.telefonos_proyecto` tp ON pc.id_persona = tp.id_persona
            JOIN `{PROYECTO_COBRANZA}.telefonos` t ON tp.id_telefono = t.id_telefono
            WHERE tp.id_proyecto = '{proyecto}'
        ),
        correos_proyecto AS (
            SELECT 
                pc.identificacion,
                pc.nombre,
                c.correo AS numero,
                cp.fuente,
                cp.estado,
                cp.prioridad,
                'CORREO' AS tipo_contacto
            FROM personas_cobranza pc
            JOIN `{PROYECTO_COBRANZA}.correos_proyecto` cp ON pc.id_persona = cp.id_persona
            JOIN `{PROYECTO_COBRANZA}.correos` c ON cp.id_correo = c.id_correo
            WHERE cp.id_proyecto = '{proyecto}'
        ),
        empresas_proyecto AS (
            SELECT 
                pc.identificacion,
                pc.nombre,
                c.empresa,
                c.ocupacion,
                c.direccion
            FROM personas_cobranza pc
            JOIN `{PROYECTO_COBRANZA}.cuentas` c ON pc.id_persona = c.id_persona
            WHERE c.id_proyecto = '{proyecto}'
              AND c.empresa IS NOT NULL
        )
        SELECT * FROM telefonos_proyecto
        UNION ALL
        SELECT * FROM correos_proyecto
    """
    try:
        df_contactos = ejecutar_query(query)
    except:
        df_contactos = pd.DataFrame()
    
    try:
        df_empresas = ejecutar_query(f"""
            SELECT identificacion, nombre, empresa, ocupacion, direccion
            FROM empresas_proyecto
        """)
    except:
        df_empresas = pd.DataFrame()
    
    return df_contactos, df_empresas

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_telefono(valor):
    if pd.isna(valor) or not str(valor).strip():
        return None
    valor = str(valor).strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    if valor.endswith('.0'):
        valor = valor[:-2]
    if not valor.isdigit():
        return None
    if len(valor) not in [7, 8]:
        return None
    return valor

def normalizar_correo(valor):
    if pd.isna(valor) or not str(valor).strip():
        return None
    valor = str(valor).strip().lower()
    if '@' not in valor or '.' not in valor:
        return None
    return valor

# ============================================================
# PROCESO DE INVESTIGACIÓN
# ============================================================

def procesar_investigacion(df, proyecto):
    total = len(df)
    errores = 0
    
    # Encontrar columna de cédula
    col_cedula = None
    for col in df.columns:
        if col.lower() in ['cedula', 'cédula', 'identificacion', 'id']:
            col_cedula = col
            break
    if not col_cedula:
        return {
            'total': total,
            'errores': total,
            'detalle': 'No se encontró columna de cédula.',
            'telefonos_nuevos': pd.DataFrame(),
            'empresas_actualizadas': pd.DataFrame(),
            'df_css': pd.DataFrame()
        }
    
    cedulas = df[col_cedula].astype(str).str.strip()
    cedulas = cedulas[cedulas != ''].dropna().unique().tolist()
    
    if not cedulas:
        return {
            'total': total,
            'errores': total,
            'detalle': 'No se encontraron cédulas válidas.',
            'telefonos_nuevos': pd.DataFrame(),
            'empresas_actualizadas': pd.DataFrame(),
            'df_css': pd.DataFrame()
        }
    
    # Consultar CSS
    df_css = consultar_css(cedulas)
    if df_css.empty:
        return {
            'total': total,
            'errores': total,
            'detalle': 'No se encontraron datos en CSS.',
            'telefonos_nuevos': pd.DataFrame(),
            'empresas_actualizadas': pd.DataFrame(),
            'df_css': pd.DataFrame()
        }
    
    # Obtener personas existentes
    query_personas = f"""
        SELECT id_persona, identificacion, nombre
        FROM `{PROYECTO_COBRANZA}.personas`
        WHERE identificacion IN ('{"', '".join(cedulas)}')
    """
    df_personas = ejecutar_query(query_personas)
    map_personas = dict(zip(df_personas['identificacion'], df_personas['id_persona']))
    
    # Obtener teléfonos existentes
    query_telefonos_existentes = f"""
        SELECT p.identificacion, t.numero
        FROM `{PROYECTO_COBRANZA}.telefonos_proyecto` tp
        JOIN `{PROYECTO_COBRANZA}.personas` p ON tp.id_persona = p.id_persona
        JOIN `{PROYECTO_COBRANZA}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE tp.id_proyecto = '{proyecto}'
          AND p.identificacion IN ('{"', '".join(cedulas)}')
    """
    df_telefonos_existentes = ejecutar_query(query_telefonos_existentes)
    telefonos_existentes_set = set()
    for _, row in df_telefonos_existentes.iterrows():
        tel = normalizar_telefono(row['numero'])
        if tel:
            telefonos_existentes_set.add((row['identificacion'], tel))
    
    # Obtener correos existentes
    query_correos_existentes = f"""
        SELECT p.identificacion, c.correo
        FROM `{PROYECTO_COBRANZA}.correos_proyecto` cp
        JOIN `{PROYECTO_COBRANZA}.personas` p ON cp.id_persona = p.id_persona
        JOIN `{PROYECTO_COBRANZA}.correos` c ON cp.id_correo = c.id_correo
        WHERE cp.id_proyecto = '{proyecto}'
          AND p.identificacion IN ('{"', '".join(cedulas)}')
    """
    df_correos_existentes = ejecutar_query(query_correos_existentes)
    correos_existentes_set = set()
    for _, row in df_correos_existentes.iterrows():
        corr = normalizar_correo(row['correo'])
        if corr:
            correos_existentes_set.add((row['identificacion'], corr))
    
    # Preparar datos para insertar
    telefonos_nuevos = []
    telefonos_proyecto_nuevos = []
    empresas_actualizadas = []
    
    # Obtener IDs de teléfonos existentes
    telefonos_numeros = [t[1] for t in telefonos_existentes_set]
    if telefonos_numeros:
        query_telefonos_ids = f"""
            SELECT numero, id_telefono
            FROM `{PROYECTO_COBRANZA}.telefonos`
            WHERE numero IN ('{"', '".join(telefonos_numeros)}')
        """
        df_telefonos_ids = ejecutar_query(query_telefonos_ids)
        map_telefono_a_id = dict(zip(df_telefonos_ids['numero'], df_telefonos_ids['id_telefono']))
    else:
        map_telefono_a_id = {}
    
    # Procesar cada registro de CSS
    for _, row in df_css.iterrows():
        cedula = str(row['cedula']).strip()
        if cedula not in map_personas:
            continue
        id_persona = map_personas[cedula]
        nombre = df_personas[df_personas['identificacion'] == cedula]['nombre'].iloc[0] if cedula in df_personas['identificacion'].values else ''
        
        # Teléfono
        telefono_css = normalizar_telefono(row.get('TEL1'))
        if telefono_css and (cedula, telefono_css) not in telefonos_existentes_set:
            if telefono_css not in map_telefono_a_id:
                id_telefono = str(uuid.uuid4())
                map_telefono_a_id[telefono_css] = id_telefono
                telefonos_nuevos.append({'id_telefono': id_telefono, 'numero': telefono_css})
            else:
                id_telefono = map_telefono_a_id[telefono_css]
            telefonos_proyecto_nuevos.append({
                'id_telefono': id_telefono,
                'id_persona': id_persona,
                'id_proyecto': proyecto,
                'fuente': 'INVESTIGACION_CSS',
                'prioridad': 10,
                'estado': 'ACTIVO'
            })
        
        # Empresa
        empresa_css = str(row.get('RAZON_SO', '')).strip()
        if empresa_css and empresa_css != 'nan':
            empresas_actualizadas.append({
                'cedula': cedula,
                'id_persona': id_persona,
                'nombre': nombre,
                'empresa_nueva': empresa_css,
                'patrono': str(row.get('PATRONO', '')).strip(),
                'fecha': str(row.get('FECHA', '')).strip(),
                'salario': row.get('SALARIO')
            })
    
    # Insertar en BigQuery
    registros_insertados = 0
    
    if telefonos_nuevos:
        valores = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
        ejecutar_query(f"""
            INSERT INTO `{PROYECTO_COBRANZA}.telefonos` (id_telefono, numero)
            VALUES {', '.join(valores)}
        """)
        registros_insertados += len(telefonos_nuevos)
    
    if telefonos_proyecto_nuevos:
        valores = []
        for t in telefonos_proyecto_nuevos:
            valores.append(f"""(
                '{t['id_telefono']}',
                '{t['id_persona']}',
                '{t['id_proyecto']}',
                '{t['fuente']}',
                {t['prioridad']},
                '{t['estado']}'
            )""")
        ejecutar_query(f"""
            INSERT INTO `{PROYECTO_COBRANZA}.telefonos_proyecto`
            (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
            VALUES {', '.join(valores)}
        """)
        registros_insertados += len(telefonos_proyecto_nuevos)
    
    if empresas_actualizadas:
        for emp in empresas_actualizadas:
            query_cuenta = f"""
                SELECT id_cuenta
                FROM `{PROYECTO_COBRANZA}.cuentas`
                WHERE id_persona = '{emp['id_persona']}' AND id_proyecto = '{proyecto}'
                ORDER BY created_at DESC LIMIT 1
            """
            df_cuenta = ejecutar_query(query_cuenta)
            if not df_cuenta.empty():
                id_cuenta = df_cuenta['id_cuenta'].iloc[0]
                ejecutar_query(f"""
                    UPDATE `{PROYECTO_COBRANZA}.cuentas`
                    SET empresa = '{emp['empresa_nueva']}', updated_at = CURRENT_TIMESTAMP()
                    WHERE id_cuenta = '{id_cuenta}'
                """)
                registros_insertados += 1
    
    # Generar DataFrames de reporte
    df_telefonos_nuevos = pd.DataFrame(telefonos_proyecto_nuevos)
    if not df_telefonos_nuevos.empty:
        df_telefonos_nuevos = df_telefonos_nuevos.merge(
            df_personas[['id_persona', 'identificacion', 'nombre']],
            on='id_persona', how='left'
        )
        df_telefonos_nuevos = df_telefonos_nuevos[['identificacion', 'nombre', 'id_telefono', 'fuente']]
    
    df_empresas = pd.DataFrame(empresas_actualizadas)
    if not df_empresas.empty:
        df_empresas = df_empresas[['cedula', 'nombre', 'empresa_nueva', 'patrono', 'fecha', 'salario']]
    
    return {
        'total': total,
        'errores': errores,
        'detalle': f'{registros_insertados} registros anexados.',
        'telefonos_nuevos': df_telefonos_nuevos,
        'empresas_actualizadas': df_empresas,
        'df_css': df_css
    }

# ============================================================
# GENERAR EXCEL DE INVESTIGACIÓN
# ============================================================

def generar_excel_investigacion(proyecto):
    """Genera un Excel con todas las hojas de investigación para un proyecto"""
    output = io.BytesIO()
    
    try:
        # Obtener datos del proyecto
        df_contactos, df_empresas = obtener_datos_completos_proyecto(proyecto)
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Hoja 1: RESUMEN
            resumen = pd.DataFrame({
                'Metrica': ['Total contactos', 'Teléfonos', 'Correos', 'Empresas registradas'],
                'Valor': [
                    len(df_contactos),
                    len(df_contactos[df_contactos['tipo_contacto'] == 'TELEFONO']) if not df_contactos.empty else 0,
                    len(df_contactos[df_contactos['tipo_contacto'] == 'CORREO']) if not df_contactos.empty else 0,
                    len(df_empresas) if not df_empresas.empty else 0
                ]
            })
            resumen.to_excel(writer, sheet_name='RESUMEN', index=False)
            
            # Hoja 2: CONTACTOS (teléfonos + correos)
            if not df_contactos.empty:
                df_contactos.to_excel(writer, sheet_name='CONTACTOS', index=False)
            else:
                pd.DataFrame({'Mensaje': ['No hay contactos registrados']}).to_excel(writer, sheet_name='CONTACTOS', index=False)
            
            # Hoja 3: EMPRESAS
            if not df_empresas.empty:
                df_empresas.to_excel(writer, sheet_name='EMPRESAS', index=False)
            else:
                pd.DataFrame({'Mensaje': ['No hay empresas registradas']}).to_excel(writer, sheet_name='EMPRESAS', index=False)
            
            # Ajustar columnas
            for sheet in writer.sheets:
                worksheet = writer.sheets[sheet]
                for i, col in enumerate(pd.read_excel(output, sheet_name=sheet).columns):
                    worksheet.set_column(i, i, 20)
    except Exception as e:
        # Si falla, crear un Excel con el error
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pd.DataFrame({'Error': [str(e)]}).to_excel(writer, sheet_name='Error', index=False)
    
    return output.getvalue()

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
        .divider { margin: 24px 0; border-top: 1px solid #e5e7eb; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">🔍 Investigación y Anexado</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube un archivo con cédulas para consultar fuentes externas (CSS) y anexar los datos encontrados a Cobranza.</div>', unsafe_allow_html=True)
    
    # Obtener proyectos
    proyectos_df = obtener_proyectos_activos()
    if proyectos_df.empty:
        st.warning("⚠️ No hay proyectos activos.")
        return
    
    opciones = {row['nombre']: row['id_proyecto'] for _, row in proyectos_df.iterrows()}
    nombres = list(opciones.keys())
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    proyecto_seleccionado_nombre = st.selectbox("🏢 Proyecto", nombres, index=0)
    proyecto_seleccionado = opciones[proyecto_seleccionado_nombre]
    
    st.markdown('<div class="helper-text" style="font-size:13px;color:#6b6b6b;margin-top:-8px;margin-bottom:16px;">Los datos investigados se anexarán a este proyecto.</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col2:
        # Botón para descargar investigación actual
        excel_bytes = generar_excel_investigacion(proyecto_seleccionado)
        st.download_button(
            label="📥 Descargar Investigación",
            data=excel_bytes,
            file_name=f"INVESTIGACION_{proyecto_seleccionado}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    st.markdown("---")
    st.markdown("### 📤 Subir archivo para investigar")
    
    # Uploader (sin área falsa)
    uploaded_file = st.file_uploader(
        "Sube tu archivo con cédulas",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
        key="investigacion_uploader"
    )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Procesar archivo
    if uploaded_file is not None:
        with st.spinner("📊 Procesando archivo..."):
            try:
                # Leer archivo
                if uploaded_file.name.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(uploaded_file)
                else:
                    contenido = uploaded_file.getvalue().decode('utf-8-sig')
                    from io import StringIO
                    for sep in [',', ';']:
                        try:
                            df = pd.read_csv(StringIO(contenido), sep=sep, dtype=str)
                            if len(df.columns) > 1:
                                break
                        except:
                            continue
                
                # Vista previa
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown('<div class="card-title">📊 Vista previa</div>', unsafe_allow_html=True)
                st.dataframe(df.head(10), use_container_width=True)
                st.markdown(f'**Total:** {len(df)} filas')
                st.markdown('</div>', unsafe_allow_html=True)
                
                if st.button("🚀 Procesar Investigación", type="primary", use_container_width=True):
                    with st.spinner("🔍 Consultando CSS..."):
                        resultado = procesar_investigacion(df, proyecto_seleccionado)
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📊 Cédulas", resultado['total'])
                        with col2:
                            st.metric("🔍 Encontrados CSS", len(resultado['df_css']))
                        with col3:
                            st.metric("📱 Teléfonos anexados", len(resultado['telefonos_nuevos']))
                        with col4:
                            st.metric("🏢 Empresas actualizadas", len(resultado['empresas_actualizadas']))
                        
                        if resultado['errores'] == 0:
                            st.success(f"✅ {resultado['detalle']}")
                        else:
                            st.warning(f"⚠️ {resultado['detalle']}")
                        
                        # Mostrar resultados
                        if not resultado['telefonos_nuevos'].empty:
                            st.markdown("#### 📱 Teléfonos anexados")
                            st.dataframe(resultado['telefonos_nuevos'], use_container_width=True)
                        
                        if not resultado['empresas_actualizadas'].empty:
                            st.markdown("#### 🏢 Empresas actualizadas")
                            st.dataframe(resultado['empresas_actualizadas'], use_container_width=True)
                        
                        # Botón para descargar el Excel actualizado
                        st.markdown("---")
                        if st.button("📥 Descargar Investigación Actualizada", use_container_width=True):
                            excel_bytes_updated = generar_excel_investigacion(proyecto_seleccionado)
                            st.download_button(
                                label="📥 Descargar",
                                data=excel_bytes_updated,
                                file_name=f"INVESTIGACION_{proyecto_seleccionado}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)

# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    render()

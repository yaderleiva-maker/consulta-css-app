import streamlit as st
import pandas as pd
import io
from datetime import datetime

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"

# ============================================================
# FUNCIONES
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    query = f"""
        SELECT id_proyecto, nombre
        FROM `{PROYECTO_BQ}.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    df = ejecutar_query(query)
    return df

def generar_reporte_investigacion(proyecto_id):
    """
    Genera un Excel con todas las investigaciones del proyecto:
    - Personas con sus datos
    - Teléfonos (con origen: CLIENTE o INVESTIGACION)
    - Correos (con origen)
    - Empresas (de cuentas)
    """
    # 1. Personas + Cuentas (para tener la empresa)
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
    
    # 2. Teléfonos (con fuente)
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
        "Teléfonos origen CLIENTE": len(df_telefonos[df_telefonos['origen'] == 'CARGA_INICIAL']) if not df_telefonos.empty else 0,
        "Teléfonos origen INVESTIGACION": len(df_telefonos[df_telefonos['origen'] == 'INVESTIGACION_CSS']) if not df_telefonos.empty else 0,
    }
    df_resumen = pd.DataFrame([resumen])
    
    return {
        'resumen': df_resumen,
        'personas': df_personas,
        'telefonos': df_telefonos,
        'correos': df_correos
    }

def generar_excel_reporte(data):
    """Genera un archivo Excel con múltiples hojas"""
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
# VISTA PRINCIPAL
# ============================================================

def render():
    st.markdown("""
    <style>
        .main-header {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
        }
        .sub-header {
            font-size: 14px;
            color: #6b6b6b;
            margin-bottom: 24px;
        }
        .card {
            background-color: #ffffff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
            border: 1px solid #f0f0f0;
            margin-bottom: 16px;
            text-align: center;
        }
        .card-title {
            font-size: 16px;
            font-weight: 500;
            color: #1a1a1a;
            margin-bottom: 12px;
        }
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
        .btn-primary:hover {
            background-color: #b91c1c;
        }
        .btn-primary:disabled {
            background-color: #9ca3af;
            cursor: not-allowed;
        }
        .metric-card {
            background-color: #f8fafc;
            border-radius: 8px;
            padding: 16px;
            text-align: center;
            border: 1px solid #e5e7eb;
        }
        .metric-value {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
        }
        .metric-label {
            font-size: 13px;
            color: #6b6b6b;
        }
        .helper-text {
            font-size: 13px;
            color: #6b6b6b;
            margin-top: 4px;
        }
        .project-selector {
            margin-bottom: 16px;
        }
        .project-selector label {
            font-weight: 500;
            color: #1a1a1a;
            font-size: 14px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">🔍 Investigación y Anexado</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Consulta y descarga el reporte consolidado de investigación de un proyecto.</div>', unsafe_allow_html=True)
    
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
        help="Selecciona el proyecto del cual quieres descargar la investigación"
    )
    proyecto_seleccionado = opciones_proyectos.get(proyecto_seleccionado_nombre)
    
    st.markdown('<div class="helper-text">Se generará un Excel con: Resumen, Clientes, Teléfonos y Correos.</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ============================================================
    # BOTÓN DE DESCARGA
    # ============================================================
    
    if proyecto_seleccionado:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        
        with st.spinner(f"📊 Generando reporte para {proyecto_seleccionado_nombre}..."):
            try:
                data = generar_reporte_investigacion(proyecto_seleccionado)
                
                # Mostrar métricas rápidas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("👥 Clientes", f"{len(data['personas']):,}")
                with col2:
                    st.metric("📱 Teléfonos", f"{len(data['telefonos']):,}")
                with col3:
                    st.metric("📧 Correos", f"{len(data['correos']):,}")
                with col4:
                    total_inv = len(data['telefonos'][data['telefonos']['origen'] == 'INVESTIGACION_CSS']) if not data['telefonos'].empty else 0
                    st.metric("🔍 Investigados", f"{total_inv:,}")
                
                # Botón de descarga
                excel_bytes = generar_excel_reporte(data)
                st.download_button(
                    label="📥 Descargar Reporte de Investigación",
                    data=excel_bytes,
                    file_name=f"INVESTIGACION_{proyecto_seleccionado}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"❌ Error al generar el reporte: {str(e)}")
                st.exception(e)
        
        st.markdown('</div>', unsafe_allow_html=True)

# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    render()

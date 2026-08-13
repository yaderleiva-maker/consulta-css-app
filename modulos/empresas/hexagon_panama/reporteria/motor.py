import streamlit as st
from datetime import datetime

# Importar los submódulos de cada proyecto
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import carga as jamar_carga
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import carga_gestiones as jamar_gestiones
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import reportes as jamar_reportes

# ============================================================
# REGISTRO DE PROYECTOS
# ============================================================

PROYECTOS = {
    "Jamar": {
        "id": "JAMAR",
        "nombre": "Jamar S.A.",
        "carga": jamar_carga.render,
        "gestiones": jamar_gestiones.render,
        "reportes": jamar_reportes.REPORTES,
        "icono": "📊"
    },
    # Futuros proyectos...
}
    # Futuros proyectos:
    # "IFX": {
    #     "id": "IFX",
    #     "nombre": "IFX Network",
    #     "carga": ifx_carga.render,
    #     "reportes": ifx_reportes.REPORTES,
    #     "icono": "📈"
    # },
}

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para Reportería"""
    
    st.markdown("""
    <style>
        .main-header {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 4px;
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
        }
        .card-title {
            font-size: 16px;
            font-weight: 500;
            color: #1a1a1a;
            margin-bottom: 12px;
        }
        .project-card {
            background-color: #f8fafc;
            border-radius: 8px;
            padding: 16px;
            border: 1px solid #e5e7eb;
            text-align: center;
            cursor: pointer;
            transition: all 0.2s;
        }
        .project-card:hover {
            border-color: #dc2626;
            background-color: #fef2f2;
        }
        .project-card .icon {
            font-size: 32px;
            margin-bottom: 8px;
        }
        .project-card .name {
            font-weight: 500;
            color: #1a1a1a;
        }
        .project-card .id {
            font-size: 12px;
            color: #9ca3af;
        }
        .btn-primary {
            background-color: #dc2626;
            color: white;
            border: none;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            width: 100%;
            transition: background-color 0.2s;
        }
        .btn-primary:hover {
            background-color: #b91c1c;
        }
        .btn-outline {
            background-color: transparent;
            color: #dc2626;
            border: 1px solid #dc2626;
            padding: 8px 20px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn-outline:hover {
            background-color: #fef2f2;
        }
        .helper-text {
            font-size: 13px;
            color: #6b6b6b;
            margin-top: 4px;
        }
        .tab-content {
            margin-top: 16px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">📊 Reportería por Proyecto</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Selecciona un proyecto para cargar su cartera o generar reportes específicos.</div>', unsafe_allow_html=True)
    
    # ============================================================
    # SELECCIÓN DE PROYECTO
    # ============================================================
    
    proyectos_lista = list(PROYECTOS.keys())
    
    proyecto_seleccionado = None
    
    # Mostrar proyectos como tarjetas
    cols = st.columns(min(3, len(proyectos_lista)))
    
    for idx, (nombre, config) in enumerate(PROYECTOS.items()):
        col_idx = idx % 3
        with cols[col_idx]:
            if st.button(
                f"{config['icono']}\n\n{config['nombre']}\n\n`{config['id']}`",
                key=f"proyecto_{config['id']}",
                use_container_width=True,
                help=f"Seleccionar {config['nombre']}"
            ):
                proyecto_seleccionado = nombre
    
    # Si no hay selección, usar el primero
    if proyecto_seleccionado is None and proyectos_lista:
        proyecto_seleccionado = proyectos_lista[0]
    
    if not proyecto_seleccionado:
        st.warning("⚠️ No hay proyectos configurados.")
        return
    
    config = PROYECTOS[proyecto_seleccionado]
    
    # ============================================================
    # CONTENIDO DEL PROYECTO
    # ============================================================
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        st.markdown(f"## {config['icono']}")
    with col2:
        st.markdown(f"## {config['nombre']}")
        st.caption(f"ID: `{config['id']}`")
    with col3:
        if st.button("🔄 Refrescar", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    # ============================================================
    # TABS
    # ============================================================
    
    tab1, tab2 = st.tabs(["📥 Cargar Cartera", "📄 Generar Reportes"])
    
    with tab1:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config['carga']()
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        
        reportes = config['reportes']
        
        if not reportes:
            st.info("ℹ️ No hay reportes configurados para este proyecto.")
        else:
            st.markdown("#### 📋 Reportes disponibles")
            
            for nombre_reporte, funcion in reportes.items():
                with st.container():
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"**{nombre_reporte}**")
                    with col2:
                        if st.button(f"📊 Generar", key=f"gen_{nombre_reporte}"):
                            with st.spinner(f"Generando {nombre_reporte}..."):
                                excel_bytes, mensaje = funcion(config['id'])
                                if excel_bytes:
                                    st.session_state[f"reporte_{nombre_reporte}"] = excel_bytes
                                    st.session_state[f"mensaje_{nombre_reporte}"] = mensaje
                                    st.rerun()
                                else:
                                    st.warning(mensaje)
                    with col3:
                        if st.session_state.get(f"reporte_{nombre_reporte}"):
                            st.download_button(
                                label="📥 Descargar",
                                data=st.session_state[f"reporte_{nombre_reporte}"],
                                file_name=f"{config['id']}_{nombre_reporte.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"download_{nombre_reporte}"
                            )
                            if st.session_state.get(f"mensaje_{nombre_reporte}"):
                                st.success(st.session_state[f"mensaje_{nombre_reporte}"])
                    
                    st.markdown("---")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # ============================================================
    # FOOTER
    # ============================================================
    
    st.markdown("""
    <div style="text-align: center; margin-top: 32px; font-size: 12px; color: #9ca3af; border-top: 1px solid #f0f0f0; padding-top: 16px;">
        Hexagon · Reportería · Versión 1.0
    </div>
    """, unsafe_allow_html=True)

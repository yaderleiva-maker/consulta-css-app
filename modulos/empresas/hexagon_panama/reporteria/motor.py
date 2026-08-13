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
}

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para Reporteria"""
    
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
        .helper-text {
            font-size: 13px;
            color: #6b6b6b;
            margin-top: 4px;
        }
        .tab-content {
            margin-top: 16px;
        }
        .project-selector {
            margin-bottom: 16px;
        }
        .project-selector label {
            font-weight: 500;
            color: #1a1a1a;
            font-size: 14px;
        }
        .status-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }
        .status-badge.success {
            background-color: #dcfce7;
            color: #166534;
        }
        .status-badge.warning {
            background-color: #fef3c7;
            color: #92400e;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">📊 Reporteria por Proyecto</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Selecciona un proyecto para cargar su cartera, gestiones diarias o generar reportes especificos.</div>', unsafe_allow_html=True)
    
    # ============================================================
    # SELECCION DE PROYECTO
    # ============================================================
    
    proyectos_lista = list(PROYECTOS.keys())
    
    if not proyectos_lista:
        st.warning("⚠️ No hay proyectos configurados.")
        return
    
    proyecto_seleccionado = st.selectbox(
        "🏢 Proyecto",
        proyectos_lista,
        key="reporteria_proyecto_selector"
    )
    
    if not proyecto_seleccionado:
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
    # SELECCION DE ACCION (reemplaza los tabs)
    # ============================================================
    
    seccion = st.radio(
        "Accion",
        ["Cargar Cartera", "Cargar Gestiones", "Generar Reportes"],
        horizontal=True,
        key="reporteria_seccion",
    )
    
    st.markdown("---")
    
    if seccion == "Cargar Cartera":
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config["carga"]()
        st.markdown('</div>', unsafe_allow_html=True)
    
    elif seccion == "Cargar Gestiones":
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config["gestiones"]()
        st.markdown('</div>', unsafe_allow_html=True)
    
    else:  # Generar Reportes
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        reportes = config["reportes"]
        
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
                                excel_bytes, mensaje = funcion(config["id"])
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
        Hexagon · Reporteria · Version 1.0
    </div>
    """, unsafe_allow_html=True)


# ============================================================
# PUNTO DE ENTRADA PARA EJECUCION DIRECTA
# ============================================================

if __name__ == "__main__":
    render()

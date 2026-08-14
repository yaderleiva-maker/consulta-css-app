import streamlit as st
from datetime import datetime
import pandas as pd

# Importar los submódulos de cada proyecto
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import carga as jamar_carga
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import carga_gestiones as jamar_gestiones
from modulos.empresas.hexagon_panama.reporteria.proyectos.jamar import pagos as jamar_pagos
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
        "pagos": jamar_pagos.render,
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
    st.markdown('<div class="sub-header">Selecciona un proyecto para cargar su cartera, gestiones diarias, pagos o generar reportes especificos.</div>', unsafe_allow_html=True)
    
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
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📥 Cargar Cartera",
        "📞 Cargar Gestiones",
        "💰 Cargar Pagos",
        "📄 Generar Reportes"
    ])
    
    with tab1:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config["carga"]()
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config["gestiones"]()
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        config["pagos"]()
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        
    # ---- Selector de día único ----
    st.markdown("#### 📅 Seleccionar día para el reporte")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        fecha_reporte = st.date_input(
            "Selecciona el día",
            value=datetime.now().date() - pd.Timedelta(days=1),
            key="fecha_reporte_unica"
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📊 Aplicar filtro", use_container_width=True):
            st.session_state["fecha_filtro_aplicado"] = True
            st.rerun()
    
    # Opción para ver todas las fechas (sin filtro)
    ver_todas = st.checkbox("📅 Ver todas las fechas (sin filtro)", key="ver_todas_fechas")
    
    # Mostrar la fecha seleccionada
    if not ver_todas:
        st.info(f"📌 Reporte para el día: **{fecha_reporte.strftime('%d/%m/%Y')}**")
    else:
        st.info("📌 Reporte sin filtro de fecha (todas las fechas)")
    
    st.markdown("---")
    
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
                            # Pasar fecha única a la función
                            if ver_todas:
                                excel_bytes, mensaje = funcion(config["id"])
                            else:
                                excel_bytes, mensaje = funcion(config["id"], fecha_reporte)
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
                            file_name=f"{config['id']}_{nombre_reporte.replace(' ', '_')}_{fecha_reporte.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{nombre_reporte}"
                        )
                        if st.session_state.get(f"mensaje_{nombre_reporte}"):
                            st.success(st.session_state[f"mensaje_{nombre_reporte}"])
                
                st.markdown("---")
    
    st.markdown('</div>', unsafe_allow_html=True)

"""
Módulo IFX - Dashboard Comercial
Punto de entrada para el menú de NEXO SUITE
Estilo: Microsoft Fluent + Notion + Stripe + SAP Fiori
"""
import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .dashboard import DashboardIFX
from .embudo import EmbudoIFX
from .reportes import ReportesIFX


# ============================================
# ESTILOS CSS PERSONALIZADOS
# ============================================
def inject_custom_css():
    """Inyecta estilos CSS personalizados para lograr la estética deseada"""
    st.markdown("""
    <style>
        /* ============================================
           FUENTES Y TIPOGRAFÍA
           ============================================ */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
        
        html, body, .stApp {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: #f8f9fc;
        }
        
        /* ============================================
           CONTENEDOR PRINCIPAL - RESPETAR BORDES
           ============================================ */
        .main > div {
            padding: 0rem 1.5rem 1.5rem 1.5rem !important;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        /* ============================================
           TARJETAS - FLUENT + NOTION + STRIPE
           ============================================ */
        .card {
            background: #ffffff;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #eef0f2;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.02);
            transition: box-shadow 0.2s ease, transform 0.1s ease;
        }
        
        .card:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.06), 0 8px 24px rgba(0,0,0,0.04);
        }
        
        /* ============================================
           TARJETAS DE KPI - ESTILO STRIPE
           ============================================ */
        .kpi-card {
            background: #ffffff;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            border: 1px solid #eef0f2;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
            transition: all 0.2s ease;
            height: 100%;
        }
        
        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0,0,0,0.06);
        }
        
        .kpi-label {
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.03em;
            color: #6b7280;
            margin-bottom: 0.35rem;
        }
        
        .kpi-value {
            font-size: 1.75rem;
            font-weight: 700;
            color: #111827;
            line-height: 1.2;
        }
        
        .kpi-sub {
            font-size: 0.8rem;
            color: #6b7280;
            margin-top: 0.25rem;
        }
        
        /* ============================================
           TÍTULOS - SAP FIORI + NOTION
           ============================================ */
        h1, h2, h3, h4, h5, h6 {
            font-weight: 600;
            letter-spacing: -0.01em;
            color: #111827;
        }
        
        .section-title {
            font-size: 1.25rem;
            font-weight: 600;
            color: #111827;
            margin-bottom: 0.75rem;
            letter-spacing: -0.01em;
        }
        
        .section-subtitle {
            font-size: 0.875rem;
            color: #6b7280;
            margin-bottom: 1.25rem;
        }
        
        /* ============================================
           ACENTOS ROJOS INSTITUCIONALES
           ============================================ */
        .red-accent {
            color: #dc2626;
        }
        
        .red-border {
            border-left: 3px solid #dc2626;
            padding-left: 0.75rem;
        }
        
        /* ============================================
           TABS - FLUENT DESIGN
           ============================================ */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.25rem;
            background-color: transparent;
            border-bottom: 1px solid #eef0f2;
            padding: 0 0.5rem;
        }
        
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            padding: 0.6rem 1.25rem;
            font-weight: 500;
            font-size: 0.875rem;
            color: #6b7280;
            background: transparent;
            transition: all 0.2s ease;
            border: none;
        }
        
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background-color: transparent;
            color: #dc2626 !important;
            font-weight: 600;
            border-bottom: 2.5px solid #dc2626;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background-color: #f3f4f6;
            color: #111827;
        }
        
        /* ============================================
           BOTONES - FLUENT + STRIPE
           ============================================ */
        .stButton > button {
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.875rem;
            padding: 0.5rem 1.25rem;
            transition: all 0.2s ease;
            border: 1px solid #eef0f2;
            background: #ffffff;
            color: #111827;
        }
        
        .stButton > button:hover {
            background: #f3f4f6;
            border-color: #d1d5db;
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }
        
        .stButton > button[kind="primary"] {
            background: #dc2626;
            border-color: #dc2626;
            color: #ffffff;
        }
        
        .stButton > button[kind="primary"]:hover {
            background: #b91c1c;
            border-color: #b91c1c;
        }
        
        /* ============================================
           SELECT BOXES - FLUENT
           ============================================ */
        .stSelectbox > div > div {
            border-radius: 8px;
            border: 1px solid #eef0f2;
            background: #ffffff;
        }
        
        .stSelectbox > div > div:hover {
            border-color: #d1d5db;
        }
        
        /* ============================================
           DATE INPUTS - FLUENT
           ============================================ */
        .stDateInput > div > div {
            border-radius: 8px;
            border: 1px solid #eef0f2;
            background: #ffffff;
        }
        
        /* ============================================
           DATA FRAME - STRIPE
           ============================================ */
        .stDataFrame {
            border: 1px solid #eef0f2;
            border-radius: 12px;
            overflow: hidden;
        }
        
        .stDataFrame thead th {
            background: #f9fafb;
            font-weight: 600;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.02em;
            color: #6b7280;
            padding: 0.6rem 0.75rem;
        }
        
        .stDataFrame tbody td {
            padding: 0.5rem 0.75rem;
            font-size: 0.875rem;
            color: #111827;
        }
        
        .stDataFrame tbody tr:hover {
            background: #f9fafb;
        }
        
        /* ============================================
           SIDEBAR - SAP FIORI
           ============================================ */
        [data-testid="stSidebar"] {
            background-color: #ffffff;
            border-right: 1px solid #eef0f2;
            padding-top: 1rem;
        }
        
        [data-testid="stSidebar"] .sidebar-content {
            padding: 0 0.75rem;
        }
        
        /* ============================================
           FOOTER
           ============================================ */
        .footer {
            margin-top: 2.5rem;
            padding-top: 1.25rem;
            border-top: 1px solid #eef0f2;
            font-size: 0.75rem;
            color: #9ca3af;
            text-align: center;
        }
        
        /* ============================================
           EMBUDO Y GRÁFICOS
           ============================================ */
        .plotly-container {
            background: #ffffff;
            border-radius: 12px;
            padding: 0.5rem;
            border: 1px solid #eef0f2;
        }
        
        /* ============================================
           SPACING - NOTION
           ============================================ */
        .spacer-8 { height: 0.5rem; }
        .spacer-16 { height: 1rem; }
        .spacer-24 { height: 1.5rem; }
        .spacer-32 { height: 2rem; }
        
        /* ============================================
           METRIC CARDS - HOVER EFFECT
           ============================================ */
        .metric-highlight {
            background: linear-gradient(135deg, #ffffff 0%, #fafafa 100%);
        }
        
        /* ============================================
           RESPONSIVE
           ============================================ */
        @media (max-width: 768px) {
            .kpi-value {
                font-size: 1.25rem;
            }
            .main > div {
                padding: 0 0.75rem !important;
            }
        }
        
        /* ============================================
           SCROLLBAR
           ============================================ */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb {
            background: #d1d5db;
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #9ca3af;
        }
        
        /* ============================================
           TOOLTIP
           ============================================ */
        .stTooltipContent {
            background: #ffffff !important;
            border: 1px solid #eef0f2 !important;
            border-radius: 8px !important;
            box-shadow: 0 4px 16px rgba(0,0,0,0.06) !important;
        }
        
        /* ============================================
           METRIC CONTAINER - STRIPE STYLE
           ============================================ */
        [data-testid="stMetric"] {
            background: #ffffff;
            border-radius: 12px;
            padding: 0.75rem 1rem;
            border: 1px solid #eef0f2;
            box-shadow: 0 1px 3px rgba(0,0,0,0.02);
        }
        
        [data-testid="stMetric"]:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }
        
        [data-testid="stMetric"] label {
            font-weight: 500 !important;
            font-size: 0.75rem !important;
            text-transform: uppercase;
            letter-spacing: 0.02em;
            color: #6b7280 !important;
        }
        
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
            font-weight: 700 !important;
            color: #111827 !important;
        }
    </style>
    """, unsafe_allow_html=True)


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def run(usuario: str):
    """Función principal que ejecuta el módulo IFX"""
    
    # Inyectar estilos personalizados
    inject_custom_css()
    
    # ============================================
    # HEADER
    # ============================================
    st.markdown("""
    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem;">
        <div>
            <h1 style="font-size: 1.5rem; font-weight: 700; margin: 0; display: flex; align-items: center; gap: 0.5rem;">
                <span style="color: #dc2626;">●</span>
                IFX Network
                <span style="font-size: 0.75rem; font-weight: 400; color: #6b7280; margin-left: 0.5rem;">
                    Dashboard Comercial
                </span>
            </h1>
        </div>
        <div style="display: flex; align-items: center; gap: 1rem;">
            <span style="font-size: 0.75rem; color: #6b7280;">
                👤 <span style="font-weight: 500;">{usuario}</span>
            </span>
        </div>
    </div>
    """.format(usuario=usuario), unsafe_allow_html=True)
    
    # ============================================
    # INICIALIZAR DASHBOARD
    # ============================================
    if 'ifx_dashboard' not in st.session_state:
        st.session_state.ifx_dashboard = DashboardIFX()
    
    # ============================================
    # SIDEBAR - CONTROLES
    # ============================================
    with st.sidebar:
        st.markdown("""
        <div style="padding: 0.5rem 0 0.75rem 0;">
            <span style="font-weight: 600; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; color: #6b7280;">
                🎛️ Controles IFX
            </span>
        </div>
        """, unsafe_allow_html=True)
        
        # Botón actualizar
        if st.button("🔄 Actualizar Datos", use_container_width=True, type="primary"):
            with st.spinner("Actualizando tabla en BigQuery..."):
                resultado = st.session_state.ifx_dashboard.actualizar_tabla()
            st.success(resultado)
            st.rerun()
        
        st.markdown("""
        <div style="height: 1rem;"></div>
        <div style="border-top: 1px solid #eef0f2; padding-top: 0.75rem;">
            <span style="font-weight: 500; font-size: 0.8rem; color: #111827;">📅 Filtros</span>
        </div>
        """, unsafe_allow_html=True)
        
        fecha_fin = datetime.now().date()
        fecha_inicio = fecha_fin - timedelta(days=30)
        
        # Filtros en columnas para mejor organización
        col1, col2 = st.columns(2)
        with col1:
            desde = st.date_input("Desde", fecha_inicio, key="ifx_desde", label_visibility="collapsed")
        with col2:
            hasta = st.date_input("Hasta", fecha_fin, key="ifx_hasta", label_visibility="collapsed")
        
        st.markdown('<div style="height: 0.25rem;"></div>', unsafe_allow_html=True)
        
        # Agentes
        agentes = st.session_state.ifx_dashboard.get_agentes()
        agente = st.selectbox("👤 Agente", ["Todos"] + agentes, key="ifx_agente")
        
        # Resultados
        resultados = st.session_state.ifx_dashboard.get_resultados()
        resultado = st.selectbox("📋 Resultado", ["Todos"] + resultados, key="ifx_resultado")
        
        st.markdown('<div style="height: 0.5rem;"></div>', unsafe_allow_html=True)
        
        if st.button("🔍 Aplicar Filtros", use_container_width=True):
            st.rerun()
        
        # Footer del sidebar
        st.markdown("""
        <div style="position: absolute; bottom: 1rem; left: 0; right: 0; padding: 0 1rem;">
            <div style="border-top: 1px solid #eef0f2; padding-top: 0.75rem;">
                <span style="font-size: 0.65rem; color: #9ca3af;">
                    Datos actualizados automáticamente desde AppSheet y BigQuery.
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # ============================================
    # CUERPO PRINCIPAL
    # ============================================
    
    # Preparar filtros
    filtros = {
        'fecha_desde': desde.strftime('%Y-%m-%d'),
        'fecha_hasta': hasta.strftime('%Y-%m-%d'),
        'agente': agente,
        'resultado': resultado
    }
    
    # Cargar datos
    with st.spinner("Cargando datos..."):
        df = st.session_state.ifx_dashboard.get_data(filtros)
    
    if df.empty:
        st.markdown("""
        <div style="text-align: center; padding: 3rem 1rem;">
            <span style="font-size: 2.5rem;">📊</span>
            <h3 style="color: #6b7280; font-weight: 400;">No hay datos para los filtros seleccionados</h3>
            <p style="color: #9ca3af; font-size: 0.875rem;">Ajusta los filtros o actualiza los datos.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # ============================================
    # KPIS - TARJETAS ESTILO STRIPE
    # ============================================
    kpi = st.session_state.ifx_dashboard.get_kpis(df)
    
    # Renderizar KPIs con estilo personalizado
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📞 Gestiones</div>
            <div class="kpi-value">{kpi.total_gestiones}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">✅ Citas</div>
            <div class="kpi-value">{kpi.total_citas}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">🎯 Citas Atendidas</div>
            <div class="kpi-value">{kpi.total_citas_atendidas}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">🔥 Interesados</div>
            <div class="kpi-value">{kpi.total_interesados}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col5:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📈 Conversión</div>
            <div class="kpi-value">{kpi.tasa_conversion:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col6:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">👤 Agentes</div>
            <div class="kpi-value">{kpi.agentes_activos}</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('<div class="spacer-16"></div>', unsafe_allow_html=True)
    
    # ============================================
    # TABS - ESTILO FLUENT DESIGN
    # ============================================
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard",
        "🔄 Embudo Comercial",
        "📥 Reportes",
        "📋 Datos Detallados"
    ])
    
    with tab1:
        # Gráficos en 2 columnas
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<p class="section-title">👤 Gestiones por Agente</p>', unsafe_allow_html=True)
            st.session_state.ifx_dashboard.render_chart_agentes(df)
        
        with col2:
            st.markdown('<p class="section-title">📋 Distribución de Resultados</p>', unsafe_allow_html=True)
            st.session_state.ifx_dashboard.render_chart_resultados(df)
        
        st.markdown('<div class="spacer-16"></div>', unsafe_allow_html=True)
        
        # Evolución diaria
        st.markdown('<p class="section-title">📈 Evolución Diaria</p>', unsafe_allow_html=True)
        st.session_state.ifx_dashboard.render_chart_evolucion(df)
    
    with tab2:
        embudo = EmbudoIFX()
        embudo.render(df)
    
    with tab3:
        reportes = ReportesIFX()
        reportes.render(df)
    
    with tab4:
        # Tabla de datos con estilo
        st.markdown('<p class="section-title">📋 Últimos Registros</p>', unsafe_allow_html=True)
        
        # Selector de columnas a mostrar
        columnas_disponibles = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Flag_Cita', 'Flag_Cita_Atendida']
        columnas_mostrar = st.multiselect(
            "Columnas a mostrar",
            options=df.columns.tolist(),
            default=columnas_disponibles,
            key="ifx_columnas"
        )
        
        if columnas_mostrar:
            st.dataframe(
                df[columnas_mostrar].sort_values('Fecha', ascending=False).head(200),
                use_container_width=True,
                hide_index=True
            )
        
        # Estadísticas rápidas
        st.caption(f"📊 Total de registros: {len(df)} | Columnas: {len(df.columns)}")
    
    # ============================================
    # FOOTER
    # ============================================
    st.markdown("""
    <div class="footer">
        <span>IFX Network · Datos actualizados desde AppSheet y BigQuery</span>
        <br>
        <span style="font-size: 0.65rem;">NEXO SUITE · Versión 1.0</span>
    </div>
    """, unsafe_allow_html=True)

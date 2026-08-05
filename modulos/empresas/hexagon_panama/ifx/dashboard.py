"""
Dashboard IFX - Métricas y KPIs
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from services.bigquery import BigQueryService
from .models import ResumenKPI, EmbudoEtapa


class DashboardIFX:
    """Panel de control para IFX"""
    
    def __init__(self):
        self.bq = BigQueryService()
        self.project = "proyecto-css-panama"
        self.dataset = "ifx"
        self.table = "tabla_dashboard_ifx"
        self.view = "vw_dashboard_ifx"
    
    def get_data(self, filters: Dict[str, Any]) -> pd.DataFrame:
        """Obtiene datos de la tabla materializada"""
        query = f"""
        SELECT * FROM `{self.project}.{self.dataset}.{self.table}`
        WHERE 1=1
        """
        
        if filters.get('fecha_desde'):
            query += f" AND Fecha >= '{filters['fecha_desde']}'"
        if filters.get('fecha_hasta'):
            query += f" AND Fecha <= '{filters['fecha_hasta']}'"
        if filters.get('agente') and filters['agente'] != 'Todos':
            query += f" AND Nombre_Agente = '{filters['agente']}'"
        if filters.get('resultado') and filters['resultado'] != 'Todos':
            query += f" AND Resultado = '{filters['resultado']}'"
        
        return self.bq.execute_query(query)
    
    def get_kpis(self, df: pd.DataFrame) -> ResumenKPI:
        """Calcula los KPIs principales"""
        if df.empty:
            return ResumenKPI()
        
        total_gestiones = len(df)
        total_contactos = df['Flag_Contacto'].sum() if 'Flag_Contacto' in df else 0
        total_citas = df['Flag_Cita'].sum() if 'Flag_Cita' in df else 0
        total_citas_atendidas = df['Flag_Cita_Atendida'].sum() if 'Flag_Cita_Atendida' in df else 0
        total_interesados = df['Flag_Interesado'].sum() if 'Flag_Interesado' in df else 0
        
        tasa_conversion = (total_citas / total_gestiones * 100) if total_gestiones > 0 else 0
        agentes_activos = df['Nombre_Agente'].nunique() if 'Nombre_Agente' in df else 0
        clientes_unicos = df['Cliente'].nunique() if 'Cliente' in df else 0
        
        return ResumenKPI(
            total_gestiones=total_gestiones,
            total_contactos=total_contactos,
            total_citas=total_citas,
            total_citas_atendidas=total_citas_atendidas,
            total_interesados=total_interesados,
            tasa_conversion=tasa_conversion,
            agentes_activos=agentes_activos,
            clientes_unicos=clientes_unicos,
            periodo=f"{df['Fecha'].min()} al {df['Fecha'].max()}" if not df.empty else ""
        )
    
    def get_agentes(self) -> List[str]:
        """Obtiene lista de agentes para filtros"""
        query = f"""
        SELECT DISTINCT Nombre_Agente
        FROM `{self.project}.{self.dataset}.{self.table}`
        WHERE Nombre_Agente IS NOT NULL
        ORDER BY Nombre_Agente
        """
        df = self.bq.execute_query(query)
        return df['Nombre_Agente'].tolist() if not df.empty else []
    
    def get_resultados(self) -> List[str]:
        """Obtiene lista de resultados para filtros"""
        query = f"""
        SELECT DISTINCT Resultado
        FROM `{self.project}.{self.dataset}.{self.table}`
        WHERE Resultado IS NOT NULL
        ORDER BY Resultado
        """
        df = self.bq.execute_query(query)
        return df['Resultado'].tolist() if not df.empty else []
    
    def render_kpi_cards(self, kpi: ResumenKPI):
        """Renderiza tarjetas de KPIs"""
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("📞 Gestiones", kpi.total_gestiones)
        with col2:
            st.metric("✅ Citas", kpi.total_citas)
        with col3:
            st.metric("🎯 Citas Atendidas", kpi.total_citas_atendidas)
        with col4:
            st.metric("🔥 Interesados", kpi.total_interesados)
        with col5:
            st.metric("📈 Conversión", f"{kpi.tasa_conversion:.1f}%")
    
    def render_chart_agentes(self, df: pd.DataFrame):
        """Renderiza gráfico de agentes"""
        if df.empty:
            st.info("No hay datos para mostrar")
            return
        
        agentes_group = df.groupby('Nombre_Agente').agg({
            'Gestion': 'count',
            'Flag_Cita': 'sum',
            'Flag_Cita_Atendida': 'sum'
        }).reset_index()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                agentes_group,
                x='Nombre_Agente',
                y='Gestion',
                title='Gestiones por Agente',
                text='Gestion',
                color='Nombre_Agente',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_traces(textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig_citas = px.bar(
                agentes_group,
                x='Nombre_Agente',
                y=['Flag_Cita', 'Flag_Cita_Atendida'],
                title='Citas Programadas vs Atendidas',
                barmode='group',
                color_discrete_map={'Flag_Cita': '#ff7f0e', 'Flag_Cita_Atendida': '#2ca02c'},
                labels={'value': 'Cantidad', 'variable': 'Tipo'}
            )
            st.plotly_chart(fig_citas, use_container_width=True)
    
    def render_chart_evolucion(self, df: pd.DataFrame):
        """Renderiza gráfico de evolución diaria"""
        if df.empty:
            return
        
        df_daily = df.groupby('Fecha').size().reset_index(name='Gestiones')
        fig = px.line(
            df_daily,
            x='Fecha',
            y='Gestiones',
            title='Evolución Diaria de Gestiones',
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    def render(self, df: pd.DataFrame, kpi: ResumenKPI):
        """Renderiza el dashboard completo"""
        self.render_kpi_cards(kpi)
        st.divider()
        
        tab1, tab2, tab3 = st.tabs(["📊 Agentes", "📈 Evolución", "📋 Detalle"])
        
        with tab1:
            self.render_chart_agentes(df)
        
        with tab2:
            self.render_chart_evolucion(df)
        
        with tab3:
            st.dataframe(
                df[['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Flag_Cita']].head(100),
                use_container_width=True,
                hide_index=True
            )

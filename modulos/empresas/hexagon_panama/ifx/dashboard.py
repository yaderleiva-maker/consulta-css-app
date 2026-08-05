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
    
    def actualizar_tabla(self) -> str:
        """Ejecuta la actualización de la tabla materializada"""
        query = f"""
        CREATE OR REPLACE TABLE `{self.project}.{self.dataset}.{self.table}` AS
        SELECT * FROM `{self.project}.{self.dataset}.{self.view}`
        """
        self.bq.execute_query(query)
        return "✅ Datos actualizados correctamente"
    
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
        
        fig = px.bar(
            agentes_group,
            x='Nombre_Agente',
            y='Gestion',
            title='',
            text='Gestion',
            color='Nombre_Agente',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(family="Inter, sans-serif"),
            showlegend=False,
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    def render_chart_resultados(self, df: pd.DataFrame):
        """Renderiza gráfico de distribución de resultados"""
        if df.empty:
            return
        
        resultados_group = df.groupby('Resultado').size().reset_index(name='count')
        resultados_group = resultados_group.sort_values('count', ascending=False).head(10)
        
        fig = px.pie(
            resultados_group,
            values='count',
            names='Resultado',
            title='',
            hole=0.3,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(family="Inter, sans-serif"),
            height=300,
            showlegend=True,
            legend=dict(orientation='v', yanchor='top', y=0.95, xanchor='left', x=0.95)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    def render_chart_evolucion(self, df: pd.DataFrame):
        """Renderiza gráfico de evolución diaria"""
        if df.empty:
            return
        
        df_daily = df.groupby('Fecha').size().reset_index(name='Gestiones')
        
        fig = px.line(
            df_daily,
            x='Fecha',
            y='Gestiones',
            title='',
            markers=True
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(family="Inter, sans-serif"),
            height=300,
            showlegend=False
        )
        fig.update_traces(line=dict(color='#dc2626', width=2.5))
        fig.update_xaxes(gridcolor='#f3f4f6', showgrid=True)
        fig.update_yaxes(gridcolor='#f3f4f6', showgrid=True)
        st.plotly_chart(fig, use_container_width=True)
    
    def render_kpi_cards(self, kpi: ResumenKPI):
        """Renderiza tarjetas de KPIs - VERSIÓN CON ESTILOS MANUALES (ya no se usa porque main.py maneja los KPIs)"""
        pass

"""
Embudo Comercial IFX
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import List, Dict, Any

from .models import EmbudoEtapa


class EmbudoIFX:
    """Embudo comercial de IFX"""
    
    ETAPAS = [
        ("Cita Atendida", 1),
        ("Cita Programada", 2),
        ("Interesado", 3),
        ("Cita Cancelada", 4),
        ("Seguimiento", 5),
        ("Contactado", 6),
        ("No Interesado", 7),
        ("No Aplica", 8),
    ]
    
    def get_embudo_data(self, df: pd.DataFrame) -> List[EmbudoEtapa]:
        """Calcula datos del embudo basado en Resultado_Embudo"""
        if df.empty:
            return []
        
        total_clientes = df['Cliente'].nunique()
        embudo_data = []
        
        for etapa, prioridad in self.ETAPAS:
            clientes = df[df['Resultado_Embudo'] == etapa]['Cliente'].nunique()
            porcentaje = (clientes / total_clientes * 100) if total_clientes > 0 else 0
            embudo_data.append(EmbudoEtapa(
                etapa=etapa,
                prioridad=prioridad,
                clientes=clientes,
                porcentaje=porcentaje
            ))
        
        return embudo_data
    
    def render_chart(self, embudo_data: List[EmbudoEtapa]):
        """Renderiza el gráfico de embudo"""
        if not embudo_data:
            st.info("No hay datos para el embudo")
            return
        
        # Filtrar etapas con clientes > 0
        data = [e for e in embudo_data if e.clientes > 0]
        
        if not data:
            st.info("No hay clientes en el embudo")
            return
        
        fig = go.Figure(go.Funnel(
            name='Embudo IFX',
            y=[e.etapa for e in data],
            x=[e.clientes for e in data],
            textinfo="value+percent initial",
            textposition="inside",
            marker={"color": ["#2ca02c", "#ff7f0e", "#1f77b4", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]}
        ))
        
        fig.update_layout(
            title="Embudo Comercial - Clientes únicos por etapa",
            width=800,
            height=500,
            funnelmode="stack"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render(self, df: pd.DataFrame):
        """Renderiza el embudo completo"""
        st.subheader("🔄 Embudo Comercial")
        
        # Filtro de contactabilidad
        df_contactos = df[df['Contactabilidad'] == 1] if 'Contactabilidad' in df else df
        
        embudo_data = self.get_embudo_data(df_contactos)
        self.render_chart(embudo_data)
        
        # Tabla de detalle
        if embudo_data:
            st.divider()
            st.caption("Detalle del embudo")
            df_embudo = pd.DataFrame([e.to_dict() for e in embudo_data])
            st.dataframe(df_embudo, use_container_width=True, hide_index=True)

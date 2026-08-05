"""
Generación de Reportes IFX
"""
import streamlit as st
import pandas as pd
import io
from datetime import datetime


class ReportesIFX:
    """Generación de reportes para IFX - SIN depender de archivos.py"""
    
    def generar_excel(self, df: pd.DataFrame) -> bytes:
        """Genera archivo Excel con los datos (en memoria, sin Drive)"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Hoja principal: Dashboard
            df.to_excel(writer, index=False, sheet_name='Dashboard')
            
            # Hoja de métricas
            if not df.empty:
                metrics_data = {
                    'Métrica': [
                        'Total Gestiones',
                        'Total Citas',
                        'Citas Atendidas',
                        'Interesados',
                        'Clientes Únicos',
                        'Agentes Activos',
                        'Tasa de Conversión'
                    ],
                    'Valor': [
                        len(df),
                        df['Flag_Cita'].sum() if 'Flag_Cita' in df else 0,
                        df['Flag_Cita_Atendida'].sum() if 'Flag_Cita_Atendida' in df else 0,
                        df['Flag_Interesado'].sum() if 'Flag_Interesado' in df else 0,
                        df['Cliente'].nunique() if 'Cliente' in df else 0,
                        df['Nombre_Agente'].nunique() if 'Nombre_Agente' in df else 0,
                        f"{(df['Flag_Cita'].sum() / len(df) * 100):.1f}%" if len(df) > 0 else "0%"
                    ]
                }
                metrics = pd.DataFrame(metrics_data)
                metrics.to_excel(writer, index=False, sheet_name='Métricas')
                
                # Hoja de resultados por agente
                if 'Nombre_Agente' in df:
                    agentes = df.groupby('Nombre_Agente').agg({
                        'Gestion': 'count',
                        'Flag_Cita': 'sum',
                        'Flag_Cita_Atendida': 'sum',
                        'Flag_Interesado': 'sum'
                    }).reset_index()
                    agentes.columns = ['Agente', 'Gestiones', 'Citas', 'Citas Atendidas', 'Interesados']
                    agentes['Conversión'] = (agentes['Citas'] / agentes['Gestiones'] * 100).round(1).astype(str) + '%'
                    agentes.to_excel(writer, index=False, sheet_name='Agentes')
                
                # Hoja de resultados por resultado
                if 'Resultado' in df:
                    resultados = df.groupby('Resultado').size().reset_index(name='Cantidad')
                    resultados = resultados.sort_values('Cantidad', ascending=False)
                    resultados.to_excel(writer, index=False, sheet_name='Resultados')
        
        return output.getvalue()
    
    def render(self, df: pd.DataFrame):
        """Renderiza la sección de reportes"""
        st.markdown("""
        <div style="margin-bottom: 1rem;">
            <span style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; color: #6b7280;">
                📥 Reportes
            </span>
        </div>
        """, unsafe_allow_html=True)
        
        if df.empty:
            st.warning("No hay datos para generar reportes")
            return
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("""
            <div style="background: #ffffff; border-radius: 12px; padding: 1.25rem; border: 1px solid #eef0f2;">
                <p style="font-weight: 500; margin-bottom: 0.5rem;">📊 Reporte Ejecutivo</p>
                <p style="font-size: 0.8rem; color: #6b7280;">Descarga el reporte completo con todas las métricas y detalles.</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("📥 Generar Reporte Excel", use_container_width=True):
                with st.spinner("Generando archivo..."):
                    excel_data = self.generar_excel(df)
                    st.download_button(
                        label="✅ Descargar Excel",
                        data=excel_data,
                        file_name=f"IFX_Reporte_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="ifx_download_excel"
                    )
        
        with col2:
            st.markdown("""
            <div style="background: #ffffff; border-radius: 12px; padding: 1.25rem; border: 1px solid #eef0f2;">
                <p style="font-weight: 500; margin-bottom: 0.5rem;">📋 Resumen Rápido</p>
                <div style="font-size: 0.875rem; color: #111827; line-height: 1.8;">
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.25rem 1rem;">
                <span style="color: #6b7280;">📊 Registros</span>
                <span style="font-weight: 600;">{len(df)}</span>
                <span style="color: #6b7280;">📅 Citas</span>
                <span style="font-weight: 600;">{df['Flag_Cita'].sum() if 'Flag_Cita' in df else 0}</span>
                <span style="color: #6b7280;">✅ Citas Atendidas</span>
                <span style="font-weight: 600;">{df['Flag_Cita_Atendida'].sum() if 'Flag_Cita_Atendida' in df else 0}</span>
                <span style="color: #6b7280;">👤 Agentes</span>
                <span style="font-weight: 600;">{df['Nombre_Agente'].nunique() if 'Nombre_Agente' in df else 0}</span>
                <span style="color: #6b7280;">🏢 Clientes</span>
                <span style="font-weight: 600;">{df['Cliente'].nunique() if 'Cliente' in df else 0}</span>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.divider()
        
        # Vista previa de los datos
        with st.expander("📋 Vista previa de los datos a descargar", expanded=False):
            st.caption(f"Mostrando los primeros 20 registros de {len(df)}")
            
            # Seleccionar columnas para mostrar
            columnas_mostrar = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Flag_Cita', 'Flag_Cita_Atendida']
            columnas_disponibles = [col for col in columnas_mostrar if col in df.columns]
            
            st.dataframe(
                df[columnas_disponibles].sort_values('Fecha', ascending=False).head(20),
                use_container_width=True,
                hide_index=True
            )

"""
Generación de Reportes IFX
"""
import streamlit as st
import pandas as pd
import io
from datetime import datetime

from services.archivos import ArchivosService


class ReportesIFX:
    """Generación de reportes para IFX"""
    
    def generar_excel(self, df: pd.DataFrame, nombre: str = "reporte_ifx") -> bytes:
        """Genera archivo Excel con los datos"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Hoja principal
            df.to_excel(writer, index=False, sheet_name='Dashboard')
            
            # Hoja de métricas
            if not df.empty:
                metrics = pd.DataFrame({
                    'Métrica': ['Total Gestiones', 'Total Citas', 'Citas Atendidas', 'Interesados'],
                    'Valor': [
                        len(df),
                        df['Flag_Cita'].sum(),
                        df['Flag_Cita_Atendida'].sum(),
                        df['Flag_Interesado'].sum()
                    ]
                })
                metrics.to_excel(writer, index=False, sheet_name='Métricas')
        
        return output.getvalue()
    
    def render_download_button(self, df: pd.DataFrame):
        """Renderiza botón de descarga"""
        if df.empty:
            st.warning("No hay datos para descargar")
            return
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("📥 Descargar Reporte (Excel)", use_container_width=True):
                with st.spinner("Generando archivo..."):
                    excel_data = self.generar_excel(df)
                    st.download_button(
                        label="✅ Descargar",
                        data=excel_data,
                        file_name=f"IFX_Reporte_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

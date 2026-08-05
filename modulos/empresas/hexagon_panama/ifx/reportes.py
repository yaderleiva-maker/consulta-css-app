"""
Generación de Reportes IFX
"""
import streamlit as st
import pandas as pd
import io
from datetime import datetime
from typing import Dict, List, Optional


class ReportesIFX:
    """Generación de reportes para IFX"""
    
    # Configuración de reportes
    REPORTES_CONFIG = {
        "📅 Citas Programadas": {
            "descripcion": "Todas las citas agendadas con fecha, cliente, agente y estado",
            "columnas": ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Proximo_Contacto', 'Notas'],
            "filtro": "Flag_Cita = 1"
        },
        "✅ Citas Atendidas": {
            "descripcion": "Citas que se concretaron exitosamente",
            "columnas": ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Notas'],
            "filtro": "Flag_Cita_Atendida = 1"
        },
        "🔄 Volver a Llamar": {
            "descripcion": "Clientes que requieren seguimiento prioritario",
            "columnas": ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Proximo_Contacto', 'Notas'],
            "filtro": "Resultado = 'VOLVER A LLAMAR'"
        },
        "🔥 Interesados": {
            "descripcion": "Clientes con interés activo (oportunidades)",
            "columnas": ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Notas'],
            "filtro": "Flag_Interesado = 1"
        },
        "📊 Resumen por Agente": {
            "descripcion": "KPIs y métricas por cada asesor",
            "columnas": ['Nombre_Agente', 'Gestiones', 'Citas', 'Citas_Atendidas', 'Interesados', 'Conversion'],
            "tipo": "agregado"
        },
        "🏢 Clientes Únicos": {
            "descripcion": "Base de clientes con último y mejor resultado",
            "columnas": ['Nombre_Original', 'Nombre_Agente', 'Ultimo_Resultado', 'Mejor_Resultado_Historico', 'Total_Gestiones'],
            "tipo": "clientes"
        },
        "📈 Embudo Comercial": {
            "descripcion": "Clientes por etapa del embudo comercial",
            "columnas": ['Etapa', 'Clientes', 'Porcentaje'],
            "tipo": "embudo"
        },
        "📋 Todas las Gestiones": {
            "descripcion": "Base completa de todas las gestiones",
            "columnas": ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Flag_Cita', 'Flag_Cita_Atendida'],
            "filtro": "1=1"
        }
    }
    
    def generar_reporte(self, df: pd.DataFrame, tipo_reporte: str, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Genera un reporte específico según el tipo"""
        
        if tipo_reporte == "📅 Citas Programadas":
            return self._reporte_citas_programadas(df, filtros)
        elif tipo_reporte == "✅ Citas Atendidas":
            return self._reporte_citas_atendidas(df, filtros)
        elif tipo_reporte == "🔄 Volver a Llamar":
            return self._reporte_volver_llamar(df, filtros)
        elif tipo_reporte == "🔥 Interesados":
            return self._reporte_interesados(df, filtros)
        elif tipo_reporte == "📊 Resumen por Agente":
            return self._reporte_resumen_agente(df, filtros)
        elif tipo_reporte == "🏢 Clientes Únicos":
            return self._reporte_clientes_unicos(df, filtros)
        elif tipo_reporte == "📈 Embudo Comercial":
            return self._reporte_embudo(df, filtros)
        else:  # "📋 Todas las Gestiones"
            return self._reporte_todas_gestiones(df, filtros)
    
    def _reporte_citas_programadas(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte de citas programadas"""
        df_filtrado = df[df['Flag_Cita'] == 1].copy()
        columnas = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Proximo_Contacto', 'Notas']
        columnas_existentes = [col for col in columnas if col in df_filtrado.columns]
        return df_filtrado[columnas_existentes].sort_values('Fecha', ascending=False)
    
    def _reporte_citas_atendidas(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte de citas atendidas"""
        df_filtrado = df[df['Flag_Cita_Atendida'] == 1].copy()
        columnas = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Notas']
        columnas_existentes = [col for col in columnas if col in df_filtrado.columns]
        return df_filtrado[columnas_existentes].sort_values('Fecha', ascending=False)
    
    def _reporte_volver_llamar(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte de clientes que requieren volver a llamar"""
        df_filtrado = df[df['Resultado'] == 'VOLVER A LLAMAR'].copy()
        columnas = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Proximo_Contacto', 'Notas']
        columnas_existentes = [col for col in columnas if col in df_filtrado.columns]
        return df_filtrado[columnas_existentes].sort_values('Proximo_Contacto', ascending=True)
    
    def _reporte_interesados(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte de clientes interesados"""
        df_filtrado = df[df['Flag_Interesado'] == 1].copy()
        columnas = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Notas']
        columnas_existentes = [col for col in columnas if col in df_filtrado.columns]
        return df_filtrado[columnas_existentes].sort_values('Fecha', ascending=False)
    
    def _reporte_resumen_agente(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Resumen de KPIs por agente"""
        if df.empty or 'Nombre_Agente' not in df:
            return pd.DataFrame()
        
        agentes = df.groupby('Nombre_Agente').agg({
            'Gestion': 'count',
            'Flag_Cita': 'sum',
            'Flag_Cita_Atendida': 'sum',
            'Flag_Interesado': 'sum',
            'Flag_Contacto': 'sum'
        }).reset_index()
        
        agentes.columns = ['Agente', 'Gestiones', 'Citas', 'Citas_Atendidas', 'Interesados', 'Contactos']
        agentes['Conversion'] = (agentes['Citas'] / agentes['Gestiones'] * 100).round(1)
        agentes['Tasa_Contacto'] = (agentes['Contactos'] / agentes['Gestiones'] * 100).round(1)
        
        # Ordenar por mejor conversión
        return agentes.sort_values('Conversion', ascending=False)
    
    def _reporte_clientes_unicos(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte de clientes únicos con su información"""
        if df.empty or 'Cliente' not in df:
            return pd.DataFrame()
        
        # Obtener último resultado y mejor resultado histórico
        clientes = df.groupby('Cliente').agg({
            'Nombre_Original': 'first',
            'Nombre_Agente': lambda x: x.iloc[-1] if len(x) > 0 else None,
            'Resultado': lambda x: x.iloc[-1] if len(x) > 0 else None,
            'Mejor_Resultado_Historico': 'first',
            'Gestion': 'count',
            'Flag_Cita': 'sum',
            'Flag_Cita_Atendida': 'sum',
            'Flag_Interesado': 'sum'
        }).reset_index()
        
        clientes.columns = ['Cliente', 'Nombre', 'Ultimo_Agente', 'Ultimo_Resultado', 
                           'Mejor_Resultado', 'Total_Gestiones', 'Citas', 'Citas_Atendidas', 'Interesados']
        
        return clientes.sort_values('Total_Gestiones', ascending=False)
    
    def _reporte_embudo(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte del embudo comercial"""
        if df.empty or 'Resultado_Embudo' not in df or 'Contactabilidad' not in df:
            return pd.DataFrame()
        
        # Filtrar solo contactos
        df_contactos = df[df['Contactabilidad'] == 1]
        
        # Prioridad de etapas
        prioridad = {
            'Cita Atendida': 1,
            'Cita Programada': 2,
            'Interesado': 3,
            'Cita Cancelada': 4,
            'Seguimiento': 5,
            'Contactado': 6,
            'No Interesado': 7,
            'No Aplica': 8
        }
        
        embudo = df_contactos.groupby('Resultado_Embudo').agg({
            'Cliente': 'nunique'
        }).reset_index()
        
        embudo.columns = ['Etapa', 'Clientes']
        embudo['Prioridad'] = embudo['Etapa'].map(prioridad).fillna(99)
        embudo = embudo.sort_values('Prioridad')
        
        total = embudo['Clientes'].sum()
        embudo['Porcentaje'] = (embudo['Clientes'] / total * 100).round(1) if total > 0 else 0
        
        return embudo[['Etapa', 'Clientes', 'Porcentaje']]
    
    def _reporte_todas_gestiones(self, df: pd.DataFrame, filtros: Optional[Dict] = None) -> pd.DataFrame:
        """Reporte completo de todas las gestiones"""
        columnas = ['Fecha', 'Nombre_Original', 'Nombre_Agente', 'Resultado', 'Flag_Cita', 'Flag_Cita_Atendida', 'Notas']
        columnas_existentes = [col for col in columnas if col in df.columns]
        return df[columnas_existentes].sort_values('Fecha', ascending=False)
    
    def generar_excel_multiple(self, reportes: Dict[str, pd.DataFrame]) -> bytes:
        """Genera un archivo Excel con múltiples hojas"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for nombre, df in reportes.items():
                if not df.empty:
                    # Limitar nombre de hoja a 31 caracteres (Excel)
                    sheet_name = nombre[:31]
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
        return output.getvalue()
    
def render(self, df: pd.DataFrame):
    """Renderiza la sección de reportes"""
    
    # Título
    st.markdown("### 📥 Reportes IFX")
    
    # Botón principal de descarga
    col1, col2 = st.columns([2, 1])
    with col2:
        if st.button("📥 Descargar Excel", use_container_width=True, type="primary"):
            self._generar_descarga(df)
    
    st.divider()
    
    # ============================================
    # SELECCIÓN DE REPORTES - ¡AQUÍ ESTÁ LA LISTA!
    # ============================================
    st.markdown("### 📋 Selecciona los reportes a generar")
    
    col1, col2 = st.columns(2)
    
    with col1:
        reportes_seleccionados = []
        for i, (nombre, config) in enumerate(list(self.REPORTES_CONFIG.items())[:4]):
            if st.checkbox(f"{nombre}", key=f"reporte_{i}", value=True):
                reportes_seleccionados.append(nombre)
    
    with col2:
        for i, (nombre, config) in enumerate(list(self.REPORTES_CONFIG.items())[4:]):
            if st.checkbox(f"{nombre}", key=f"reporte_{i+4}", value=True):
                reportes_seleccionados.append(nombre)
    
    # Vista previa del primer reporte seleccionado
    if reportes_seleccionados:
        st.divider()
        st.markdown(f"**📋 Vista previa: {reportes_seleccionados[0]}**")
        
        df_reporte = self.generar_reporte(df, reportes_seleccionados[0])
        st.dataframe(
            df_reporte.head(20),
            use_container_width=True,
            hide_index=True
        )
    
    def _generar_descarga(self, df: pd.DataFrame):
        """Genera y descarga los reportes seleccionados"""
        # Obtener reportes seleccionados
        reportes_seleccionados = []
        for i, nombre in enumerate(self.REPORTES_CONFIG.keys()):
            if st.session_state.get(f"reporte_{i}", False) or st.session_state.get(f"reporte_{i+4}", False):
                reportes_seleccionados.append(nombre)
        
        if not reportes_seleccionados:
            st.warning("Selecciona al menos un reporte")
            return
        
        with st.spinner("Generando reportes..."):
            reportes_data = {}
            for nombre in reportes_seleccionados:
                df_reporte = self.generar_reporte(df, nombre)
                if not df_reporte.empty:
                    reportes_data[nombre] = df_reporte
            
            if reportes_data:
                excel_data = self.generar_excel_multiple(reportes_data)
                st.download_button(
                    label="✅ Descargar Excel",
                    data=excel_data,
                    file_name=f"IFX_Reportes_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="ifx_download_excel_final"
                )
                st.success(f"✅ {len(reportes_data)} reportes generados correctamente")
            else:
                st.warning("No se pudo generar ningún reporte con los datos disponibles")

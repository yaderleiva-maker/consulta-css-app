# cobranza/reporte_clientes/reporte_jamar.py
import streamlit as st
import pandas as pd
import yaml
from services.bigquery import ejecutar_query

def cargar_config_jamar():
    with open("herramientas/proyectos/jamar.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def render_ui():
    st.title("📋 Reporte Operativo - Jamar")
    st.caption("Generación del reporte entregable con jerarquía de Predemanda")

    config = cargar_config_jamar()
    
    # Filtros de fecha
    col1, col2 = st.columns(2)
    fecha_inicio = col1.date_input("Fecha Inicio", value=pd.to_datetime("today"))
    fecha_fin = col2.date_input("Fecha Fin", value=pd.to_datetime("today"))

    if st.button("🔍 Generar Reporte Predemanda"):
        query = f"""
            SELECT 
                cuenta,
                codigo_jamar,
                gestion AS mejor_gestion_jamar,
                contactabilidad AS resultado,
                prioridad,
                fecha_gestion AS created_at
            FROM `proyecto-css-panama.cobranza.gestiones`
            WHERE DATE(fecha_gestion) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            ORDER BY prioridad ASC, fecha_gestion DESC
        """
        
        with st.spinner("Consultando BigQuery..."):
            df_reporte = ejecutar_query(query)

        if df_reporte.empty:
            st.warning("No se encontraron gestiones para el rango de fechas seleccionado.")
        else:
            st.success(f"Se encontraron {len(df_reporte)} registros.")
            st.dataframe(df_reporte, use_container_width=True)

            # Exportador en Excel listo para cliente
            csv = df_reporte.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar Reporte Predemanda (CSV)",
                data=csv,
                file_name=f"Reporte_Predemanda_Jamar_{fecha_inicio}_{fecha_fin}.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    render_ui()

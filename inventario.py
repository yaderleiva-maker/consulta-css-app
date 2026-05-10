import streamlit as st

def run(usuario):
    st.title("📦 NEXO STOCK - PRUEBA")
    st.write(f"Usuario: {usuario}")
    st.success("✅ Si ves esto, el import funciona correctamente")
    
    # Verificar dependencias
    try:
        import google.cloud.bigquery
        st.info("✅ google-cloud-bigquery está instalado")
    except ImportError as e:
        st.error(f"❌ google-cloud-bigquery NO está instalado: {e}")
    
    try:
        import pandas as pd
        st.info("✅ pandas está instalado")
    except ImportError as e:
        st.error(f"❌ pandas NO está instalado: {e}")

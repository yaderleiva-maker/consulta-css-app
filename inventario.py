import streamlit as st

def run(usuario):
    st.title("📦 NEXO STOCK")
    st.write(f"Usuario logueado: {usuario}")
    st.success("✅ Módulo de inventario funcionando correctamente")
    
    # Prueba de conexión a secrets
    try:
        if "BIGQUERY_CREDENTIALS" in st.secrets:
            st.info("🔐 Secrets de BigQuery encontrados")
        else:
            st.warning("⚠️ No se encontraron secrets de BigQuery")
    except:
        st.error("❌ No se pueden leer secrets")

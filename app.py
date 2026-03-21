import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# -----------------------
# LOGIN SIMPLE
# -----------------------

usuarios = {
    "yader@gmail.com": "1234",
    "supervisor@gmail.com": "abcd"
}

if "login_ok" not in st.session_state:
    st.session_state.login_ok = False

# Botón de cerrar sesión (solo si está logueado)
if st.session_state.login_ok:
    if st.button("Cerrar sesión"):
        st.session_state.login_ok = False
        st.rerun()

if not st.session_state.login_ok:
    st.title("🔐 Acceso a Consulta CSS")

    usuario = st.text_input("Correo")
    password = st.text_input("Contraseña", type="password")

    if st.button("Ingresar"):
        if usuario in usuarios and usuarios[usuario] == password:
            st.session_state.login_ok = True
            st.success("Acceso concedido ✅")
            st.rerun()
        else:
            st.error("Credenciales incorrectas ❌")

    st.stop()

# -----------------------
# APP PRINCIPAL
# -----------------------

st.title("Consulta CSS Panamá 🔍")

uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])

if uploaded_file:

    # Leer archivo (detecta separador automáticamente)
    df = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig')

    # Limpiar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()

    # Limpiar columna cedula
    if 'cedula' in df.columns:
        df['cedula'] = df['cedula'].astype(str).str.strip()

    st.write("Archivo cargado:", df.head())

    if st.checkbox("Ver archivo completo"):
        st.dataframe(df)
    else:
        st.dataframe(df.head())

    # -----------------------
    # CONEXIÓN BIGQUERY
    # -----------------------

    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )

    except Exception as e:
        st.error(f"❌ Error de conexión a BigQuery: {e}")
        st.stop()

    # -----------------------
    # SUBIR DATA
    # -----------------------

    table_id = "proyecto-css-panama.consultas.temp_clientes"

    with st.spinner("Subiendo datos a BigQuery..."):
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )
        )
        job.result()

    st.success("✅ Datos subidos correctamente")

    # -----------------------
    # QUERY (CORREGIDO)
    # -----------------------

    query = """
    SELECT 
      a.cedula,
      b.NOMBRE,
      b.PATRONO,
      b.RAZON_SO,
      b.TEL1,
      b.FECHA,
      b.SALARIO
    FROM `proyecto-css-panama.consultas.temp_clientes` a
    LEFT JOIN `proyecto-css-panama.css_data.css-actual` b
    ON a.cedula = b.cedula
    """

    with st.spinner("Ejecutando consulta..."):
        result = client.query(query).to_dataframe()

    from datetime import datetime

historial = pd.DataFrame([{
    "usuario": usuario,
    "fecha": datetime.now(),
    "cantidad_registros": len(result)
}])

client.load_table_from_dataframe(
    historial,
    "proyecto-css-panama.consultas.historial_consultas",
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
).result()

st.success("✅ Consulta lista 🎉")

    # -----------------------
    # DESCARGA
    # -----------------------

    st.download_button(
        "Descargar resultado",
        result.to_csv(index=False),
        file_name="resultado.csv",
        mime="text/csv"
    )

    # Preview
    st.write("Vista previa de resultados:")
    st.dataframe(result.head(10))

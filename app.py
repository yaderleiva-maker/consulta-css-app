import streamlit as st
import pandas as pd
from google.cloud import bigquery

if st.button("Cerrar sesión"):
    st.session_state.login_ok = False
    st.rerun()

# -----------------------
# LOGIN SIMPLE
# -----------------------

usuarios = {
    "yader@gmail.com": "1234",
    "supervisor@gmail.com": "abcd"
}

if "login_ok" not in st.session_state:
    st.session_state.login_ok = False

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

    # Leer archivo con detección automática de separador
    df = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig')

    # Limpiar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()

    # Limpiar columna cedula (quitar espacios)
    if 'cedula' in df.columns:
        df['cedula'] = df['cedula'].astype(str).str.strip()

    st.write("Archivo cargado:", df.head())

    # Conectar a BigQuery
    client = bigquery.Client()

    table_id = "proyecto-css-panama.consultas.temp_clientes"

    # Subir datos (reemplaza tabla si ya existe)
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    )

    job.result()

    # Query de cruce
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

    result = client.query(query).to_dataframe()

    st.success("Consulta lista 🎉")

    st.download_button(
        "Descargar resultado",
        result.to_csv(index=False),
        file_name="resultado.csv"
    )

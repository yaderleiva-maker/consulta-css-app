import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def run(usuario):

    st.title("Consulta CSS Panamá 🔍")

    uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])

    if uploaded_file:

        df = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig')

        df.columns = df.columns.str.strip().str.lower()

        if 'cedula' in df.columns:
            df['cedula'] = df['cedula'].astype(str).str.strip()

        st.write("Archivo cargado:", df.head())

        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )

        table_id = "proyecto-css-panama.consultas.temp_clientes"

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )
        )
        job.result()

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

         # -----------------------
    # HISTORIAL
    # -----------------------

    historial = pd.DataFrame([{
        "usuario": st.session_state.usuario,
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

    # -----------------------
    # RESULTADO
    # -----------------------

    st.success("✅ Consulta lista 🎉")

    st.download_button(
        "Descargar resultado",
        result.to_csv(index=False),
        file_name="resultado.csv",
        mime="text/csv"
    )

    st.write("Vista previa de resultados:")
    st.dataframe(result.head(10))

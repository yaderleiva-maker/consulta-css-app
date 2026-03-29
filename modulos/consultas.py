import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

def run(usuario):

    st.write(f"👤 Usuario: {usuario}")
    st.title("HEXAGON - Extractor de Datos 🔍")

    tipo_consulta = st.selectbox(
        "¿Qué deseas consultar?",
        ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
    )

    uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])

    if uploaded_file:

        df = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig')

        df.columns = df.columns.str.strip().str.lower()

        # -----------------------
# VALIDACIÓN DE ESTRUCTURA
# -----------------------

        columnas_validas = [
            "cedula", "nombre", "correo1", "correo2",
            "telf1", "telf2", "telf3", "telf4", "telf5",
            "telf6", "telf7", "telf8", "telf9", "telf10"
        ]

    # Columnas del archivo
        columnas_archivo = list(df.columns)

# 1. Validar columnas inválidas
        columnas_invalidas = [col for col in columnas_archivo if col not in columnas_validas]

        if columnas_invalidas:
            st.error(f"❌ Columnas no permitidas: {columnas_invalidas}")
            st.stop()

# 2. Validar que exista cedula
        if "cedula" not in df.columns:
            st.error("❌ El archivo debe contener la columna 'cedula' obligatoriamente")
            st.stop()

# 3. Validar que cedula no esté vacía
        if df["cedula"].isnull().all():
            st.error("❌ La columna 'cedula' está vacía")
            st.stop()

        st.success("✅ Archivo válido, estructura correcta")
       
        if 'cedula' in df.columns:
            df['cedula'] = df['cedula'].astype(str).str.strip()

        st.write("Archivo cargado:", df.head())

        # -----------------------
        # CONEXIÓN
        # -----------------------
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )

        # -----------------------
        # SUBIR DATA
        # -----------------------
        table_id = "proyecto-css-panama.consultas.temp_clientes"

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )
        )
        job.result()
       
        # -----------------------
        # QUERY
        # -----------------------
    if tipo_consulta == "CSS":

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

    elif tipo_consulta == "TELÉFONOS NUEVOS":

        query = """
        WITH archivo AS (
          SELECT cedula, telf1 AS numero FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf2 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf3 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf4 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf5 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf6 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf7 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf8 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf9 FROM `proyecto-css-panama.consultas.temp_clientes`
          UNION ALL SELECT cedula, telf10 FROM `proyecto-css-panama.consultas.temp_clientes`
        ),

        archivo_limpio AS (
          SELECT 
            cedula,
            numero,
            CONCAT(cedula, numero) AS clave
          FROM archivo
          WHERE numero IS NOT NULL AND numero != ''
        ),

        bigquery_data AS (
          SELECT 
            cedula,
            numero,
            CONCAT(cedula, numero) AS clave
          FROM `proyecto-css-panama.css_data.telefonos-actual`
        )

        SELECT 
          b.cedula,
          b.numero
        FROM bigquery_data b
        LEFT JOIN archivo_limpio a
        ON b.clave = a.clave
        WHERE a.clave IS NULL
        """

# luego aquí ejecutas SIEMPRE igual
result = client.query(query).to_dataframe()

        result = client.query(query).to_dataframe()

        # -----------------------
        # HISTORIAL
        # -----------------------
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

        st.dataframe(result.head(10))

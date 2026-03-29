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

        # -----------------------
        # LEER ARCHIVO
        # -----------------------
        df = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower()

        # -----------------------
        # VALIDACIONES
        # -----------------------
        columnas_validas = [
            "cedula", "nombre", "correo1", "correo2",
            "telf1", "telf2", "telf3", "telf4", "telf5",
            "telf6", "telf7", "telf8", "telf9", "telf10"
        ]

        columnas_invalidas = [col for col in df.columns if col not in columnas_validas]

        if columnas_invalidas:
            st.error(f"❌ Columnas no permitidas: {columnas_invalidas}")
            st.stop()

        if "cedula" not in df.columns:
            st.error("❌ Falta la columna 'cedula'")
            st.stop()

        if df["cedula"].isnull().all():
            st.error("❌ La columna 'cedula' está vacía")
            st.stop()

        df["cedula"] = df["cedula"].astype(str).str.strip()

        st.success("✅ Archivo válido")
        st.write(df.head())

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

        client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )
        ).result()

        # -----------------------
        # QUERY DINÁMICO
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
              SELECT cedula, telf1 AS valor FROM `proyecto-css-panama.consultas.temp_clientes`
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
              SELECT cedula, valor, CONCAT(cedula, valor) clave
              FROM archivo
              WHERE valor IS NOT NULL AND valor != ''
            ),

            base AS (
              SELECT cedula, numero AS valor, CONCAT(cedula, numero) clave
              FROM `proyecto-css-panama.css_data.telefonos-actual`
            )

            SELECT b.cedula, b.valor AS numero
            FROM base b
            LEFT JOIN archivo_limpio a
            ON b.clave = a.clave
            WHERE a.clave IS NULL
            """

        elif tipo_consulta == "CORREOS NUEVOS":

            query = """
            WITH archivo AS (
              SELECT cedula, correo1 AS valor FROM `proyecto-css-panama.consultas.temp_clientes`
              UNION ALL SELECT cedula, correo2 FROM `proyecto-css-panama.consultas.temp_clientes`
            ),

            archivo_limpio AS (
              SELECT cedula, valor, CONCAT(cedula, valor) clave
              FROM archivo
              WHERE valor IS NOT NULL AND valor != ''
            ),

            base AS (
              SELECT cedula, correo AS valor, CONCAT(cedula, correo) clave
              FROM `proyecto-css-panama.css_data.correos-actual`
            )

            SELECT b.cedula, b.valor AS correo
            FROM base b
            LEFT JOIN archivo_limpio a
            ON b.clave = a.clave
            WHERE a.clave IS NULL
            """

        # -----------------------
        # EJECUTAR QUERY
        # -----------------------
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
        st.success(f"✅ Consulta {tipo_consulta} lista 🎉")

        st.download_button(
            "Descargar resultado",
            result.to_csv(index=False),
            file_name="resultado.csv",
            mime="text/csv"
        )

        st.dataframe(result.head(10))

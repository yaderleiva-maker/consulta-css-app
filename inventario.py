import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

import sys
import traceback

# Al inicio del archivo, después de los imports
st.write("🔍 Depuración: inventario.py cargado correctamente")
# =========================================
# CONFIGURACIÓN
# =========================================
PROJECT_ID = "proyecto-css-panama"
DATASET = "inventario"

# =========================================
# CLIENTE BIGQUERY
# =========================================
def get_bq_client():
    try:

        # STREAMLIT CLOUD
        if "BIGQUERY_CREDENTIALS" in st.secrets:

            creds = service_account.Credentials.from_service_account_info(
                st.secrets["BIGQUERY_CREDENTIALS"]
            )

            client = bigquery.Client(
                credentials=creds,
                project=PROJECT_ID
            )

        # LOCAL
        else:
            client = bigquery.Client(project=PROJECT_ID)

        return client

    except Exception as e:
        st.error(f"❌ Error conectando con BigQuery: {e}")
        return None


# =========================================
# FUNCIÓN PRINCIPAL
# =========================================


def run(usuario):
try:
    st.write("🔍 Depuración: Entrando a run()")
        # todo tu código
except Exception as e:
    st.error(f"Error detallado: {str(e)}")
    st.code(traceback.format_exc())

    # =========================================
    # CLIENTE BIGQUERY
    # =========================================
    client = get_bq_client()

    if client is None:
        return

    # =========================================
    # CARGAR EMPRESAS
    # =========================================
    try:

        query_empresas = f"""
        SELECT
            id_empresa,
            nombre_empresa
        FROM `{PROJECT_ID}.{DATASET}.empresas`
        WHERE estado = 'ACTIVO'
        ORDER BY nombre_empresa
        """

        empresas_df = client.query(query_empresas).to_dataframe()

    except Exception as e:
        st.error(f"❌ Error cargando empresas: {e}")
        return

    # =========================================
    # VALIDAR EMPRESAS
    # =========================================
    if empresas_df.empty:
        st.warning("⚠️ No existen empresas activas.")
        return

    # =========================================
    # SELECTBOX EMPRESA
    # =========================================
    empresa_nombre = st.selectbox(
        "Seleccione la empresa",
        empresas_df["nombre_empresa"]
    )

    id_empresa = empresas_df.loc[
        empresas_df["nombre_empresa"] == empresa_nombre,
        "id_empresa"
    ].values[0]

    st.success(f"📌 Empresa seleccionada: {empresa_nombre}")

    st.markdown("---")

    # =========================================
    # SUBIR ARCHIVO
    # =========================================
    archivo = st.file_uploader(
        "Suba archivo Excel",
        type=["xlsx", "xls"]
    )

    # =========================================
    # SI HAY ARCHIVO
    # =========================================
    if archivo is not None:

        try:

            # =========================================
            # LEER EXCEL
            # =========================================
            df = pd.read_excel(archivo)

            st.subheader("📄 Vista previa")

            st.dataframe(df.head(10))

            # =========================================
            # VALIDAR COLUMNAS
            # =========================================
            columnas_requeridas = [
                "codigo_barra",
                "nombre_producto"
            ]

            faltantes = [
                col for col in columnas_requeridas
                if col not in df.columns
            ]

            if faltantes:
                st.error(f"❌ Faltan columnas: {faltantes}")
                return

            # =========================================
            # LIMPIEZA
            # =========================================
            df["codigo_barra"] = (
                df["codigo_barra"]
                .astype(str)
                .str.strip()
            )

            df["nombre_producto"] = (
                df["nombre_producto"]
                .astype(str)
                .str.strip()
            )

            # =========================================
            # COLUMNAS OPCIONALES
            # =========================================
            if "clasificacion" not in df.columns:
                df["clasificacion"] = "GENERAL"

            if "tipo_control" not in df.columns:
                df["tipo_control"] = "CANTIDAD"

            # =========================================
            # ELIMINAR DUPLICADOS DEL EXCEL
            # =========================================
            df = df.drop_duplicates(
                subset=["codigo_barra"]
            )

            # =========================================
            # BOTÓN CARGAR
            # =========================================
            if st.button("🚀 Cargar Productos"):

                now = datetime.now()

                productos = []

                progress_bar = st.progress(0)

                status = st.empty()

                for idx, row in df.iterrows():

                    producto = {
                        "id_producto": str(uuid.uuid4()),
                        "id_empresa": id_empresa,
                        "codigo_barra": row["codigo_barra"],
                        "nombre_producto": row["nombre_producto"],
                        "clasificacion": row["clasificacion"],
                        "tipo_control": row["tipo_control"],
                        "estado_producto": "ACTIVO",
                        "fuente_producto": "CLIENTE",
                        "fecha_creacion": now,
                        "fecha_actualizacion": now
                    }

                    productos.append(producto)

                    # =========================================
                    # PROGRESO
                    # =========================================
                    porcentaje = (idx + 1) / len(df)

                    progress_bar.progress(porcentaje)

                    status.text(
                        f"Procesando {idx + 1}/{len(df)}"
                    )

                # =========================================
                # DATAFRAME FINAL
                # =========================================
                df_final = pd.DataFrame(productos)

                # =========================================
                # INSERTAR BIGQUERY
                # =========================================
                tabla_destino = (
                    f"{PROJECT_ID}.{DATASET}.productos"
                )

                job = client.load_table_from_dataframe(
                    df_final,
                    tabla_destino
                )

                job.result()

                # =========================================
                # FINAL
                # =========================================
                st.success(
                    f"✅ {len(df_final)} productos cargados correctamente."
                )

                st.balloons()

        except Exception as e:
            st.error(f"❌ Error procesando archivo: {e}")

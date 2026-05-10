import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.cloud import bigquery

# CLIENTE BIGQUERY
client = bigquery.Client()

# ==========================
# FUNCIÓN PRINCIPAL
# ==========================
def run(usuario):

    st.title("📦 Inventario - Carga de Productos")

    # ==========================
    # CARGAR EMPRESAS
    # ==========================
    query_empresas = """
    SELECT
        id_empresa,
        nombre_empresa
    FROM `inventario.empresas`
    WHERE estado = 'ACTIVO'
    ORDER BY nombre_empresa
    """

    empresas_df = client.query(query_empresas).to_dataframe()

    if empresas_df.empty:
        st.warning("⚠️ No existen empresas activas.")
        return

    # SELECT EMPRESA
    empresa_nombre = st.selectbox(
        "Seleccione la empresa",
        empresas_df["nombre_empresa"]
    )

    # OBTENER ID_EMPRESA
    id_empresa = empresas_df.loc[
        empresas_df["nombre_empresa"] == empresa_nombre,
        "id_empresa"
    ].values[0]

    st.success(f"Empresa seleccionada: {empresa_nombre}")

    # ==========================
    # SUBIR EXCEL
    # ==========================
    archivo = st.file_uploader(
        "Suba el archivo Excel",
        type=["xlsx"]
    )

    if archivo is not None:

        try:

            df = pd.read_excel(archivo)

            st.subheader("📄 Vista previa")

            st.dataframe(df.head())

            # ==========================
            # VALIDAR COLUMNAS
            # ==========================
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

            # ==========================
            # LIMPIEZA
            # ==========================
            df["codigo_barra"] = df["codigo_barra"].astype(str).str.strip()
            df["nombre_producto"] = df["nombre_producto"].astype(str).str.strip()

            # OPCIONALES
            if "clasificacion" not in df.columns:
                df["clasificacion"] = "GENERAL"

            if "tipo_control" not in df.columns:
                df["tipo_control"] = "CANTIDAD"

            # ==========================
            # CAMPOS DEL SISTEMA
            # ==========================
            now = datetime.now()

            df["id_producto"] = [
                str(uuid.uuid4()) for _ in range(len(df))
            ]

            df["id_empresa"] = id_empresa
            df["estado_producto"] = "ACTIVO"
            df["fuente_producto"] = "CLIENTE"
            df["fecha_creacion"] = now
            df["fecha_actualizacion"] = now

            # ==========================
            # BOTÓN CARGAR
            # ==========================
            if st.button("📥 Insertar Productos"):

                tabla = "inventario.productos"

                job = client.load_table_from_dataframe(df, tabla)

                job.result()

                st.success(
                    f"✅ Se cargaron {len(df)} productos correctamente."
                )

        except Exception as e:
            st.error(f"❌ Error: {e}")

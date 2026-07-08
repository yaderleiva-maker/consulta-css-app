# modulos/hexagon_panama/consultas.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

def run(usuario, tipo_consulta):
    st.write(f"👤 Usuario: {usuario}")
    st.title("HEXAGON - Extractor de Datos 🔍")
    
    # Selector en la página principal
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write("### Selecciona el tipo de consulta")
    with col2:
        tipo_consulta = st.selectbox(
            "Tipo",
            ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            index=["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"].index(tipo_consulta),
            key="consulta_selector"
        )
    
    st.info(f"📌 **Consultando:** {tipo_consulta}")
    st.markdown("---")
    
    uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])

    if uploaded_file:

        # -----------------------
        # LEER ARCHIVO CON AUTODETECCIÓN DE SEPARADOR
        # -----------------------
        try:
            # Leer el contenido para detectar separador
            contenido = uploaded_file.getvalue().decode('utf-8-sig')
            primeras_lineas = contenido.split('\n')[:5]
            
            # Detectar separador (; o ,)
            separador_detectado = None
            max_cols = 0
            
            for sep in [';', ',', '\t', '|']:
                for linea in primeras_lineas:
                    if linea.count(sep) > max_cols:
                        max_cols = linea.count(sep)
                        separador_detectado = sep
            
            # Si no se detectó nada, usar coma por defecto
            if separador_detectado is None:
                separador_detectado = ','
            
            st.info(f"📌 Separador detectado: {'PUNTO Y COMA (;)' if separador_detectado == ';' else 'COMA (,)' if separador_detectado == ',' else 'TAB (\\t)' if separador_detectado == '\t' else f'({separador_detectado})'}")
            
            # Volver a leer el archivo desde el inicio
            uploaded_file.seek(0)
            
            # Leer CSV con el separador detectado
            df = pd.read_csv(
                uploaded_file,
                sep=separador_detectado,
                engine='python',
                encoding='utf-8-sig',
                dtype=str
            )
            
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {e}")
            st.stop()

        # Eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        
        # Limpiar nombres de columnas (minúsculas, sin espacios)
        df.columns = df.columns.str.strip().str.lower()
        
        # -----------------------
        # ELIMINAR FILAS SIN CÉDULA
        # -----------------------
        if "cedula" not in df.columns:
            st.error("❌ Falta la columna 'cedula'")
            st.stop()
        
        # Eliminar filas sin cédula
        df = df[df['cedula'].notna() & (df['cedula'].astype(str).str.strip() != '')]
        
        if df.empty:
            st.error("❌ El archivo no contiene cédulas válidas")
            st.stop()

        # -----------------------
        # LIMPIAR DECIMALES .0 EN TELÉFONOS
        # -----------------------
        columnas_telefono = [
            "telf1","telf2","telf3","telf4","telf5",
            "telf6","telf7","telf8","telf9","telf10"
        ]

        for col in columnas_telefono:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"\.0$", "", regex=True)
                    .str.strip()
                    .replace('nan', '')
                    .replace('None', '')
                )
        
        # -----------------------
        # VALIDACIONES
        # -----------------------
         # -----------------------
        # ASEGURAR COLUMNAS EXISTENTES
        # -----------------------
        # Columnas que deben existir para las consultas
        columnas_telefono = ["telf1", "telf2", "telf3", "telf4", "telf5",
                            "telf6", "telf7", "telf8", "telf9", "telf10"]
        
        columnas_correo = ["correo1", "correo2"]
        
        # Crear columnas faltantes
        for col in columnas_telefono + columnas_correo + ["nombre"]:
            if col not in df.columns:
                df[col] = ""
                st.warning(f"⚠️ Columna '{col}' no encontrada, creada vacía")

        # -----------------------
        # LIMPIAR DECIMALES .0 EN TELÉFONOS
        # -----------------------
        for col in columnas_telefono:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"\.0$", "", regex=True)
                    .str.strip()
                    .replace('nan', '')
                    .replace('None', '')
                )
        
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
            st.warning(f"⚠️ Columnas adicionales ignoradas: {columnas_invalidas}")

        if "cedula" not in df.columns:
            st.error("❌ Falta la columna 'cedula'")
            st.stop()

        # Eliminar filas sin cédula
        df = df[df['cedula'].notna() & (df['cedula'].astype(str).str.strip() != '')]
        
        if df.empty:
            st.error("❌ El archivo no contiene cédulas válidas")
            st.stop()

        # 🔥 NORMALIZAR TODO A STRING
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '').replace('None', '')

        st.success(f"✅ Archivo válido con {len(df)} filas y {len(df.columns)} columnas")
        st.write("Vista previa:", df.head())
        
        # -----------------------
        # CONEXIÓN A BIGQUERY
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
            try:
                client.load_table_from_dataframe(
                    df,
                    table_id,
                    job_config=bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE"
                    )
                ).result()
                st.success("✅ Datos subidos correctamente")
            except Exception as e:
                st.error(f"❌ Error al subir datos: {e}")
                st.stop()

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
              SELECT 
                CAST(cedula AS STRING) AS cedula,
                CAST(valor AS STRING) AS valor,
                CONCAT(CAST(cedula AS STRING), CAST(valor AS STRING)) AS clave
              FROM archivo
              WHERE valor IS NOT NULL 
                AND TRIM(CAST(valor AS STRING)) != ''
            ),
            base AS (
              SELECT 
                CAST(CEDULA AS STRING) AS cedula,
                CAST(NUMERO AS STRING) AS valor,
                CAST(TIPO AS STRING) AS tipo,
                CONCAT(CAST(CEDULA AS STRING), CAST(NUMERO AS STRING)) AS clave
              FROM `proyecto-css-panama.css_data.telefonos-actual`
              WHERE CEDULA IN (
                  SELECT DISTINCT cedula 
                  FROM `proyecto-css-panama.consultas.temp_clientes`
              )
            )
            SELECT b.cedula, b.valor AS numero, b.tipo
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
              SELECT 
                CAST(cedula AS STRING) AS cedula,
                CAST(valor AS STRING) AS valor,
                CONCAT(CAST(cedula AS STRING), CAST(valor AS STRING)) AS clave
              FROM archivo
              WHERE valor IS NOT NULL 
                AND TRIM(CAST(valor AS STRING)) != ''
            ),
            base AS (
              SELECT 
                CAST(CEDULA AS STRING) AS cedula, 
                CAST(EMAIL AS STRING) AS valor,
                CONCAT(CAST(CEDULA AS STRING), CAST(EMAIL AS STRING)) AS clave
              FROM `proyecto-css-panama.css_data.correos-actual`
              WHERE CEDULA IN (
                  SELECT DISTINCT cedula 
                  FROM `proyecto-css-panama.consultas.temp_clientes`
              )
            )
            SELECT b.cedula, b.valor AS correo
            FROM base b
            LEFT JOIN archivo_limpio a
            ON b.clave = a.clave
            WHERE a.clave IS NULL
            """

        else:
            st.error(f"❌ Tipo de consulta no reconocido: {tipo_consulta}")
            st.stop()

        # -----------------------
        # EJECUTAR QUERY
        # -----------------------
        with st.spinner("Ejecutando consulta..."):
            try:
                result = client.query(query).to_dataframe()
                st.success(f"✅ Consulta completada: {len(result)} registros encontrados")
            except Exception as e:
                st.error(f"❌ Error al ejecutar consulta: {e}")
                st.stop()

        # -----------------------
        # HISTORIAL
        # -----------------------
        try:
            historial = pd.DataFrame([{
                "usuario": usuario,
                "fecha": datetime.now(),
                "tipo_consulta": tipo_consulta,
                "cantidad_registros": len(result),
                "archivo": uploaded_file.name
            }])

            client.load_table_from_dataframe(
                historial,
                "proyecto-css-panama.consultas.historial_consultas",
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND"
                )
            ).result()
        except Exception as e:
            st.warning(f"⚠️ No se pudo guardar historial: {e}")

        # -----------------------
        # RESULTADO
        # -----------------------
        st.success(f"✅ Consulta {tipo_consulta} lista 🎉")

        col1, col2 = st.columns([1, 3])
        with col1:
            st.download_button(
                "📥 Descargar resultado",
                result.to_csv(index=False),
                file_name=f"resultado_{tipo_consulta.replace(' ', '_')}.csv",
                mime="text/csv"
            )
        with col2:
            st.write(f"📊 **{len(result)}** registros encontrados")

        st.dataframe(result.head(20))

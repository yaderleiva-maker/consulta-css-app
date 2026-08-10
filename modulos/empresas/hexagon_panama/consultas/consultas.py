import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Import del puente a Cobranza (ruta relativa correcta)
from ..cobranza.investigacion import (
    anexar_investigacion,
    generar_reporte_investigacion,
    generar_excel_reporte,
)

# ============================================================
# FUNCIÓN PARA OBTENER CARTERA CARGADA DESDE BIGQUERY
# ============================================================

def obtener_cartera_para_consulta(client, proyecto_id):
    """
    Obtiene la cartera cargada de un proyecto en el formato que espera la consulta.
    Retorna DataFrame con columnas: cedula, nombre, telf1..telf10, correo1, correo2
    """
    query = """
    WITH clientes AS (
      SELECT DISTINCT p.id_persona, p.identificacion AS cedula, p.nombre
      FROM `proyecto-css-panama.cobranza.cuentas` c
      JOIN `proyecto-css-panama.cobranza.personas` p ON p.id_persona = c.id_persona
      WHERE c.id_proyecto = @proyecto_id
    ),
    telefonos_numerados AS (
      SELECT tp.id_persona, t.numero,
             ROW_NUMBER() OVER (
               PARTITION BY tp.id_persona ORDER BY tp.prioridad, t.numero
             ) AS posicion
      FROM `proyecto-css-panama.cobranza.telefonos_proyecto` tp
      JOIN `proyecto-css-panama.cobranza.telefonos` t ON t.id_telefono = tp.id_telefono
      WHERE tp.id_proyecto = @proyecto_id
    ),
    telefonos AS (
      SELECT id_persona,
        MAX(IF(posicion = 1, numero, NULL)) AS telf1,
        MAX(IF(posicion = 2, numero, NULL)) AS telf2,
        MAX(IF(posicion = 3, numero, NULL)) AS telf3,
        MAX(IF(posicion = 4, numero, NULL)) AS telf4,
        MAX(IF(posicion = 5, numero, NULL)) AS telf5,
        MAX(IF(posicion = 6, numero, NULL)) AS telf6,
        MAX(IF(posicion = 7, numero, NULL)) AS telf7,
        MAX(IF(posicion = 8, numero, NULL)) AS telf8,
        MAX(IF(posicion = 9, numero, NULL)) AS telf9,
        MAX(IF(posicion = 10, numero, NULL)) AS telf10
      FROM telefonos_numerados GROUP BY id_persona
    ),
    correos_numerados AS (
      SELECT cp.id_persona, c.correo,
             ROW_NUMBER() OVER (
               PARTITION BY cp.id_persona ORDER BY cp.prioridad, c.correo
             ) AS posicion
      FROM `proyecto-css-panama.cobranza.correos_proyecto` cp
      JOIN `proyecto-css-panama.cobranza.correos` c ON c.id_correo = cp.id_correo
      WHERE cp.id_proyecto = @proyecto_id
    ),
    correos AS (
      SELECT id_persona,
        MAX(IF(posicion = 1, correo, NULL)) AS correo1,
        MAX(IF(posicion = 2, correo, NULL)) AS correo2
      FROM correos_numerados GROUP BY id_persona
    )
    SELECT clientes.cedula, clientes.nombre,
           telefonos.telf1, telefonos.telf2, telefonos.telf3, telefonos.telf4,
           telefonos.telf5, telefonos.telf6, telefonos.telf7, telefonos.telf8,
           telefonos.telf9, telefonos.telf10, correos.correo1, correos.correo2
    FROM clientes
    LEFT JOIN telefonos USING (id_persona)
    LEFT JOIN correos USING (id_persona)
    ORDER BY cedula
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("proyecto_id", "STRING", proyecto_id)
    ])
    return client.query(query, job_config=job_config).to_dataframe()

# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def run(usuario, tipo_consulta):
    st.write(f"👤 Usuario: {usuario}")
    st.title("HEXAGON - Extractor de Datos 🔍")
    
    # Selector de tipo de consulta
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
    # SELECCIONAR PROYECTO
    # -----------------------
    proyectos = client.query("""
        SELECT id_proyecto, nombre
        FROM `proyecto-css-panama.cobranza.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre
    """).to_dataframe()

    if proyectos.empty:
        st.error("❌ No hay proyectos activos.")
        st.stop()

    nombre_proyecto = st.selectbox(
        "🏢 Proyecto de cartera",
        proyectos['nombre'].tolist(),
        key="proyecto_consulta",
    )
    proyecto_id = proyectos.loc[
        proyectos['nombre'].eq(nombre_proyecto), 'id_proyecto'
    ].iloc[0]

    # -----------------------
    # ORIGEN DE LOS DATOS (CARTERA CARGADA O CSV EXTERNO)
    # -----------------------
    origen = st.radio(
        "📂 Base para investigar",
        ["📊 Cartera cargada del proyecto", "📄 CSV externo"],
        horizontal=True,
        key="origen_consulta"
    )

    df = None
    if origen == "📊 Cartera cargada del proyecto":
        with st.spinner("📊 Cargando cartera desde BigQuery..."):
            df = obtener_cartera_para_consulta(client, proyecto_id)
        if df.empty:
            st.warning("⚠️ El proyecto no tiene clientes cargados todavía. Carga una cartera primero en 'Carga de Cartera'.")
            st.stop()
        st.success(f"✅ Usando cartera cargada: {len(df):,} clientes.")
        st.dataframe(df.head(5), use_container_width=True)
    else:
        uploaded_file = st.file_uploader("📄 Sube tu archivo CSV", type=["csv"], key="upload_consulta")
        if uploaded_file is None:
            st.stop()

        # -----------------------
        # LEER CSV (código original)
        # -----------------------
        try:
            contenido = uploaded_file.getvalue().decode('utf-8-sig')
            primeras_lineas = contenido.split('\n')[:5]
            
            separador_detectado = None
            max_cols = 0
            
            for sep in [';', ',', '\t', '|']:
                for linea in primeras_lineas:
                    if linea.count(sep) > max_cols:
                        max_cols = linea.count(sep)
                        separador_detectado = sep
            
            if separador_detectado is None:
                separador_detectado = ','
            
            st.info(f"📌 Separador detectado: {'PUNTO Y COMA (;)' if separador_detectado == ';' else 'COMA (,)' if separador_detectado == ',' else 'TAB (\\t)' if separador_detectado == '\t' else f'({separador_detectado})'}")
            
            uploaded_file.seek(0)
            
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
        df.columns = df.columns.str.strip().str.lower()
        
        # Validar columna 'cedula'
        if "cedula" not in df.columns:
            st.error("❌ Falta la columna 'cedula'")
            st.stop()
        
        df = df[df['cedula'].notna() & (df['cedula'].astype(str).str.strip() != '')]
        
        if df.empty:
            st.error("❌ El archivo no contiene cédulas válidas")
            st.stop()

        # Limpiar decimales .0 en teléfonos y asegurar columnas
        columnas_telefono = ["telf1","telf2","telf3","telf4","telf5",
                            "telf6","telf7","telf8","telf9","telf10"]
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
        
        columnas_correo = ["correo1", "correo2"]
        for col in columnas_telefono + columnas_correo + ["nombre"]:
            if col not in df.columns:
                df[col] = ""
        
        # Normalizar todo a string
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '').replace('None', '')

        st.success(f"✅ Archivo válido con {len(df)} filas")
        st.dataframe(df.head(5), use_container_width=True)

    # -----------------------
    # SUBIR DF A TEMP (independientemente del origen)
    # -----------------------
    table_id = "proyecto-css-panama.consultas.temp_clientes"
    with st.spinner("🔄 Subiendo datos a BigQuery..."):
        try:
            client.load_table_from_dataframe(
                df,
                table_id,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE"
                )
            ).result()
            st.success("✅ Datos subidos correctamente a temp_clientes")
        except Exception as e:
            st.error(f"❌ Error al subir datos: {e}")
            st.stop()

    # -----------------------
    # EJECUTAR CONSULTA
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
    elif tipo_consulta in ("TELÉFONOS NUEVOS", "TELÉFONOS NUEVOS"):
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

    with st.spinner("🔍 Ejecutando consulta..."):
        try:
            result = client.query(query).to_dataframe()
            st.success(f"✅ Consulta completada: {len(result):,} registros encontrados")
        except Exception as e:
            st.error(f"❌ Error al ejecutar consulta: {e}")
            st.stop()

    # -----------------------
    # HISTORIAL (opcional)
    # -----------------------
    try:
        historial = pd.DataFrame([{
            "usuario": usuario,
            "fecha": datetime.now(),
            "tipo_consulta": tipo_consulta,
            "cantidad_registros": len(result),
            "archivo": "cartera" if origen == "📊 Cartera cargada del proyecto" else "CSV externo"
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
    # RESULTADO + ANEXADO AUTOMÁTICO
    # -----------------------
    st.success(f"✅ Consulta {tipo_consulta} lista: {len(result):,} registros")

    # Determinar si el tipo de consulta permite anexado
    if tipo_consulta in ("TELEFONOS NUEVOS", "TELÉFONOS NUEVOS"):
        tipo_anexo = "telefonos"
    elif tipo_consulta == "CORREOS NUEVOS":
        tipo_anexo = "correos"
    else:
        tipo_anexo = None

    # Si es de teléfonos o correos, anexar automáticamente
    if tipo_anexo:
        with st.spinner(f"🔄 Anexando {tipo_anexo} automáticamente a Cobranza..."):
            total, anexados, errores, detalle = anexar_investigacion(
                result, proyecto_id, tipo_anexo
            )

        if errores:
            st.warning(f"⚠️ {detalle}")
        else:
            st.success(f"✅ {detalle}")

        # Mostrar métricas de anexado
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total", f"{total:,}")
        with col2:
            st.metric("✅ Anexados", f"{anexados:,}")
        with col3:
            st.metric("❌ Errores", f"{errores:,}")

        # Generar reporte completo DESPUÉS del anexo
        with st.spinner("📊 Generando reporte actualizado..."):
            reporte = generar_reporte_investigacion(proyecto_id)

        st.download_button(
            label="📥 Descargar reporte completo actualizado",
            data=generar_excel_reporte(reporte),
            file_name=f"INVESTIGACION_{proyecto_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # Siempre ofrecer descarga del resultado de la consulta (CSV)
    st.download_button(
        label="📥 Descargar resultado de la consulta (CSV)",
        data=result.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"resultado_{tipo_consulta.replace(' ', '_')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.dataframe(result.head(20), use_container_width=True)

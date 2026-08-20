# modulos/empresas/hexagon_panama/cobranza/gestiones_diarias.py
import streamlit as st
import pandas as pd
import yaml
import json
import uuid
from datetime import datetime
from google.cloud import bigquery
from services.bigquery import get_client

# ============================================================
# HELPER: CARGAR CONFIGURACIÓN YAML UNIFICADA
# ============================================================
def cargar_configuracion(proyecto: str = "jamar") -> dict:
    path = f"modulos/empresas/hexagon_panama/consultas/proyectos/{proyecto.lower()}.yaml"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"❌ Error al cargar la configuración de {proyecto} desde '{path}': {e}")
        return {}

# ============================================================
# HELPER: MAPEO Y NORMALIZACIÓN DE COLUMNAS DEL CRM
# ============================================================
def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    mapa_columnas = {
        'Cuenta': 'cuenta',
        'Resultado de la llamada': 'gestion',
        'Comentario': 'comentario',
        'Num de contacto': 'telefono',
        'Created At': 'fecha_gestion',
        'Fecha de Promesa de Pago': 'fecha_promesa',
        'Monto de promesa de pago': 'monto_promesa',
        'Asignado a': 'asesor',
        'Estado de la cuenta': 'estado_cuenta',
        'Fecha de reprogramación': 'fecha_reprogramacion'
    }
    return df.rename(columns=mapa_columnas)

# ============================================================
# MOTOR DE VALIDACIÓN Y HOMOLOGACIÓN
# ============================================================
def procesar_y_validar_gestiones(df: pd.DataFrame, config: dict):
    df = normalizar_columnas(df)
    
    validaciones_cfg = config.get("validaciones", {})
    arbol = config.get("arbol_tipologias", {})

    id_carga_sesion = str(uuid.uuid4())
    id_proyecto = str(config.get("proyecto", "JAMAR"))

    filas_validas = []
    filas_invalidas = []
    
    # Contador de gestiones con al menos un error
    gestiones_rechazadas_cnt = 0

    for idx, row in df.iterrows():
        fallos = []
        
        # ID Único de esta gestión para vinculación relacional
        id_gestion = str(uuid.uuid4())
        
        cuenta = str(row.get("cuenta", "")).strip()
        gestion = str(row.get("gestion", "")).strip()
        comentario = str(row.get("comentario", "")).strip()
        telefono = str(row.get("telefono", "")).strip()
        fecha_gestion = row.get("fecha_gestion")
        fecha_promesa = row.get("fecha_promesa")
        monto_promesa = row.get("monto_promesa")
        asesor = str(row.get("asesor", "")).strip()

        # 1. Validaciones de presencia
        if validaciones_cfg.get("comentario_obligatorio") and (not comentario or comentario.lower() == 'nan'):
            fallos.append(("CAMPO_OBLIGATORIO", "comentario", "Comentario es obligatorio"))
            
        if validaciones_cfg.get("numero_contacto_obligatorio") and (not telefono or telefono.lower() == 'nan'):
            fallos.append(("CAMPO_OBLIGATORIO", "telefono", "Teléfono de contacto es obligatorio"))
            
        if validaciones_cfg.get("resultado_obligatorio") and (not gestion or gestion.lower() == 'nan'):
            fallos.append(("CAMPO_OBLIGATORIO", "gestion", "Resultado/Gestión es obligatorio"))

        # 2. Homologación con árbol de tipologías
        info_arbol = arbol.get(gestion)
        if not info_arbol:
            fallos.append(("TIPOLOGIA_INVALIDA", "gestion", f"La tipología '{gestion}' no existe en el árbol del proyecto"))
            es_promesa = False
            codigo_jamar = None
            contactabilidad = None
            prioridad = None
        else:
            es_promesa = info_arbol.get("es_promesa", False)
            codigo_jamar = info_arbol.get("codigo_jamar")
            contactabilidad = info_arbol.get("contactabilidad")
            prioridad = info_arbol.get("prioridad")

        # 3. Validaciones de promesa de pago
        if es_promesa and validaciones_cfg.get("promesa", {}).get("requiere_fecha"):
            if pd.isna(fecha_promesa) or str(fecha_promesa).strip() in ["", "nan", "None"]:
                fallos.append(("PROMESA_SIN_FECHA", "fecha_promesa", "Esta tipología requiere fecha de promesa de pago"))
                
        if es_promesa and validaciones_cfg.get("promesa", {}).get("requiere_monto"):
            try:
                monto_val = float(monto_promesa) if pd.notna(monto_promesa) else 0.0
                if monto_val <= 0:
                    fallos.append(("PROMESA_MONTO_INVALIDO", "monto_promesa", "Esta tipología requiere un monto de promesa válido"))
            except (ValueError, TypeError):
                fallos.append(("PROMESA_MONTO_INVALIDO", "monto_promesa", "El monto de promesa no es un número válido"))

        # Registro operacional para cobranza.gestiones
        registro = {
            "id_gestion": id_gestion,
            "cuenta": cuenta,
            "gestion": gestion,
            "comentario": comentario,
            "telefono": telefono,
            "fecha_gestion": pd.to_datetime(fecha_gestion) if pd.notna(fecha_gestion) else datetime.now(),
            "fecha_promesa": pd.to_datetime(fecha_promesa) if pd.notna(fecha_promesa) and str(fecha_promesa).strip() not in ["", "nan", "None"] else None,
            "monto_promesa": float(monto_promesa) if pd.notna(monto_promesa) and str(monto_promesa).strip() not in ["", "nan", "None"] else 0.0,
            "asesor": asesor,
            "codigo_jamar": codigo_jamar,
            "contactabilidad": contactabilidad,
            "prioridad": prioridad,
            "created_at": datetime.now()
        }

        if fallos:
            gestiones_rechazadas_cnt += 1
            datos_raw_str = json.dumps(row.astype(str).to_dict(), ensure_ascii=False)
            now_ts = datetime.now()
            
            # Generamos una fila de auditoría por cada error detectado vinculada a id_gestion
            for tipo_err, campo_err, msj_err in fallos:
                registro_error = {
                    "id_validacion": str(uuid.uuid4()),
                    "id_gestion": str(id_gestion),
                    "id_carga": str(id_carga_sesion),
                    "id_proyecto": str(id_proyecto),
                    "cuenta": str(cuenta),
                    "tipo_error": str(tipo_err),
                    "campo": str(campo_err),
                    "mensaje_error": str(msj_err),
                    "datos_raw": datos_raw_str,
                    "estado": "PENDIENTE",
                    "created_at": now_ts
                }
                filas_invalidas.append(registro_error)
        else:
            filas_validas.append(registro)

    df_validos = pd.DataFrame(filas_validas)
    df_invalidos = pd.DataFrame(filas_invalidas)

    # TIPADO Y ALINEACIÓN EXPLÍCITA DEL DATAFRAME DE ERRORES
    columnas_validaciones = [
        "id_validacion", "id_gestion", "id_carga", "id_proyecto", 
        "cuenta", "tipo_error", "campo", "mensaje_error", 
        "datos_raw", "estado", "created_at"
    ]

    if not df_invalidos.empty:
        for col in columnas_validaciones:
            if col not in df_invalidos.columns:
                df_invalidos[col] = None

        df_invalidos = df_invalidos[columnas_validaciones]

        # Casting explícito de tipos STRING
        columnas_string = [
            "id_validacion", "id_gestion", "id_carga", "id_proyecto",
            "cuenta", "tipo_error", "campo", "mensaje_error",
            "datos_raw", "estado"
        ]
        for col in columnas_string:
            df_invalidos[col] = df_invalidos[col].astype(str)

        # Casting explícito a TIMESTAMP
        df_invalidos["created_at"] = pd.to_datetime(df_invalidos["created_at"], errors="coerce")

    return df_validos, df_invalidos, gestiones_rechazadas_cnt

# ============================================================
# INTERFAZ STREAMLIT
# ============================================================
def render_ui():
    st.title("💼 Carga y Validación de Gestiones Diarias")
    st.caption("Procesamiento operativo contra reglas de negocio YAML")

    proyecto = st.selectbox("Selecciona el Proyecto", ["JAMAR"], index=0)
    config = cargar_configuracion(proyecto)
    
    if not config:
        st.error("No se pudo cargar la configuración.")
        return

    archivo = st.file_uploader("Cargar archivo de gestiones (CSV o Excel)", type=["csv", "xlsx"])

    if archivo:
        try:
            if archivo.name.endswith(".csv"):
                df_input = pd.read_csv(archivo)
            else:
                df_input = pd.read_excel(archivo)

            st.subheader("Preview de datos a procesar")
            st.dataframe(df_input.head(5), use_container_width=True)

            if st.button("🚀 Validar y Cargar a BigQuery", type="primary"):
                with st.spinner("Procesando y aplicando normalización de columnas..."):
                    df_validos, df_invalidos, rechazadas_cnt = procesar_y_validar_gestiones(df_input, config)

                # Muestra métricas transparentes al usuario
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Gestiones", len(df_input))
                col2.metric("Válidas (BigQuery)", len(df_validos))
                col3.metric("Gestiones Rechazadas", rechazadas_cnt)
                col4.metric("Errores Detectados", len(df_invalidos))

                client = get_client()

                # 1. CARGA DE REGISTROS VÁLIDOS
                if not df_validos.empty:
                    job = client.load_table_from_dataframe(
                        df_validos, 
                        "proyecto-css-panama.cobranza.gestiones"
                    )
                    job.result()
                    st.success(f"✅ {len(df_validos)} gestiones insertadas en cobranza.gestiones")

                # 2. CARGA DE REGISTROS DE AUDITORÍA Y CONTROL DE CALIDAD
                if not df_invalidos.empty:
                    st.warning(f"⚠️ {rechazadas_cnt} gestiones rechazadas ({len(df_invalidos)} errores detallados)")
                    st.dataframe(
                        df_invalidos[["cuenta", "tipo_error", "campo", "mensaje_error"]], 
                        use_container_width=True
                    )

                    # Configuración estricta del esquema según la definición oficial de BigQuery
                    job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField("id_validacion", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("id_gestion", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("id_carga", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("id_proyecto", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("cuenta", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("tipo_error", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("campo", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("mensaje_error", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("datos_raw", "JSON", mode="NULLABLE"),
                            bigquery.SchemaField("estado", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                        ],
                        write_disposition="WRITE_APPEND"
                    )

                    job_err = client.load_table_from_dataframe(
                        df_invalidos, 
                        "proyecto-css-panama.cobranza.validaciones_gestiones",
                        job_config=job_config
                    )
                    job_err.result()
                    st.toast("Alertas guardadas en log de validaciones", icon="⚠️")

        except Exception as e:
            st.error(f"❌ Error al procesar el archivo: {e}")

if __name__ == "__main__":
    render_ui()

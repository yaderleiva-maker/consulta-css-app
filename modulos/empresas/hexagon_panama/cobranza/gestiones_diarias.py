# modulos/empresas/hexagon_panama/cobranza/gestiones_diarias.py
import streamlit as st
import pandas as pd
import yaml
import json
import uuid
from datetime import datetime
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
    # 1. Mapeo de nombres de columnas
    df = normalizar_columnas(df)
    
    validaciones_cfg = config.get("validaciones", {})
    arbol = config.get("arbol_tipologias", {})

    filas_validas = []
    filas_invalidas = []

    for idx, row in df.iterrows():
        errores = []
        
        # Extracción de campos
        cuenta = str(row.get("cuenta", "")).strip()
        gestion = str(row.get("gestion", "")).strip()
        comentario = str(row.get("comentario", "")).strip()
        telefono = str(row.get("telefono", "")).strip()
        fecha_gestion = row.get("fecha_gestion")
        fecha_promesa = row.get("fecha_promesa")
        monto_promesa = row.get("monto_promesa")
        asesor = str(row.get("asesor", "")).strip()

        # Validaciones de presencia
        if validaciones_cfg.get("comentario_obligatorio") and (not comentario or comentario.lower() == 'nan'):
            errores.append("Comentario es obligatorio")
        if validaciones_cfg.get("numero_contacto_obligatorio") and (not telefono or telefono.lower() == 'nan'):
            errores.append("Teléfono de contacto es obligatorio")
        if validaciones_cfg.get("resultado_obligatorio") and (not gestion or gestion.lower() == 'nan'):
            errores.append("Resultado/Gestión es obligatorio")

        # Homologación con árbol de tipologías
        info_arbol = arbol.get(gestion)
        if not info_arbol:
            errores.append(f"La tipología '{gestion}' no existe en el árbol del proyecto")
            es_promesa = False
            codigo_jamar = None
            contactabilidad = None
            prioridad = None
        else:
            es_promesa = info_arbol.get("es_promesa", False)
            codigo_jamar = info_arbol.get("codigo_jamar")
            contactabilidad = info_arbol.get("contactabilidad")
            prioridad = info_arbol.get("prioridad")

        # Validaciones de promesa de pago
        if es_promesa and validaciones_cfg.get("promesa", {}).get("requiere_fecha"):
            if pd.isna(fecha_promesa) or str(fecha_promesa).strip() in ["", "nan", "None"]:
                errores.append("Esta tipología requiere fecha de promesa de pago")
                
        if es_promesa and validaciones_cfg.get("promesa", {}).get("requiere_monto"):
            try:
                monto_val = float(monto_promesa) if pd.notna(monto_promesa) else 0.0
                if monto_val <= 0:
                    errores.append("Esta tipología requiere un monto de promesa válido")
            except (ValueError, TypeError):
                errores.append("El monto de promesa no es un número válido")

        # Construcción de registro para BQ
        registro = {
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

        if errores:
            registro_error = {
                "id_validacion": str(uuid.uuid4()),  # Campo obligatorio para el esquema de BigQuery
                "fecha_proceso": datetime.now(),
                "proyecto": config.get("proyecto", "JAMAR"),
                "archivo": "Carga_Manual_Streamlit",
                "datos_raw": json.dumps(row.astype(str).to_dict(), ensure_ascii=False),
                "errores": " | ".join(errores)
            }
            filas_invalidas.append(registro_error)
        else:
            filas_validas.append(registro)

    return pd.DataFrame(filas_validas), pd.DataFrame(filas_invalidas)

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
                    df_validos, df_invalidos = procesar_y_validar_gestiones(df_input, config)

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Registros", len(df_input))
                col2.metric("Válidos (BigQuery)", len(df_validos))
                col3.metric("Rechazados", len(df_invalidos))

                client = get_client()

                if not df_validos.empty:
                    job = client.load_table_from_dataframe(
                        df_validos, 
                        "proyecto-css-panama.cobranza.gestiones"
                    )
                    job.result()
                    st.success(f"✅ {len(df_validos)} gestiones insertadas en cobranza.gestiones")

                if not df_invalidos.empty:
                    st.warning(f"⚠️ {len(df_invalidos)} registros con inconsistencias detectadas")
                    st.dataframe(df_invalidos[["datos_raw", "errores"]], use_container_width=True)
                    
                    job_err = client.load_table_from_dataframe(
                        df_invalidos, 
                        "proyecto-css-panama.cobranza.validaciones_gestiones"
                    )
                    job_err.result()
                    st.toast("Alertas guardadas en log de validaciones", icon="⚠️")

        except Exception as e:
            st.error(f"❌ Error al procesar el archivo: {e}")

if __name__ == "__main__":
    render_ui()

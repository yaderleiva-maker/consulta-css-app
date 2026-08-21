# modulos/empresas/hexagon_panama/cobranza/gestiones_diarias.py
import streamlit as st
import pandas as pd
import yaml
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
    df = normalizar_columnas(df)
    
    validaciones_cfg = config.get("validaciones", {})
    arbol = config.get("arbol_tipologias", {})

    id_carga_sesion = str(uuid.uuid4())
    id_proyecto = str(config.get("proyecto", "JAMAR"))
    now_dt = datetime.now()
    now_iso = now_dt.isoformat()

    filas_validas = []
    registros_invalidos_json = []
    
    gestiones_rechazadas_cnt = 0

    for idx, row in df.iterrows():
        fallos = []
        
        id_gestion = str(uuid.uuid4())
        
        cuenta = str(row.get("cuenta", "")).strip()
        gestion = str(row.get("gestion", "")).strip()
        comentario = str(row.get("comentario", "")).strip()
        telefono = str(row.get("telefono", "")).strip()
        fecha_gestion = row.get("fecha_gestion")
        fecha_promesa = row.get("fecha_promesa")
        monto_promesa = row.get("monto_promesa")
        asesor = str(row.get("asesor", "")).strip()
        estado_cuenta = str(row.get("estado_cuenta", "")).strip()
        fecha_reprogramacion = row.get("fecha_reprogramacion")

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

        if fallos:
            gestiones_rechazadas_cnt += 1
            # Objeto dict directo para evitar conflicto con PyArrow y el campo JSON
            row_dict_raw = row.astype(str).to_dict()
            
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
                    "datos_raw": row_dict_raw,  # Dict nativo enviado directamente a JSON en BigQuery
                    "estado": "PENDIENTE",
                    "created_at": now_iso
                }
                registros_invalidos_json.append(registro_error)
        else:
            # Registro operacional alineado al esquema oficial de cobranza.gestiones
            registro_valido = {
                "id_gestion": str(id_gestion),
                "id_carga": str(id_carga_sesion),
                "id_proyecto": str(id_proyecto),
                "cuenta": str(cuenta),
                "estado_cuenta": str(estado_cuenta) if estado_cuenta != 'nan' else None,
                "resultado_llamada": str(gestion),
                "asignado_a": str(asesor) if asesor != 'nan' else None,
                "numero_contacto": str(telefono) if telefono != 'nan' else None,
                "monto_promesa": float(monto_promesa) if pd.notna(monto_promesa) and str(monto_promesa).strip() not in ["", "nan", "None"] else 0.0,
                "fecha_promesa": pd.to_datetime(fecha_promesa) if pd.notna(fecha_promesa) and str(fecha_promesa).strip() not in ["", "nan", "None"] else None,
                "fecha_reprogramacion": pd.to_datetime(fecha_reprogramacion) if pd.notna(fecha_reprogramacion) and str(fecha_reprogramacion).strip() not in ["", "nan", "None"] else None,
                "comentario": str(comentario),
                "codigo_jamar": str(codigo_jamar) if codigo_jamar else None,
                "contactabilidad": str(contactabilidad) if contactabilidad else None,
                "prioridad": int(prioridad) if prioridad is not None else None,
                "created_at": pd.to_datetime(fecha_gestion) if pd.notna(fecha_gestion) else now_dt,
                "fuente": "CARGA_MANUAL_STREAMLIT",
                "fecha_carga": now_dt,
                "usuario_carga": "SISTEMA"
            }
            filas_validas.append(registro_valido)

    df_validos = pd.DataFrame(filas_validas)
    
    return df_validos, registros_invalidos_json, gestiones_rechazadas_cnt

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
                    df_validos, registros_invalidos_json, rechazadas_cnt = procesar_y_validar_gestiones(df_input, config)

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Gestiones", len(df_input))
                col2.metric("Válidas (BigQuery)", len(df_validos))
                col3.metric("Gestiones Rechazadas", rechazadas_cnt)
                col4.metric("Errores Detectados", len(registros_invalidos_json))

                client = get_client()

                # 1. CARGA DE REGISTROS VÁLIDOS
                if not df_validos.empty:
                    job = client.load_table_from_dataframe(
                        df_validos, 
                        "proyecto-css-panama.cobranza.gestiones"
                    )
                    job.result()
                    st.success(f"✅ {len(df_validos)} gestiones insertadas en cobranza.gestiones")

                # 2. CARGA DE AUDITORÍA VÍA insert_rows_json (SIN PYARROW)
                if registros_invalidos_json:
                    st.warning(f"⚠️ {rechazadas_cnt} gestiones rechazadas ({len(registros_invalidos_json)} errores detallados)")
                    
                    df_preview_errors = pd.DataFrame(registros_invalidos_json)
                    st.dataframe(
                        df_preview_errors[["cuenta", "tipo_error", "campo", "mensaje_error"]], 
                        use_container_width=True
                    )

                    # Inserción directa en streaming/JSON sin pasar por PyArrow
                    errores_bq = client.insert_rows_json(
                        "proyecto-css-panama.cobranza.validaciones_gestiones",
                        registros_invalidos_json
                    )

                    if errores_bq:
                        st.error(f"❌ Error al guardar log en BigQuery: {errores_bq}")
                    else:
                        st.toast(f"✅ {len(registros_invalidos_json)} errores registrados en validaciones_gestiones", icon="⚠️")

        except Exception as e:
            st.error(f"❌ Error al procesar el archivo: {e}")

if __name__ == "__main__":
    render_ui()

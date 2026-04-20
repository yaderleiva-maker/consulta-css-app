import streamlit as st
import pandas as pd
import uuid
import re
from google.cloud import bigquery
from datetime import datetime

# -----------------------
# CONFIG
# -----------------------
client = bigquery.Client.from_service_account_json('clave_investigacion.json')

PROJECT_ID = "proyecto-css-panama"
DATASET = "crm_core"

st.title("🚀 Carga Inteligente de Clientes")

# -----------------------
# SELECCIÓN PROYECTO
# -----------------------
proyectos = ["veterinaria_001", "cobros_001", "ventas_001"]
id_proyecto = st.selectbox("Selecciona el proyecto", proyectos)

uploaded_file = st.file_uploader("Sube tu CSV", type=["csv"])

# -----------------------
# FUNCIONES MEJORADAS
# -----------------------

def limpiar_numero(numero):
    """Limpia y valida número de teléfono"""
    if pd.isna(numero) or numero == "":
        return None
    numero = str(numero).replace(".0", "").strip()
    # Solo dígitos
    numero = ''.join(filter(str.isdigit, numero))
    if len(numero) not in [7, 8]:
        return None
    if len(set(numero)) == 1:  # 1111111, 2222222
        return None
    return numero

def tipo_telefono(numero):
    if numero.startswith("6") and len(numero) == 8:
        return "celular"
    elif not numero.startswith("0") and len(numero) == 7:
        return "fijo"
    return "otro"

def extraer_operador(correo):
    try:
        return correo.split("@")[1].split(".")[0]
    except:
        return None

def validar_cedula(cedula):
    """Valida cédula (ajusta según tu país)"""
    cedula = str(cedula).strip()
    if not cedula.isdigit():
        return None
    if len(cedula) not in [7, 8]:  # Ajusta según tu país
        return None
    return cedula

def validar_email(correo):
    """Valida formato de email correctamente"""
    if pd.isna(correo) or correo == "":
        return None
    correo = str(correo).lower().strip()
    # Regex robusto para email
    patron = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(patron, correo):
        return correo
    return None

def registrar_log(archivo, proyecto, filas_procesadas, estado):
    """Registra el log de carga en BigQuery"""
    try:
        log_data = [{
            "id_log": str(uuid.uuid4()),
            "archivo": archivo,
            "proyecto": proyecto,
            "filas_procesadas": filas_procesadas,
            "estado": estado,
            "fecha_carga": pd.Timestamp.utcnow()
        }]
        df_log = pd.DataFrame(log_data)
        
        # Crear tabla si no existe
        query_create = f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.log_cargas` (
            id_log STRING,
            archivo STRING,
            proyecto STRING,
            filas_procesadas INT64,
            estado STRING,
            fecha_carga TIMESTAMP
        )
        """
        client.query(query_create).result()
        
        # Insertar log
        table = f"{PROJECT_ID}.{DATASET}.log_cargas"
        client.load_table_from_dataframe(df_log, table).result()
    except Exception as e:
        st.warning(f"No se pudo registrar log: {e}")

# -----------------------
# PROCESAMIENTO
# -----------------------

if uploaded_file:

    df = pd.read_csv(uploaded_file, dtype=str).fillna("").apply(lambda x: x.str.strip())
    df["id_proyecto"] = id_proyecto

    st.write("Vista previa:", df.head())
    
    # Log inicial
    st.info(f"📄 Archivo: {uploaded_file.name}")
    st.info(f"📅 Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if st.button("🚀 Cargar datos"):
        
        try:
            # =====================
            # 1. CLIENTES (UPSERT con cédula validada)
            # =====================
            df_clientes = df[['id_cliente','nombre','cedula','genero','fecha_nac','direccion']].drop_duplicates('id_cliente')
            
            # Validar cédulas y REEMPLAZAR
            df_clientes['cedula_validada'] = df_clientes['cedula'].apply(validar_cedula)
            df_clientes = df_clientes[df_clientes['cedula_validada'].notna()]
            # 🔥 CORREGIDO: usar la cédula validada
            df_clientes['cedula'] = df_clientes['cedula_validada']
            df_clientes = df_clientes.drop(columns=['cedula_validada'])

            if df_clientes.empty:
                st.error("❌ No hay clientes con cédulas válidas")
                registrar_log(uploaded_file.name, id_proyecto, 0, "ERROR: Sin cédulas válidas")
                st.stop()

            table_temp = f"{PROJECT_ID}.{DATASET}.tmp_clientes"
            client.load_table_from_dataframe(df_clientes, table_temp).result()

            query_clientes = f"""
            MERGE `{PROJECT_ID}.{DATASET}.cliente` T
            USING `{table_temp}` S
            ON T.id_cliente = S.id_cliente
            WHEN MATCHED THEN UPDATE SET
              nombre = S.nombre,
              cedula = S.cedula,
              genero = S.genero,
              fecha_nac = S.fecha_nac,
              direccion = S.direccion
            WHEN NOT MATCHED THEN INSERT (
              id_cliente, nombre, cedula, genero, fecha_nac, direccion, estado, fecha_creacion
            )
            VALUES (
              S.id_cliente, S.nombre, S.cedula, S.genero, S.fecha_nac, S.direccion, 'Activo', CURRENT_TIMESTAMP()
            )
            """
            client.query(query_clientes).result()
            st.success(f"✅ Clientes procesados: {len(df_clientes)}")

            # =====================
            # 2. TELEFONOS (con ID determinístico)
            # =====================
            telefonos = []
            for _, row in df.iterrows():
                for i in range(1, 16):
                    num = limpiar_numero(row.get(f"telefono{i}", ""))
                    if num:
                        # ID determinístico con strip por seguridad
                        id_telefono = f"{row['id_cliente']}_{num.strip()}"
                        telefonos.append({
                            "id_telefono": id_telefono,
                            "id_cliente": row["id_cliente"],
                            "numero": num,
                            "tipo": tipo_telefono(num),
                            "estado": "Activo",
                            "prioridad": 1,
                            "fuente": uploaded_file.name,
                            "fecha_creacion": pd.Timestamp.utcnow()  # 🔥 UTC
                        })

            df_tel = pd.DataFrame(telefonos).drop_duplicates(subset=["id_cliente", "numero"])

            if not df_tel.empty:
                table_temp = f"{PROJECT_ID}.{DATASET}.tmp_tel"
                client.load_table_from_dataframe(df_tel, table_temp).result()

                query_tel = f"""
                MERGE `{PROJECT_ID}.{DATASET}.telefono` T
                USING `{table_temp}` S
                ON T.id_cliente = S.id_cliente AND T.numero = S.numero
                WHEN NOT MATCHED THEN INSERT ROW
                """
                client.query(query_tel).result()
                st.success(f"✅ {len(df_tel)} teléfonos procesados")
            else:
                st.warning("⚠️ No se encontraron teléfonos válidos")

            # =====================
            # 3. CORREOS (con validación robusta)
            # =====================
            correos = []
            for _, row in df.iterrows():
                for i in range(1, 6):
                    correo_raw = row.get(f"correo{i}", "")
                    correo_validado = validar_email(correo_raw)
                    if correo_validado:
                        id_correo = f"{row['id_cliente']}_{correo_validado}"
                        correos.append({
                            "id_correo": id_correo,
                            "id_cliente": row["id_cliente"],
                            "correo": correo_validado,
                            "operador": extraer_operador(correo_validado),
                            "estado": "Activo",
                            "prioridad": 1,
                            "fuente": uploaded_file.name,
                            "fecha_creacion": pd.Timestamp.utcnow()  # 🔥 UTC
                        })

            df_correo = pd.DataFrame(correos).drop_duplicates(subset=["id_cliente", "correo"])

            if not df_correo.empty:
                table_temp = f"{PROJECT_ID}.{DATASET}.tmp_correo"
                client.load_table_from_dataframe(df_correo, table_temp).result()

                query_correo = f"""
                MERGE `{PROJECT_ID}.{DATASET}.correo` T
                USING `{table_temp}` S
                ON T.id_cliente = S.id_cliente AND T.correo = S.correo
                WHEN NOT MATCHED THEN INSERT ROW
                """
                client.query(query_correo).result()
                st.success(f"✅ {len(df_correo)} correos procesados")

            # =====================
            # 4. CLIENTE_PROYECTO (ID determinístico)
            # =====================
            df_rel = df[['id_cliente', 'id_proyecto']].drop_duplicates()
            # ID determinístico (no UUID)
            df_rel["id_cliente_proyecto"] = df_rel.apply(
                lambda x: f"{x['id_cliente']}_{x['id_proyecto']}", axis=1
            )
            df_rel["estado"] = "En localizacion"
            df_rel["fecha_asignacion"] = pd.Timestamp.utcnow().date()  # 🔥 UTC
            df_rel["prioridad_inicial"] = 1

            table_temp = f"{PROJECT_ID}.{DATASET}.tmp_cp"
            client.load_table_from_dataframe(df_rel, table_temp).result()

            query_cp = f"""
            MERGE `{PROJECT_ID}.{DATASET}.cliente_proyecto` T
            USING `{table_temp}` S
            ON T.id_cliente = S.id_cliente AND T.id_proyecto = S.id_proyecto
            WHEN NOT MATCHED THEN INSERT ROW
            """
            client.query(query_cp).result()

            st.success("🎉 ¡Carga completada exitosamente!")

            # =====================
            # 5. LOG DE CARGA
            # =====================
            registrar_log(
                archivo=uploaded_file.name,
                proyecto=id_proyecto,
                filas_procesadas=len(df),
                estado="EXITOSO"
            )

            # Resumen final
            st.subheader("📊 Resumen de carga")
            st.write(f"- Proyecto: **{id_proyecto}**")
            st.write(f"- Clientes: **{len(df_clientes)}**")
            st.write(f"- Teléfonos: **{len(df_tel)}**")
            st.write(f"- Correos: **{len(df_correo)}**")
            st.write(f"- Relaciones: **{len(df_rel)}**")

        except Exception as e:
            st.error(f"❌ Error durante la carga: {e}")
            registrar_log(
                archivo=uploaded_file.name,
                proyecto=id_proyecto,
                filas_procesadas=0,
                estado=f"ERROR: {str(e)[:100]}"
            )

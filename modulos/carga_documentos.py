# modulos/carga_documentos.py
import streamlit as st
import pandas as pd
import uuid
import re
from google.cloud import bigquery
from datetime import datetime

def run(usuario, tipo_consulta):
    """Módulo de carga de clientes, teléfonos y correos"""
    
    st.header("🚀 Carga Inteligente de Clientes")
    st.info(f"👤 Usuario: {usuario} | Permiso: {tipo_consulta}")
    
    # =====================
    # CONEXIÓN A BIGQUERY (USANDO SECRETOS DE STREAMLIT)
    # =====================
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ No se encontró la configuración 'gcp_service_account' en los secretos")
            st.info("Por favor, configura los secretos en Streamlit Cloud")
            return
        
        service_account_info = dict(st.secrets["gcp_service_account"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        st.success("✅ Conectado a BigQuery")
        
        PROJECT_ID = "proyecto-css-panama"
        DATASET = "crm_core"
        
    except Exception as e:
        st.error(f"❌ Error de conexión a BigQuery: {e}")
        return
    
    # =====================
    # SELECCIÓN DE PROYECTO (MANUAL)
    # =====================
    lista_proyectos = [
        "VETPET001",
        "cobros_001", 
        "ventas_001"
    ]
    
    st.markdown("### 📋 Proyectos disponibles")
    for p in lista_proyectos:
        st.markdown(f"- `{p}`")
    
    id_proyecto = st.selectbox("Selecciona el proyecto", lista_proyectos)
    
    # =====================
    # SUBIR ARCHIVO
    # =====================
    uploaded_file = st.file_uploader(
        "Sube tu archivo CSV",
        type=["csv"],
        help="""
        📋 FORMATO ESPERADO DEL CSV:
        
        OBLIGATORIAS:
        • nombre - Nombre completo
        • cedula - Número de cédula (7-8 dígitos) → USA ESTO PARA IDENTIFICAR
        
        OPCIONALES:
        • genero - M o F
        • fecha_nac - Formato YYYY-MM-DD
        • direccion - Dirección
        
        TELÉFONOS (opcional, hasta 15):
        • telefono1, telefono2 ... telefono15
        
        CORREOS (opcional, hasta 5):
        • correo1, correo2 ... correo5
        
        🔑 El ID del cliente se genera AUTOMÁTICAMENTE (UUID)
        🔑 La identificación es por CÉDULA
        
        EJEMPLO:
        nombre,cedula,genero,fecha_nac,direccion,telefono1,correo1
        Juan Perez,12345678,M,1990-05-15,Calle 1,555-1000,juan@mail.com
        """
    )
    
    # =====================
    # FUNCIONES DE LIMPIEZA
    # =====================
    def limpiar_numero(numero):
        if pd.isna(numero) or numero == "":
            return None
        numero = str(numero).replace(".0", "").strip()
        numero = ''.join(filter(str.isdigit, numero))
        if len(numero) not in [7, 8]:
            return None
        if len(set(numero)) == 1:
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
        cedula = str(cedula).strip()
        if not cedula.isdigit():
            return None
        if len(cedula) not in [7, 8]:
            return None
        return cedula
    
    def validar_email(correo):
        if pd.isna(correo) or correo == "":
            return None
        correo = str(correo).lower().strip()
        patron = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(patron, correo):
            return correo
        return None
    
    def registrar_log(archivo, proyecto, filas_procesadas, estado):
        try:
            log_data = [{
                "id_log": str(uuid.uuid4()),
                "archivo": archivo,
                "proyecto": proyecto,
                "usuario": usuario,
                "filas_procesadas": filas_procesadas,
                "estado": estado,
                "fecha_carga": pd.Timestamp.utcnow()
            }]
            df_log = pd.DataFrame(log_data)
            
            query_create = f"""
            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.log_cargas` (
                id_log STRING,
                archivo STRING,
                proyecto STRING,
                usuario STRING,
                filas_procesadas INT64,
                estado STRING,
                fecha_carga TIMESTAMP
            )
            """
            client.query(query_create).result()
            
            table = f"{PROJECT_ID}.{DATASET}.log_cargas"
            client.load_table_from_dataframe(df_log, table).result()
        except Exception as e:
            st.warning(f"No se pudo registrar log: {e}")
    
    # =====================
    # PROCESAMIENTO
    # =====================
    if uploaded_file:
        
        df = pd.read_csv(uploaded_file, dtype=str).fillna("").apply(lambda x: x.str.strip())
        
        # Verificar columnas obligatorias
        if 'nombre' not in df.columns or 'cedula' not in df.columns:
            st.error("❌ El CSV debe tener las columnas 'nombre' y 'cedula'")
            st.info("Columnas encontradas: " + ", ".join(df.columns.tolist()))
            return
        
        st.write("### Vista previa del archivo")
        st.dataframe(df.head())
        
        st.info(f"📄 Archivo: {uploaded_file.name}")
        st.info(f"📅 Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Verificar permisos
        permite_telefonos = tipo_consulta in ["CSS", "TELÉFONOS NUEVOS"]
        permite_correos = tipo_consulta in ["CSS", "CORREOS NUEVOS"]
        
        if not permite_telefonos and not permite_correos:
            st.error("❌ No tienes permisos para cargar teléfonos o correos")
            return
        
        if st.button("🚀 Cargar datos", type="primary"):
            
            try:
                # =====================
                # 1. OBTENER CLIENTES EXISTENTES POR CÉDULA
                # =====================
                query_existentes = f"""
                SELECT id_cliente, cedula 
                FROM `{PROJECT_ID}.{DATASET}.cliente`
                """
                df_existentes = client.query(query_existentes).to_dataframe()
                mapa_cedula_a_id = dict(zip(df_existentes['cedula'], df_existentes['id_cliente']))
                
                # =====================
                # 2. PROCESAR CLIENTES (INSERT/UPDATE)
                # =====================
                # Obtener clientes únicos por cédula
                df_clientes_unicos = df[['nombre','cedula','genero','fecha_nac','direccion']].drop_duplicates('cedula')
                
                # Validar cédulas
                df_clientes_unicos['cedula_validada'] = df_clientes_unicos['cedula'].apply(validar_cedula)
                df_clientes_unicos = df_clientes_unicos[df_clientes_unicos['cedula_validada'].notna()]
                df_clientes_unicos['cedula'] = df_clientes_unicos['cedula_validada']
                df_clientes_unicos = df_clientes_unicos.drop(columns=['cedula_validada'])
                
                if df_clientes_unicos.empty:
                    st.error("❌ No hay clientes con cédulas válidas")
                    registrar_log(uploaded_file.name, id_proyecto, 0, "ERROR: Sin cédulas válidas")
                    return
                
                # Preparar listas para insertar y actualizar
                clientes_para_insertar = []
                clientes_para_actualizar = []
                mapa_cedula_a_id_nuevo = {}  # Para usar después en teléfonos/correos
                
                for _, row in df_clientes_unicos.iterrows():
                    cedula = row['cedula']
                    nombre = row['nombre']
                    genero = row.get('genero', '')
                    fecha_nac = row.get('fecha_nac', None)
                    direccion = row.get('direccion', '')
                    
                    if fecha_nac == "":
                        fecha_nac = None
                    
                    if cedula in mapa_cedula_a_id:
                        # Cliente existe → actualizar
                        id_cliente = mapa_cedula_a_id[cedula]
                        clientes_para_actualizar.append({
                            "id_cliente": id_cliente,
                            "nombre": nombre,
                            "cedula": cedula,
                            "genero": genero,
                            "fecha_nac": fecha_nac,
                            "direccion": direccion
                        })
                        mapa_cedula_a_id_nuevo[cedula] = id_cliente
                    else:
                        # Cliente nuevo → crear UUID
                        id_cliente = str(uuid.uuid4())
                        clientes_para_insertar.append({
                            "id_cliente": id_cliente,
                            "nombre": nombre,
                            "cedula": cedula,
                            "genero": genero,
                            "fecha_nac": fecha_nac,
                            "direccion": direccion,
                            "estado": "Activo",
                            "fecha_creacion": pd.Timestamp.utcnow().date()
                        })
                        mapa_cedula_a_id_nuevo[cedula] = id_cliente
                
                # Insertar nuevos clientes
                if clientes_para_insertar:
                    df_insert = pd.DataFrame(clientes_para_insertar)
                    job = client.load_table_from_dataframe(df_insert, f"{PROJECT_ID}.{DATASET}.cliente")
                    st.success(f"✅ Clientes nuevos: {job.result().output_rows}")
                
                # Actualizar clientes existentes
                if clientes_para_actualizar:
                    for cliente in clientes_para_actualizar:
                        query_update = f"""
                        UPDATE `{PROJECT_ID}.{DATASET}.cliente`
                        SET 
                            nombre = '{cliente['nombre'].replace("'", "''")}',
                            genero = '{cliente['genero']}',
                            fecha_nac = {f"DATE('{cliente['fecha_nac']}')" if cliente['fecha_nac'] else 'NULL'},
                            direccion = '{cliente['direccion'].replace("'", "''")}'
                        WHERE id_cliente = '{cliente['id_cliente']}'
                        """
                        client.query(query_update).result()
                    st.success(f"✅ Clientes actualizados: {len(clientes_para_actualizar)}")
                
                # Agregar columna id_cliente al DataFrame original para usar en teléfonos/correos
                df['id_cliente'] = df['cedula'].map(mapa_cedula_a_id_nuevo)
                
                # Eliminar filas sin id_cliente (cédulas inválidas)
                df = df[df['id_cliente'].notna()]
                
                if df.empty:
                    st.warning("⚠️ No hay clientes válidos para procesar teléfonos/correos")
                else:
                    
                    # =====================
                    # 3. TELÉFONOS
                    # =====================
                    df_tel = pd.DataFrame()
                    if permite_telefonos:
                        telefonos = []
                        for _, row in df.iterrows():
                            for i in range(1, 16):
                                num = limpiar_numero(row.get(f"telefono{i}", ""))
                                if num:
                                    id_telefono = f"{row['id_cliente']}_{num.strip()}"
                                    telefonos.append({
                                        "id_telefono": id_telefono,
                                        "id_cliente": row["id_cliente"],
                                        "numero": num,
                                        "tipo": tipo_telefono(num),
                                        "estado": "Activo",
                                        "prioridad": 1,
                                        "fuente": uploaded_file.name,
                                        "fecha_creacion": pd.Timestamp.utcnow().date()
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
                    else:
                        st.info("ℹ️ Sin permiso para cargar teléfonos")
                    
                    # =====================
                    # 4. CORREOS
                    # =====================
                    df_correo = pd.DataFrame()
                    if permite_correos:
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
                                        "fecha_creacion": pd.Timestamp.utcnow().date()
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
                        else:
                            st.warning("⚠️ No se encontraron correos válidos")
                    else:
                        st.info("ℹ️ Sin permiso para cargar correos")
                
                # =====================
                # 5. CLIENTE_PROYECTO (relación)
                # =====================
                # Obtener relaciones únicas
                df_rel = df[['id_cliente', 'id_proyecto']].drop_duplicates()
                df_rel['id_proyecto'] = id_proyecto
                df_rel["id_cliente_proyecto"] = df_rel.apply(
                    lambda x: f"{x['id_cliente']}_{x['id_proyecto']}", axis=1
                )
                df_rel["estado"] = "En localizacion"
                df_rel["fecha_asignacion"] = pd.Timestamp.utcnow().date()
                df_rel["prioridad_inicial"] = 1
                
                # Eliminar posibles NaN
                df_rel = df_rel[df_rel['id_cliente'].notna()]
                
                if not df_rel.empty:
                    table_temp = f"{PROJECT_ID}.{DATASET}.tmp_cp"
                    client.load_table_from_dataframe(df_rel, table_temp).result()
                    
                    query_cp = f"""
                    MERGE `{PROJECT_ID}.{DATASET}.cliente_proyecto` T
                    USING `{table_temp}` S
                    ON T.id_cliente = S.id_cliente AND T.id_proyecto = S.id_proyecto
                    WHEN NOT MATCHED THEN INSERT ROW
                    """
                    client.query(query_cp).result()
                    st.success(f"✅ {len(df_rel)} relaciones cliente-proyecto procesadas")
                else:
                    st.warning("⚠️ No se generaron relaciones cliente-proyecto")
                
                # =====================
                # 6. LOG DE CARGA EXITOSA
                # =====================
                registrar_log(
                    archivo=uploaded_file.name,
                    proyecto=id_proyecto,
                    filas_procesadas=len(df),
                    estado="EXITOSO"
                )
                
                # =====================
                # 7. RESUMEN FINAL
                # =====================
                st.success("🎉 ¡Carga completada exitosamente!")
                
                st.subheader("📊 Resumen de carga")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Clientes nuevos", len(clientes_para_insertar))
                with col2:
                    st.metric("Clientes actualizados", len(clientes_para_actualizar))
                with col3:
                    st.metric("Teléfonos", len(df_tel) if permite_telefonos and 'df_tel' in locals() else 0)
                with col4:
                    st.metric("Correos", len(df_correo) if permite_correos and 'df_correo' in locals() else 0)
                
                st.info(f"🔗 Relaciones cliente-proyecto: {len(df_rel) if 'df_rel' in locals() else 0}")
                
            except Exception as e:
                st.error(f"❌ Error durante la carga: {e}")
                registrar_log(
                    archivo=uploaded_file.name,
                    proyecto=id_proyecto,
                    filas_procesadas=0,
                    estado=f"ERROR: {str(e)[:100]}"
                )
                st.exception(e)  # Muestra el error detallado para debugging

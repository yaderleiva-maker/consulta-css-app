# modulos/carga_documentos.py
import streamlit as st
import pandas as pd
import uuid
import re
import hashlib
from google.cloud import bigquery
from datetime import datetime

def run(usuario, tipo_consulta):
    """Módulo de carga de clientes, teléfonos y correos"""
    
    st.header("🚀 Carga Inteligente de Clientes")
    st.info(f"👤 Usuario: {usuario} | Permiso: {tipo_consulta}")
    
    # =====================
    # CONEXIÓN A BIGQUERY
    # =====================
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ No se encontró la configuración 'gcp_service_account' en los secretos")
            return
        
        service_account_info = dict(st.secrets["gcp_service_account"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        st.success("✅ Conectado a BigQuery")
        
        PROJECT_ID = "proyecto-css-panama"
        DATASET = "crm_core"
        
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return
    
    # =====================
    # SELECCIÓN DE PROYECTO
    # =====================
    lista_proyectos = [
        "VETPET001",
        "generico"
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
        FORMATO ESPERADO:
        OBLIGATORIAS: nombre, cedula
        OPCIONALES: genero, fecha_nac, direccion, telefono1...telefono15, correo1...correo5
        El separador se detecta automáticamente
        """
    )
    
    # =====================
    # FUNCIONES
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
        if pd.isna(cedula) or cedula == "":
            return None
        cedula = str(cedula).strip()
        if len(cedula) < 2:
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
    
    def normalizar_fecha(fecha_str):
        if pd.isna(fecha_str) or fecha_str == "":
            return None
        fecha_str = str(fecha_str).strip()
        formatos = [
            "%d/%m/%Y", "%d/%m/%y", "%m/%d/%Y", "%m/%d/%y",
            "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%Y/%m/%d"
        ]
        for fmt in formatos:
            try:
                fecha_obj = datetime.strptime(fecha_str, fmt)
                return fecha_obj.date()
            except ValueError:
                continue
        return None
    
    def registrar_log(archivo, proyecto, filas_procesadas, estado):
        try:
            log_data = [{
                "id_log": str(uuid.uuid4()),
                "archivo": archivo,
                "proyecto": proyecto,
                "usuario": usuario,
                "filas_procesadas": filas_procesadas,
                "estado": estado
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
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
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
        
        # Autodetección de separador
        try:
            contenido = uploaded_file.getvalue().decode('utf-8')
            primeras_lineas = contenido.split('\n')[:5]
            
            separadores = [',', ';', '\t', '|']
            separador_detectado = ','
            max_cols = 0
            
            for sep in separadores:
                for linea in primeras_lineas:
                    if linea.count(sep) > max_cols:
                        max_cols = linea.count(sep)
                        separador_detectado = sep
            
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, sep=separador_detectado, engine='python')
            df = df.fillna("").apply(lambda x: x.str.strip())
            
            nombre_sep = {
                ',': 'COMA (,)',
                ';': 'PUNTO Y COMA (;)',
                '\t': 'TAB (\\t)',
                '|': 'BARRA VERTICAL (|)'
            }.get(separador_detectado, f"'{separador_detectado}'")
            
            st.info(f"📌 Separador detectado: {nombre_sep}")
            
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {e}")
            return
        
        df.columns = df.columns.str.strip().str.replace('^\ufeff', '', regex=True)
        
        if 'nombre' not in df.columns or 'cedula' not in df.columns:
            st.error(f"❌ El CSV debe tener 'nombre' y 'cedula'")
            st.info(f"Columnas: {', '.join(df.columns.tolist())}")
            return
        
        st.write("### Vista previa")
        st.dataframe(df.head())
        st.info(f"📄 Archivo: {uploaded_file.name} | 📊 Filas: {len(df)}")
        
        permite_telefonos = tipo_consulta in ["CSS", "TELÉFONOS NUEVOS"]
        permite_correos = tipo_consulta in ["CSS", "CORREOS NUEVOS"]
        
        if not permite_telefonos and not permite_correos:
            st.error("❌ No tienes permisos para cargar teléfonos o correos")
            return
        
        if st.button("🚀 Cargar datos", type="primary"):
            
            try:
                # =====================
                # 1. CLIENTES EXISTENTES
                # =====================
                query_existentes = f"""
                SELECT id_cliente, cedula 
                FROM `{PROJECT_ID}.{DATASET}.cliente`
                """
                df_existentes = client.query(query_existentes).to_dataframe()
                mapa_cedula_a_id = dict(zip(df_existentes['cedula'], df_existentes['id_cliente']))
                
                # =====================
                # 2. PROCESAR CLIENTES
                # =====================
                df_clientes = df[['nombre','cedula','genero','fecha_nac','direccion']].drop_duplicates('cedula')
                
                # Normalizar fechas
                if 'fecha_nac' in df_clientes.columns:
                    df_clientes['fecha_nac'] = df_clientes['fecha_nac'].apply(normalizar_fecha)
                
                # Validar cédulas
                df_clientes['cedula_validada'] = df_clientes['cedula'].apply(validar_cedula)
                df_clientes = df_clientes[df_clientes['cedula_validada'].notna()]
                df_clientes['cedula'] = df_clientes['cedula_validada']
                df_clientes = df_clientes.drop(columns=['cedula_validada'])
                
                if df_clientes.empty:
                    st.error("❌ No hay identificaciones válidas")
                    registrar_log(uploaded_file.name, id_proyecto, 0, "ERROR: Sin identificaciones")
                    return
                
                clientes_insertar = []
                clientes_actualizar = []
                mapa_cedula_id = {}
                
                for _, row in df_clientes.iterrows():
                    cedula = row['cedula']
                    if cedula in mapa_cedula_a_id:
                        id_cliente = mapa_cedula_a_id[cedula]
                        clientes_actualizar.append({
                            "id_cliente": id_cliente,
                            "nombre": row['nombre'],
                            "cedula": cedula,
                            "genero": row.get('genero', ''),
                            "fecha_nac": row.get('fecha_nac', None),
                            "direccion": row.get('direccion', '')
                        })
                        mapa_cedula_id[cedula] = id_cliente
                    else:
                        id_cliente = str(uuid.uuid4())
                        clientes_insertar.append({
                            "id_cliente": id_cliente,
                            "nombre": row['nombre'],
                            "cedula": cedula,
                            "genero": row.get('genero', ''),
                            "fecha_nac": row.get('fecha_nac', None),
                            "direccion": row.get('direccion', ''),
                            "estado": "Activo"
                        })
                        mapa_cedula_id[cedula] = id_cliente
                
                # =====================
                # 3. INSERTAR NUEVOS CLIENTES (MASIVO)
                # =====================
                if clientes_insertar:
                    df_insert = pd.DataFrame(clientes_insertar)
                    
                    # Convertir fecha_nac a date si existe
                    if 'fecha_nac' in df_insert.columns:
                        df_insert['fecha_nac'] = pd.to_datetime(df_insert['fecha_nac'], errors='coerce')
                        df_insert['fecha_nac'] = df_insert['fecha_nac'].dt.date
                    
                    temp_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
                    table_temp = f"{PROJECT_ID}.{DATASET}.tmp_clientes_insert_{temp_suffix}"
                    
                    client.load_table_from_dataframe(df_insert, table_temp).result()
                    
                    query_merge_insert = f"""
                    MERGE `{PROJECT_ID}.{DATASET}.cliente` T
                    USING `{table_temp}` S
                    ON T.id_cliente = S.id_cliente
                    WHEN NOT MATCHED THEN INSERT (
                      id_cliente, nombre, cedula, genero, fecha_nac, direccion, estado, fecha_creacion
                    ) VALUES (
                      S.id_cliente, S.nombre, S.cedula, S.genero, S.fecha_nac, S.direccion, S.estado, CURRENT_TIMESTAMP()
                    )
                    """
                    client.query(query_merge_insert).result()
                    
                    client.delete_table(table_temp)
                    st.success(f"✅ Clientes nuevos: {len(clientes_insertar)}")
                
                # =====================
                # 4. ACTUALIZAR CLIENTES EXISTENTES (MASIVO - 1 SOLA QUERY)
                # =====================
                if clientes_actualizar:
                    df_update = pd.DataFrame(clientes_actualizar)
                    
                    # Convertir fecha_nac a date si existe
                    if 'fecha_nac' in df_update.columns:
                        df_update['fecha_nac'] = pd.to_datetime(df_update['fecha_nac'], errors='coerce')
                        df_update['fecha_nac'] = df_update['fecha_nac'].dt.date
                    
                    temp_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
                    table_temp = f"{PROJECT_ID}.{DATASET}.tmp_clientes_update_{temp_suffix}"
                    
                    client.load_table_from_dataframe(df_update, table_temp).result()
                    
                    query_merge_update = f"""
                    MERGE `{PROJECT_ID}.{DATASET}.cliente` T
                    USING `{table_temp}` S
                    ON T.id_cliente = S.id_cliente
                    WHEN MATCHED THEN UPDATE SET
                      T.nombre = S.nombre,
                      T.cedula = S.cedula,
                      T.genero = S.genero,
                      T.fecha_nac = S.fecha_nac,
                      T.direccion = S.direccion,
                      T.estado = 'Activo'
                    """
                    client.query(query_merge_update).result()
                    
                    client.delete_table(table_temp)
                    st.success(f"✅ Clientes actualizados: {len(clientes_actualizar)}")
                
                # Agregar columna id_cliente al DataFrame original
                df['id_cliente'] = df['cedula'].map(mapa_cedula_id)
                df = df[df['id_cliente'].notna()]
                
                # =====================
                # 5. TELÉFONOS (MASIVO)
                # =====================
                df_tel = pd.DataFrame()
                if permite_telefonos and not df.empty:
                    telefonos = []
                    for _, row in df.iterrows():
                        for i in range(1, 16):
                            num = limpiar_numero(row.get(f"telefono{i}", ""))
                            if num:
                                telefonos.append({
                                    "id_telefono": f"{row['id_cliente']}_{num}",
                                    "id_cliente": row["id_cliente"],
                                    "numero": num,
                                    "tipo": tipo_telefono(num),
                                    "estado": "Activo",
                                    "prioridad": 1,
                                    "fuente": uploaded_file.name
                                })
                    
                    df_tel = pd.DataFrame(telefonos).drop_duplicates(subset=["id_cliente", "numero"])
                    if not df_tel.empty:
                        temp_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
                        table = f"{PROJECT_ID}.{DATASET}.tmp_tel_{temp_suffix}"
                        client.load_table_from_dataframe(df_tel, table).result()
                        
                        query_tel = f"""
                        MERGE `{PROJECT_ID}.{DATASET}.telefonos` T
                        USING `{table}` S
                        ON T.id_cliente = S.id_cliente AND T.numero = S.numero
                        WHEN NOT MATCHED THEN INSERT (
                          id_telefono, id_cliente, numero, tipo, estado, prioridad, fuente, fecha_creacion
                        ) VALUES (
                          S.id_telefono, S.id_cliente, S.numero, S.tipo, S.estado, S.prioridad, S.fuente, CURRENT_TIMESTAMP()
                        )
                        """
                        client.query(query_tel).result()
                        client.delete_table(table)
                        st.success(f"✅ {len(df_tel)} teléfonos")
                
                # =====================
                # 6. CORREOS (MASIVO)
                # =====================
                df_correo = pd.DataFrame()
                if permite_correos and not df.empty:
                    correos = []
                    for _, row in df.iterrows():
                        for i in range(1, 6):
                            email = validar_email(row.get(f"correo{i}", ""))
                            if email:
                                correos.append({
                                    "id_correo": f"{row['id_cliente']}_{email}",
                                    "id_cliente": row["id_cliente"],
                                    "correo": email,
                                    "operador": extraer_operador(email),
                                    "estado": "Activo",
                                    "prioridad": 1,
                                    "fuente": uploaded_file.name
                                })
                    
                    df_correo = pd.DataFrame(correos).drop_duplicates(subset=["id_cliente", "correo"])
                    if not df_correo.empty:
                        temp_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
                        table = f"{PROJECT_ID}.{DATASET}.tmp_correo_{temp_suffix}"
                        client.load_table_from_dataframe(df_correo, table).result()
                        
                        query_correo = f"""
                        MERGE `{PROJECT_ID}.{DATASET}.correo` T
                        USING `{table}` S
                        ON T.id_cliente = S.id_cliente AND T.correo = S.correo
                        WHEN NOT MATCHED THEN INSERT (
                          id_correo, id_cliente, correo, operador, estado, prioridad, fuente, fecha_creacion
                        ) VALUES (
                          S.id_correo, S.id_cliente, S.correo, S.operador, S.estado, S.prioridad, S.fuente, CURRENT_TIMESTAMP()
                        )
                        """
                        client.query(query_correo).result()
                        client.delete_table(table)
                        st.success(f"✅ {len(df_correo)} correos")
                
                # =====================
                # 7. RELACIÓN CLIENTE_PROYECTO (MASIVO)
                # =====================
                if not df.empty:
                    df_rel = df[['id_cliente']].drop_duplicates()
                    df_rel['id_proyecto'] = id_proyecto
                    df_rel['id_cliente_proyecto'] = df_rel.apply(lambda x: f"{x['id_cliente']}_{x['id_proyecto']}", axis=1)
                    df_rel['estado'] = "En localizacion"
                    
                    temp_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
                    table = f"{PROJECT_ID}.{DATASET}.tmp_cp_{temp_suffix}"
                    client.load_table_from_dataframe(df_rel, table).result()
                    
                    query_cp = f"""
                    MERGE `{PROJECT_ID}.{DATASET}.cliente_proyecto` T
                    USING `{table}` S
                    ON T.id_cliente = S.id_cliente AND T.id_proyecto = S.id_proyecto
                    WHEN NOT MATCHED THEN INSERT (
                      id_cliente_proyecto, id_cliente, id_proyecto, estado, fecha_asignacion
                    ) VALUES (
                      S.id_cliente_proyecto, S.id_cliente, S.id_proyecto, S.estado, CURRENT_DATE()
                    )
                    """
                    client.query(query_cp).result()
                    client.delete_table(table)
                    st.success(f"✅ {len(df_rel)} relaciones")
                
                # =====================
                # 8. LOG
                # =====================
                registrar_log(uploaded_file.name, id_proyecto, len(df), "EXITOSO")
                
                # =====================
                # 9. RESUMEN
                # =====================
                st.success("🎉 ¡Carga completada!")
                st.subheader("📊 Resumen")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Nuevos", len(clientes_insertar))
                with col2:
                    st.metric("Actualizados", len(clientes_actualizar))
                with col3:
                    st.metric("Teléfonos", len(df_tel) if not df_tel.empty else 0)
                with col4:
                    st.metric("Correos", len(df_correo) if not df_correo.empty else 0)
                
            except Exception as e:
                st.error(f"❌ Error: {e}")
                registrar_log(uploaded_file.name, id_proyecto, 0, f"ERROR: {str(e)[:100]}")
                st.exception(e)

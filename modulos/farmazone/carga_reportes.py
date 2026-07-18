# modulos/farmazone/carga_reportes.py
import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.cloud import bigquery

def run(usuario):
    """Módulo principal de Farmazone - Reporte de Ventas y Compras"""
    
    st.title("💊 Farmazone - Reporte de Ventas y Compras")
    st.caption(f"👤 Usuario: {usuario}")
    st.markdown("---")
    
    # =====================
    # CONEXIÓN A BIGQUERY
    # =====================
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ No se encontró configuración en secretos")
            return
        
        service_account_info = dict(st.secrets["gcp_service_account"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        
        PROJECT_ID = "proyecto-css-panama"
        DATASET = "farmazone"
        TABLE_VENTAS = "farmazone_ventas_historico"
        TABLE_COMPRAS = "farmazone_compras_historico"
        TABLE_INVENTARIO = "inventario_actual"
        
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return
    
    # =====================
    # TAB (VENTAS / COMPRAS / UTILIDAD / INVENTARIO)
    # =====================
    tab_ventas, tab_compras, tab_utilidad, tab_inventario = st.tabs([
        "📊 Ventas", 
        "📦 Compras", 
        "💰 Utilidad Diaria",
        "📦 Inventario"
    ])
    
    # =====================
    # FUNCIONES COMPARTIDAS
    # =====================
    def limpiar_valor(valor):
        if pd.isna(valor) or valor == "" or valor == "-":
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)
        valor_str = str(valor).strip().replace(',', '.')
        valor_str = ''.join(c for c in valor_str if c.isdigit() or c == '.' or c == '-')
        if valor_str == "" or valor_str == "-":
            return 0.0
        try:
            return float(valor_str)
        except:
            return 0.0
    
    def limpiar_texto(valor):
        if pd.isna(valor) or valor == "":
            return ""
        return str(valor).strip()
    
    def leer_excel(archivo, skiprows=0):
        nombre = archivo.name.lower()
        if nombre.endswith('.csv'):
            return pd.read_csv(archivo, dtype=str, skiprows=skiprows)
        elif nombre.endswith('.xls'):
            return pd.read_excel(archivo, dtype=str, skiprows=skiprows, header=0, engine='xlrd')
        elif nombre.endswith('.xlsx'):
            return pd.read_excel(archivo, dtype=str, skiprows=skiprows, header=0, engine='openpyxl')
        else:
            raise ValueError(f"Formato no soportado: {nombre}")
    
    def leer_ventas(archivo):
        """Lee archivo de ventas con estructura correcta (skiprows=4)."""
        nombre = archivo.name.lower()
        
        # 🔥 USAR skiprows=4 para saltar las filas de título
        if nombre.endswith('.csv'):
            df = pd.read_csv(archivo, dtype=str, skiprows=4)
        elif nombre.endswith('.xls'):
            df = pd.read_excel(archivo, dtype=str, skiprows=4, header=0, engine='xlrd')
        elif nombre.endswith('.xlsx'):
            df = pd.read_excel(archivo, dtype=str, skiprows=4, header=0, engine='openpyxl')
        else:
            raise ValueError(f"Formato no soportado: {nombre}")
        
        # 🔥 NORMALIZAR NOMBRES DE COLUMNAS
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace("ñ", "n", regex=False)
            .str.replace("á", "a", regex=False)
            .str.replace("é", "e", regex=False)
            .str.replace("í", "i", regex=False)
            .str.replace("ó", "o", regex=False)
            .str.replace("ú", "u", regex=False)
        )
        
        # 🔥 RENOMBRAR 'no_de_factura' a 'no_factura'
        if 'no_de_factura' in df.columns:
            df = df.rename(columns={'no_de_factura': 'no_factura'})
        
        return df
    
    def leer_compras(archivo):
        """Lee archivo de compras (sin encabezados, desde la fila 5)"""
        nombre = archivo.name.lower()
        
        columnas = [
            'Compra', 'Referencia', 'Proveedor', 'Fecha', 'Payment Type',
            'Bodega', 'Status Proveedor', 'Codigo', 'Status Compra',
            'Descripcion', 'Cuenta', 'Centro de Costo', 'Unidades',
            'UoM Compra', 'UoM Unidades', 'UoM Instock', 'Currency',
            'Factor', 'Impuesto', 'Total de Linea'
        ]
        
        if nombre.endswith('.csv'):
            df = pd.read_csv(archivo, dtype=str, skiprows=5, header=None)
        elif nombre.endswith('.xls'):
            df = pd.read_excel(archivo, dtype=str, skiprows=5, header=None, engine='xlrd')
        elif nombre.endswith('.xlsx'):
            df = pd.read_excel(archivo, dtype=str, skiprows=5, header=None, engine='openpyxl')
        else:
            raise ValueError(f"Formato no soportado: {nombre}")
        
        num_columnas = len(df.columns)
        df.columns = columnas[:num_columnas]
        
        return df
    
    def cargar_claves_existentes(tabla):
        try:
            query = f"""
            SELECT DISTINCT clave_unica 
            FROM `{PROJECT_ID}.{DATASET}.{tabla}`
            WHERE activo = TRUE
            """
            df = client.query(query).to_dataframe()
            return set(df['clave_unica'].tolist())
        except Exception:
            return set()
    
    def limpiar_numero_inventario(valor):
        if pd.isna(valor) or valor == "" or valor == "-":
            return 0
        if isinstance(valor, (int, float)):
            return float(valor)
        try:
            valor_str = str(valor).strip().replace(',', '.')
            valor_str = ''.join(c for c in valor_str if c.isdigit() or c == '.' or c == '-')
            if valor_str == "" or valor_str == "-":
                return 0
            return float(valor_str)
        except:
            return 0
    
    def guardar_inventario(df_inventario: pd.DataFrame) -> int:
        """Guarda el inventario en BigQuery sobrescribiendo la tabla."""
        df_inventario['fecha_snapshot'] = datetime.now()
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df_inventario, table_id, job_config=job_config)
        job.result()
        return len(df_inventario)
    
    # =====================
    # TAB 1: VENTAS (CORREGIDO)
    # =====================
    with tab_ventas:
        st.subheader("📊 Carga de Ventas")
        
        col1, col2 = st.columns(2)
        with col1:
            archivo_ventas = st.file_uploader(
                "Sube el archivo de ventas (CSV o Excel)",
                type=["xlsx", "xls", "csv"],
                key="farmazone_ventas"
            )
        with col2:
            archivo_inventario = st.file_uploader(
                "Sube el archivo de inventario (CSV o Excel)",
                type=["xlsx", "xls", "csv"],
                key="farmazone_inventario"
            )
        
        if archivo_ventas and archivo_inventario:
            if st.button("🚀 Procesar Ventas", key="btn_ventas"):
                with st.spinner("Procesando ventas..."):
                    try:
                        # Leer archivos
                        df_ventas = leer_ventas(archivo_ventas)
                        df_inventario = leer_excel(archivo_inventario, skiprows=4)
                        
                        # 🔥 DIAGNÓSTICO: Mostrar columnas
                        st.write("📋 Columnas en ventas:", list(df_ventas.columns))
                        
                        # 🔥 CREAR DICCIONARIO DE INVENTARIO POR Id
                        dict_inventario = {}
                        for _, row in df_inventario.iterrows():
                            id_producto = limpiar_texto(row.get('Id', ''))
                            if id_producto:
                                dict_inventario[id_producto] = {
                                    'Ultimo_Costo_Unitario': limpiar_valor(row.get('Ultimo Costo Unitario', 0)),
                                    'Categoria_L1': limpiar_texto(row.get('Categoria L1', ''))
                                }
                        
                        st.info(f"📦 Inventario cargado: {len(dict_inventario)} productos")
                        
                        claves_existentes = cargar_claves_existentes(TABLE_VENTAS)
                        id_carga = str(uuid.uuid4())
                        registros_nuevos = []
                        duplicados = 0
                        sin_codigo = 0
                        
                        for _, row in df_ventas.iterrows():
                            no_factura = limpiar_texto(row.get('no_factura', ''))
                            
                            # 🔥 PRIORIDAD: Usar 'codigo'
                            codigo = limpiar_texto(row.get('codigo', ''))
                            
                            # 🔥 RESPALDO: item_number
                            if not codigo:
                                codigo = limpiar_texto(row.get('item_number', ''))
                            
                            # 🔥 ÚLTIMO RESPALDO: upc
                            if not codigo:
                                codigo = limpiar_texto(row.get('upc', ''))
                            
                            if not no_factura or not codigo:
                                sin_codigo += 1
                                continue
                            
                            clave = f"{no_factura}|{codigo}"
                            if clave in claves_existentes:
                                duplicados += 1
                                continue
                            
                            # 🔥 BUSCAR EN INVENTARIO POR codigo
                            datos_inv = dict_inventario.get(codigo, {})
                            categoria = datos_inv.get('Categoria_L1', '')
                            
                            unidades = limpiar_valor(row.get('unidades', 0))
                            precio_unitario = limpiar_valor(row.get('precio_unitario', 0))
                            precio_compra_orig = limpiar_valor(row.get('ult_precio_compra_original', 0))
                            
                            totalxcompra = unidades * precio_unitario
                            precio_compra_corr = datos_inv.get('Ultimo_Costo_Unitario', 0) if precio_compra_orig <= 0 else precio_compra_orig
                            total_costo = precio_compra_corr * unidades
                            utilidad = totalxcompra - total_costo
                            pct_utilidad = (utilidad / totalxcompra * 100) if totalxcompra > 0 else 0
                            
                            upc = limpiar_texto(row.get('upc', ''))
                            if not upc:
                                upc = limpiar_texto(row.get('item_number', ''))
                            
                            # 🔥 CALCULAR ano_mes_factura DESDE fecha_factura
                            fecha_factura = pd.to_datetime(row.get('fecha_factura', None), errors='coerce')
                            ano_mes_factura = fecha_factura.strftime('%Y%m') if pd.notna(fecha_factura) else ''
                            
                            registros_nuevos.append({
                                'id_registro': str(uuid.uuid4()),
                                'id_carga': id_carga,
                                'fecha_carga_lote': datetime.now(),
                                'clave_unica': clave,
                                'fecha_proceso': datetime.now(),
                                'usuario_proceso': usuario,
                                'archivo_origen': archivo_ventas.name,
                                'no_factura': no_factura,
                                'id_fiscal': limpiar_texto(row.get('id_fiscal', '')),
                                'documento': limpiar_texto(row.get('documento', '')),
                                'ano_mes_factura': ano_mes_factura,
                                'fecha_factura': fecha_factura,
                                'nombre_cliente': limpiar_texto(row.get('nombre_cliente', '')),
                                'pais': limpiar_texto(row.get('pais', '')),
                                'caja': limpiar_texto(row.get('caja', '')),
                                'vendedor': limpiar_texto(row.get('vendedor', '')),
                                'codigo': codigo,
                                'upc': upc,
                                'lote': limpiar_texto(row.get('lote', '')),
                                'arancel': limpiar_texto(row.get('arancel', '')),
                                'producto': limpiar_texto(row.get('producto', '')),
                                'marca': limpiar_texto(row.get('marca', '')),
                                'industria': limpiar_texto(row.get('industria', '')),
                                'proveedor_id': limpiar_texto(row.get('proveedor_id', '')),
                                'proveedor_nombre': limpiar_texto(row.get('proveedor_nombre', '')),
                                'categoria_l1': categoria,
                                'categoria_l2': limpiar_texto(row.get('categoria_l2', '')),
                                'categoria_l3': limpiar_texto(row.get('categoria_l3', '')),
                                'unidades': unidades,
                                'precio_unitario': precio_unitario,
                                'total_factura': limpiar_valor(row.get('total_factura', 0)),
                                'subtotal': limpiar_valor(row.get('subtotal', 0)),
                                'por_desc_linea': limpiar_valor(row.get('por_desc_linea', 0)),
                                'des_por_linea': limpiar_valor(row.get('des_por_linea', 0)),
                                'total_linea': limpiar_valor(row.get('total_linea', 0)),
                                'itbms': limpiar_valor(row.get('itbms', 0)),
                                'ult_precio_compra_original': precio_compra_orig,
                                'ult_precio_compra': precio_compra_corr,
                                'total_costo': total_costo,
                                'utilidad': utilidad,
                                'porcentaje_utilidad': pct_utilidad,
                                'margen_utilidad': limpiar_texto(row.get('margen_utilidad', '')),
                                'ultimo_costo_unitario': datos_inv.get('Ultimo_Costo_Unitario', 0),
                                'costo_promedio': limpiar_valor(row.get('costo_promedio', 0)),
                                'precio_lista': limpiar_valor(row.get('precio_lista', 0)),
                                'activo': True,
                                'fecha_actualizacion': datetime.now(),
                                'usuario_actualizacion': usuario,
                                'bodega': limpiar_texto(row.get('bodega', '')),
                                'totalxcompra': totalxcompra
                            })
                        
                        if registros_nuevos:
                            df_nuevos = pd.DataFrame(registros_nuevos)
                            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_VENTAS}"
                            client.load_table_from_dataframe(df_nuevos, table_id).result()
                            st.success(f"✅ {len(registros_nuevos)} ventas guardadas")
                        
                        st.info(f"📊 Nuevos: {len(registros_nuevos)} | Duplicados: {duplicados} | Sin código: {sin_codigo}")
                        
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        st.exception(e)
    
    # =====================
    # TAB 2: COMPRAS
    # =====================
    with tab_compras:
        st.subheader("📦 Carga de Compras")
        
        col1, col2 = st.columns(2)
        with col1:
            archivo_compras = st.file_uploader(
                "Sube el archivo de compras (CSV o Excel)",
                type=["xlsx", "xls", "csv"],
                key="farmazone_compras"
            )
        with col2:
            archivo_inventario_compras = st.file_uploader(
                "Sube el archivo de inventario (CSV o Excel) para categorías",
                type=["xlsx", "xls", "csv"],
                key="farmazone_inventario_compras"
            )
        
        if archivo_compras and archivo_inventario_compras:
            if st.button("🚀 Procesar Compras", key="btn_compras"):
                with st.spinner("Procesando compras..."):
                    try:
                        df_compras = leer_compras(archivo_compras)
                        df_inventario = leer_excel(archivo_inventario_compras, skiprows=4)
                        
                        # 🔥 CREAR DICCIONARIO DE INVENTARIO POR Id
                        dict_inventario = {}
                        for _, row in df_inventario.iterrows():
                            id_producto = limpiar_texto(row.get('Id', ''))
                            if id_producto:
                                dict_inventario[id_producto] = {
                                    'Categoria_L1': limpiar_texto(row.get('Categoria L1', ''))
                                }
                        
                        claves_existentes = cargar_claves_existentes(TABLE_COMPRAS)
                        id_carga = str(uuid.uuid4())
                        registros_nuevos = []
                        duplicados = 0
                        sin_codigo = 0
                        
                        for _, row in df_compras.iterrows():
                            no_compra = limpiar_texto(row.get('Compra', ''))
                            codigo = limpiar_texto(row.get('Codigo', ''))
                            
                            if not no_compra or not codigo:
                                sin_codigo += 1
                                continue
                            
                            clave = f"{no_compra}|{codigo}"
                            if clave in claves_existentes:
                                duplicados += 1
                                continue
                            
                            datos_inv = dict_inventario.get(codigo, {})
                            categoria = datos_inv.get('Categoria_L1', '')
                            
                            registro = {
                                'id_registro': str(uuid.uuid4()),
                                'id_carga': id_carga,
                                'fecha_carga_lote': datetime.now(),
                                'clave_unica': clave,
                                'fecha_proceso': datetime.now(),
                                'usuario_proceso': usuario,
                                'archivo_origen': archivo_compras.name,
                                'no_compra': no_compra,
                                'referencia': limpiar_texto(row.get('Referencia', '')),
                                'proveedor': limpiar_texto(row.get('Proveedor', '')),
                                'fecha_compra': pd.to_datetime(row.get('Fecha', None), errors='coerce'),
                                'payment_type': limpiar_texto(row.get('Payment Type', '')),
                                'bodega': limpiar_texto(row.get('Bodega', '')),
                                'status_proveedor': limpiar_texto(row.get('Status Proveedor', '')),
                                'codigo': codigo,
                                'status_compra': limpiar_texto(row.get('Status Compra', '')),
                                'descripcion': limpiar_texto(row.get('Descripcion', '')),
                                'cuenta': limpiar_texto(row.get('Cuenta', '')),
                                'centro_costo': limpiar_texto(row.get('Centro de Costo', '')),
                                'unidades': limpiar_valor(row.get('Unidades', 0)),
                                'uom_compra': limpiar_texto(row.get('UoM Compra', '')),
                                'uom_unidades': limpiar_valor(row.get('UoM Unidades', 0)),
                                'uom_instock': limpiar_texto(row.get('UoM Instock', '')),
                                'currency': limpiar_texto(row.get('Currency', '')),
                                'factor': limpiar_valor(row.get('Factor', 0)),
                                'impuesto': limpiar_valor(row.get('Impuesto', 0)),
                                'total_linea': limpiar_valor(row.get('Total de Linea', 0)),
                                'categoria_l1': categoria,
                                'activo': True,
                                'fecha_actualizacion': datetime.now(),
                                'usuario_actualizacion': usuario
                            }
                            registros_nuevos.append(registro)
                        
                        if registros_nuevos:
                            df_nuevos = pd.DataFrame(registros_nuevos)
                            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_COMPRAS}"
                            client.load_table_from_dataframe(df_nuevos, table_id).result()
                            st.success(f"✅ {len(registros_nuevos)} compras guardadas")
                        else:
                            st.warning("⚠️ No se encontraron compras nuevas para guardar")
                        
                        st.info(f"📊 Nuevos: {len(registros_nuevos)} | Duplicados: {duplicados} | Sin código: {sin_codigo}")
                        
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        st.exception(e)
    
    # =====================
    # TAB 3: UTILIDAD DIARIA
    # =====================
    with tab_utilidad:
        st.subheader("💰 Utilidad Diaria (Ventas - Compras)")
        
        if st.button("📊 Calcular utilidad diaria", key="btn_utilidad"):
            with st.spinner("Calculando utilidad diaria..."):
                try:
                    query = """
                    WITH ventas_diarias AS (
                        SELECT 
                            fecha_factura AS dia,
                            SUM(totalxcompra) AS total_ventas
                        FROM `proyecto-css-panama.farmazone.farmazone_ventas_historico`
                        WHERE activo = TRUE
                        GROUP BY dia
                    ),
                    compras_diarias AS (
                        SELECT 
                            fecha_compra AS dia,
                            SUM(total_linea) AS total_compras
                        FROM `proyecto-css-panama.farmazone.farmazone_compras_historico`
                        WHERE activo = TRUE
                        GROUP BY dia
                    )
                    SELECT 
                        COALESCE(v.dia, c.dia) AS dia,
                        COALESCE(v.total_ventas, 0) AS ventas,
                        COALESCE(c.total_compras, 0) AS compras,
                        COALESCE(v.total_ventas, 0) - COALESCE(c.total_compras, 0) AS utilidad_diaria
                    FROM ventas_diarias v
                    FULL OUTER JOIN compras_diarias c ON v.dia = c.dia
                    ORDER BY dia DESC
                    """
                    
                    df_utilidad = client.query(query).to_dataframe()
                    
                    if df_utilidad.empty:
                        st.warning("⚠️ No hay datos de ventas o compras")
                    else:
                        st.dataframe(df_utilidad)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Ventas", f"${df_utilidad['ventas'].sum():,.2f}")
                        with col2:
                            st.metric("Total Compras", f"${df_utilidad['compras'].sum():,.2f}")
                        with col3:
                            utilidad_total = df_utilidad['utilidad_diaria'].sum()
                            st.metric("Utilidad Total", f"${utilidad_total:,.2f}")
                        
                        csv = df_utilidad.to_csv(index=False)
                        st.download_button(
                            label="📥 Descargar utilidad diaria (CSV)",
                            data=csv,
                            file_name=f"utilidad_diaria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                        
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.exception(e)
    
    # =====================
    # TAB 4: INVENTARIO
    # =====================
    with tab_inventario:
        st.subheader("📦 Actualizar Inventario en BigQuery")
        
        st.markdown("""
        **Instrucciones:**
        1. Descarga el archivo `Inventario17.xlsx` desde el sistema de la farmacia.
        2. Súbelo aquí para actualizar la tabla en BigQuery.
        3. La tabla **se sobrescribirá completamente** con los datos más recientes.
        """)
        
        archivo_inventario_actual = st.file_uploader(
            "Sube el archivo de inventario (Inventario17.xlsx)",
            type=["xlsx", "xls", "csv"],
            key="farmazone_inventario_actual"
        )
        
        if archivo_inventario_actual:
            try:
                df_inventario = leer_excel(archivo_inventario_actual, skiprows=4)
                
                st.success(f"✅ Archivo leído: {len(df_inventario)} productos")
                
                # Limpiar columnas numéricas
                columnas_numericas = [
                    'Punto de ReOrden', 'Ultimo Precio Proveedor', 'InStock',
                    'Costo Promedio', 'Precio de Lista Predeterminada', 
                    'Costo Promedio WH', 'Ultimo Precio Proveedor Purchase',
                    'Ultimo Costo Entrada', 'Ultimo Costo Unitario F',
                    'Ultimo Costo Unitario', 'Margen', 'Comision',
                    'Grosor', 'Ancho', 'Altura', 'Largo', 'Peso'
                ]
                
                for col in columnas_numericas:
                    if col in df_inventario.columns:
                        df_inventario[col] = df_inventario[col].apply(limpiar_numero_inventario)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total productos", len(df_inventario))
                with col2:
                    if 'InStock' in df_inventario.columns:
                        productos_con_stock = len(df_inventario[df_inventario['InStock'] > 0])
                        st.metric("Productos con stock", productos_con_stock)
                with col3:
                    if 'Status' in df_inventario.columns:
                        activos = len(df_inventario[df_inventario['Status'] == 'ACTIVO'])
                        st.metric("Productos activos", activos)
                
                if st.button("🚀 Guardar Inventario en BigQuery", type="primary", key="btn_inventario"):
                    with st.spinner("Guardando inventario en BigQuery..."):
                        try:
                            filas_guardadas = guardar_inventario(df_inventario)
                            st.success(f"✅ ¡Inventario guardado exitosamente!")
                            st.success(f"📊 {filas_guardadas:,} productos en `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}`")
                            st.info(f"🕒 Snapshot creado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                            
                            if st.button("🔄 Actualizar categorías en ventas (último mes)", key="btn_actualizar_categorias"):
                                with st.spinner("Actualizando categorías en ventas..."):
                                    query_update = f"""
                                    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE_VENTAS}` v
                                    SET v.categoria_l1 = i.`Categoria L1`
                                    FROM `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}` i
                                    WHERE v.codigo = i.Id
                                      AND (v.categoria_l1 IS NULL OR v.categoria_l1 = '')
                                      AND v.fecha_factura >= '2026-07-01'
                                    """
                                    job = client.query(query_update)
                                    job.result()
                                    st.success(f"✅ Categorías actualizadas en ventas")
                            
                            if st.button("🔄 Actualizar categorías en compras (último mes)", key="btn_actualizar_categorias_compras"):
                                with st.spinner("Actualizando categorías en compras..."):
                                    query_update = f"""
                                    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE_COMPRAS}` c
                                    SET c.categoria_l1 = i.`Categoria L1`
                                    FROM `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}` i
                                    WHERE c.codigo = i.Id
                                      AND (c.categoria_l1 IS NULL OR c.categoria_l1 = '')
                                      AND c.fecha_compra >= '2026-07-01'
                                    """
                                    job = client.query(query_update)
                                    job.result()
                                    st.success(f"✅ Categorías actualizadas en compras")
                            
                        except Exception as e:
                            st.error(f"❌ Error al guardar inventario: {e}")
                            st.exception(e)
                            
            except Exception as e:
                st.error(f"❌ Error al leer el archivo: {e}")
                st.exception(e)

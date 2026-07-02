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
        
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return
    
    # =====================
    # TAB (VENTAS / COMPRAS / UTILIDAD)
    # =====================
    tab_ventas, tab_compras, tab_utilidad = st.tabs([
        "📊 Ventas", 
        "📦 Compras", 
        "💰 Utilidad Diaria"
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
    
    # 🔥 NUEVA FUNCIÓN PARA COMPRAS
    def leer_compras(archivo):
        """Lee archivo de compras (sin encabezados, desde la fila 5)"""
        nombre = archivo.name.lower()
        
        # Definir nombres de columnas manualmente
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
        
        # Asignar nombres de columnas (solo las primeras N columnas)
        df.columns = columnas[:len(df.columns)]
        
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
    
    # =====================
    # TAB 1: VENTAS
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
                        df_ventas = leer_excel(archivo_ventas, skiprows=5)
                        df_inventario = leer_excel(archivo_inventario, skiprows=4)
                        
                        dict_inventario = {}
                        for _, row in df_inventario.iterrows():
                            upc = limpiar_texto(row.get('UPC Code', ''))
                            if upc:
                                dict_inventario[upc] = {
                                    'Ultimo_Costo_Unitario': limpiar_valor(row.get('Ultimo Costo Unitario', 0)),
                                    'Categoria_L1': limpiar_texto(row.get('Categoria L1', ''))
                                }
                        
                        claves_existentes = cargar_claves_existentes(TABLE_VENTAS)
                        id_carga = str(uuid.uuid4())
                        registros_nuevos = []
                        duplicados = 0
                        sin_upc = 0
                        
                        for _, row in df_ventas.iterrows():
                            no_factura = limpiar_texto(row.get('No. de Factura', ''))
                            upc = limpiar_texto(row.get('UPC', ''))
                            if not upc:
                                upc = limpiar_texto(row.get('Item Number', ''))
                            
                            if not no_factura or not upc:
                                sin_upc += 1
                                continue
                            
                            clave = f"{no_factura}|{upc}"
                            if clave in claves_existentes:
                                duplicados += 1
                                continue
                            
                            datos_inv = dict_inventario.get(upc, {})
                            unidades = limpiar_valor(row.get('Unidades', 0))
                            precio_unitario = limpiar_valor(row.get('Precio Unitario', 0))
                            precio_compra_orig = limpiar_valor(row.get('Ult. Precio Compra', 0))
                            
                            totalxcompra = unidades * precio_unitario
                            precio_compra_corr = datos_inv.get('Ultimo_Costo_Unitario', 0) if precio_compra_orig <= 0 else precio_compra_orig
                            total_costo = precio_compra_corr * unidades
                            utilidad = totalxcompra - total_costo
                            pct_utilidad = (utilidad / totalxcompra * 100) if totalxcompra > 0 else 0
                            
                            registros_nuevos.append({
                                'id_registro': str(uuid.uuid4()),
                                'id_carga': id_carga,
                                'fecha_carga_lote': datetime.now(),
                                'clave_unica': clave,
                                'fecha_proceso': datetime.now(),
                                'usuario_proceso': usuario,
                                'archivo_origen': archivo_ventas.name,
                                'no_factura': no_factura,
                                'codigo': upc,
                                'upc': upc,
                                'producto': limpiar_texto(row.get('Producto', '')),
                                'unidades': unidades,
                                'precio_unitario': precio_unitario,
                                'totalxcompra': totalxcompra,
                                'ult_precio_compra_original': precio_compra_orig,
                                'ult_precio_compra': precio_compra_corr,
                                'total_costo': total_costo,
                                'utilidad': utilidad,
                                'porcentaje_utilidad': pct_utilidad,
                                'categoria_l1': datos_inv.get('Categoria_L1', ''),
                                'bodega': limpiar_texto(row.get('Bodega', '')),
                                'activo': True,
                                'fecha_actualizacion': datetime.now(),
                                'usuario_actualizacion': usuario
                            })
                        
                        if registros_nuevos:
                            df_nuevos = pd.DataFrame(registros_nuevos)
                            df_nuevos['fecha_factura'] = pd.to_datetime(df_ventas['Fecha Factura'], errors='coerce')
                            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_VENTAS}"
                            client.load_table_from_dataframe(df_nuevos, table_id).result()
                            st.success(f"✅ {len(registros_nuevos)} ventas guardadas")
                        
                        st.info(f"📊 Nuevos: {len(registros_nuevos)} | Duplicados: {duplicados} | Sin UPC: {sin_upc}")
                        
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        st.exception(e)
    
    # =====================
    # TAB 2: COMPRAS (CON INVENTARIO)
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
                        # Leer archivos
                        df_compras = leer_compras(archivo_compras)
                        df_inventario = leer_excel(archivo_inventario_compras, skiprows=4)
                        
                        st.write(f"📋 Columnas compras: {list(df_compras.columns)}")
                        st.write(f"📊 Filas compras: {len(df_compras)}")
                        st.dataframe(df_compras.head(3))
                        
                        # Crear diccionario de inventario (Id → Categoria_L1)
                        dict_inventario = {}
                        for _, row in df_inventario.iterrows():
                        # 🔥 USAR 'Id' como clave (ej: PRO1, PRO10, PRO100)
                            id_producto = limpiar_texto(row.get('Id', ''))
                        if id_producto:
                            dict_inventario[id_producto] = {
                                'Categoria_L1': limpiar_texto(row.get('Categoria L1', '')),
                                'Nombre': limpiar_texto(row.get('Nombre', ''))
                                }

                        st.info(f"📦 Inventario cargado: {len(dict_inventario)} productos únicos")

                        
                        claves_existentes = cargar_claves_existentes(TABLE_COMPRAS)
                        id_carga = str(uuid.uuid4())
                        registros_nuevos = []
                        duplicados = 0
                        sin_codigo = 0

                        for _, row in df_compras.iterrows():
                                no_compra = limpiar_texto(row.get('Compra', ''))
                                codigo = limpiar_texto(row.get('Codigo', ''))  # 🔥 Esto es PRO1120, PRO1217, etc.
    
                            if not no_compra or not codigo:
                                sin_codigo += 1
                                continue
    
                            clave = f"{no_compra}|{codigo}"
                            if clave in claves_existentes:
                                duplicados += 1
                                continue
    
    # 🔥 BUSCAR POR 'Id' (que coincide con 'Codigo' de compras)
    datos_inv = dict_inventario.get(codigo, {})  # codigo = PRO1120, etc.
    categoria = datos_inv.get('Categoria_L1', '')
                            
                            # Obtener categoría desde inventario
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
                                'fecha_compra': row.get('Fecha', None),
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
                                'categoria_l1': categoria,  # 🔥 NUEVO: desde inventario
                                'activo': True,
                                'fecha_actualizacion': datetime.now(),
                                'usuario_actualizacion': usuario
                            }
                            registros_nuevos.append(registro)
                        
                        st.info(f"📊 Nuevos: {len(registros_nuevos)} | Duplicados: {duplicados} | Sin código: {sin_codigo}")
                        
                        if registros_nuevos:
                            df_nuevos = pd.DataFrame(registros_nuevos)
                            df_nuevos['fecha_compra'] = pd.to_datetime(df_nuevos['fecha_compra'], errors='coerce')
                            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_COMPRAS}"
                            
                            # Crear tabla si no existe (con categoria_l1)
                            try:
                                client.get_table(table_id)
                            except Exception:
                                schema = [
                                    bigquery.SchemaField("id_registro", "STRING", mode="REQUIRED"),
                                    bigquery.SchemaField("id_carga", "STRING", mode="REQUIRED"),
                                    bigquery.SchemaField("fecha_carga_lote", "TIMESTAMP"),
                                    bigquery.SchemaField("clave_unica", "STRING", mode="REQUIRED"),
                                    bigquery.SchemaField("fecha_proceso", "TIMESTAMP"),
                                    bigquery.SchemaField("usuario_proceso", "STRING"),
                                    bigquery.SchemaField("archivo_origen", "STRING"),
                                    bigquery.SchemaField("no_compra", "STRING"),
                                    bigquery.SchemaField("referencia", "STRING"),
                                    bigquery.SchemaField("proveedor", "STRING"),
                                    bigquery.SchemaField("fecha_compra", "DATE"),
                                    bigquery.SchemaField("payment_type", "STRING"),
                                    bigquery.SchemaField("bodega", "STRING"),
                                    bigquery.SchemaField("status_proveedor", "STRING"),
                                    bigquery.SchemaField("codigo", "STRING"),
                                    bigquery.SchemaField("status_compra", "STRING"),
                                    bigquery.SchemaField("descripcion", "STRING"),
                                    bigquery.SchemaField("cuenta", "STRING"),
                                    bigquery.SchemaField("centro_costo", "STRING"),
                                    bigquery.SchemaField("unidades", "FLOAT64"),
                                    bigquery.SchemaField("uom_compra", "STRING"),
                                    bigquery.SchemaField("uom_unidades", "FLOAT64"),
                                    bigquery.SchemaField("uom_instock", "STRING"),
                                    bigquery.SchemaField("currency", "STRING"),
                                    bigquery.SchemaField("factor", "FLOAT64"),
                                    bigquery.SchemaField("impuesto", "FLOAT64"),
                                    bigquery.SchemaField("total_linea", "FLOAT64"),
                                    bigquery.SchemaField("categoria_l1", "STRING"),  # 🔥 NUEVA
                                    bigquery.SchemaField("activo", "BOOL"),
                                    bigquery.SchemaField("fecha_actualizacion", "TIMESTAMP"),
                                    bigquery.SchemaField("usuario_actualizacion", "STRING"),
                                ]
                                table = bigquery.Table(table_id, schema=schema)
                                client.create_table(table)
                                st.info(f"📋 Tabla {TABLE_COMPRAS} creada con categoría")
                            
                            job = client.load_table_from_dataframe(df_nuevos, table_id)
                            job.result()
                            st.success(f"✅ {len(registros_nuevos)} compras guardadas")
                            st.info(f"🆔 Para revertir: DELETE FROM `{table_id}` WHERE id_carga = '{id_carga}'")
                        else:
                            st.warning("⚠️ No se encontraron compras nuevas para guardar")
                        
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
                            st.metric("Utilidad Total", f"${utilidad_total:,.2f}", 
                                     delta=f"{utilidad_total/df_utilidad['ventas'].sum()*100:.1f}%" if df_utilidad['ventas'].sum() > 0 else None)
                        
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

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
        TABLE_INVENTARIO = "inventario_actual"  # 🆕 Tabla de inventario
        
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
        "📦 Inventario"  # 🆕 Nueva pestaña
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
    # 🆕 FUNCIÓN PARA GUARDAR INVENTARIO
    # =====================
    def guardar_inventario(df_inventario: pd.DataFrame, archivo_nombre: str) -> int:
        """
        Guarda el inventario en BigQuery sobrescribiendo la tabla.
        Retorna el número de filas guardadas.
        """
        # Agregar columna de fecha_snapshot
        df_inventario['fecha_snapshot'] = datetime.now()
        
        # Referencia a la tabla
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}"
        
        # Sobrescribir la tabla
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        
        job = client.load_table_from_dataframe(df_inventario, table_id, job_config=job_config)
        job.result()
        
        return len(df_inventario)
    
    # =====================
    # TAB 1: VENTAS (sin cambios)
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
    # TAB 2: COMPRAS (sin cambios)
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
                        
                        dict_inventario = {}
                        for _, row in df_inventario.iterrows():
                            id_producto = limpiar_texto(row.get('Id', ''))
                            if id_producto:
                                dict_inventario[id_producto] = {
                                    'Categoria_L1': limpiar_texto(row.get('Categoria L1', '')),
                                    'Nombre': limpiar_texto(row.get('Nombre', ''))
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
                                'categoria_l1': categoria,
                                'activo': True,
                                'fecha_actualizacion': datetime.now(),
                                'usuario_actualizacion': usuario
                            }
                            registros_nuevos.append(registro)
                        
                        if registros_nuevos:
                            df_nuevos = pd.DataFrame(registros_nuevos)
                            df_nuevos['fecha_compra'] = pd.to_datetime(df_nuevos['fecha_compra'], errors='coerce')
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
    # TAB 3: UTILIDAD DIARIA (sin cambios)
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
    
    # =====================
    # 🆕 TAB 4: INVENTARIO
    # =====================
    with tab_inventario:
        st.subheader("📦 Actualizar Inventario en BigQuery")
        
        st.markdown("""
        **Instrucciones:**
        1. Descarga el archivo `Inventario17.xlsx` desde el sistema de la farmacia.
        2. Súbelo aquí para actualizar la tabla en BigQuery.
        3. La tabla **se sobrescribirá completamente** con los datos más recientes.
        4. Esto permite actualizar las categorías en ventas y compras.
        """)
        
        archivo_inventario_actual = st.file_uploader(
            "Sube el archivo de inventario (Inventario17.xlsx)",
            type=["xlsx", "xls", "csv"],
            key="farmazone_inventario_actual"
        )
        
        if archivo_inventario_actual:
            try:
                # Leer archivo (saltando 4 filas)
                df_inventario = leer_excel(archivo_inventario_actual, skiprows=4)
                
                st.success(f"✅ Archivo leído: {len(df_inventario)} productos")
                
                # Mostrar vista previa
                with st.expander("📊 Vista previa de los datos", expanded=False):
                    st.dataframe(df_inventario.head(10))
                    st.caption(f"Total de columnas: {len(df_inventario.columns)}")
                
                # Mostrar estadísticas básicas
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
                
                # Botón para guardar
                if st.button("🚀 Guardar Inventario en BigQuery", type="primary", key="btn_inventario"):
                    with st.spinner("Guardando inventario en BigQuery..."):
                        try:
                            filas_guardadas = guardar_inventario(df_inventario, archivo_inventario_actual.name)
                            st.success(f"✅ ¡Inventario guardado exitosamente!")
                            st.success(f"📊 {filas_guardadas:,} productos en `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}`")
                            st.info(f"🕒 Snapshot creado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                            
                            # Opción para actualizar categorías en ventas
                            if st.button("🔄 Actualizar categorías en ventas (último mes)", key="btn_actualizar_categorias"):
                                with st.spinner("Actualizando categorías en ventas..."):
                                    query_update = f"""
                                    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE_VENTAS}` v
                                    SET v.categoria_l1 = i.`Categoria L1`
                                    FROM `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}` i
                                    WHERE v.upc = i.`UPC Code`
                                      AND (v.categoria_l1 IS NULL OR v.categoria_l1 = '')
                                      AND v.fecha_factura >= '2026-07-01'
                                    """
                                    job = client.query(query_update)
                                    job.result()
                                    st.success(f"✅ Categorías actualizadas en ventas")
                            
                            # Opción para actualizar categorías en compras
                            if st.button("🔄 Actualizar categorías en compras (último mes)", key="btn_actualizar_categorias_compras"):
                                with st.spinner("Actualizando categorías en compras..."):
                                    query_update = f"""
                                    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE_COMPRAS}` c
                                    SET c.categoria_l1 = i.`Categoria L1`
                                    FROM `{PROJECT_ID}.{DATASET}.{TABLE_INVENTARIO}` i
                                    WHERE c.codigo = i.`Id`
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

# modulos/farmazone/carga_reportes.py
import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.cloud import bigquery

def run(usuario):
    """Módulo principal de Farmazone - Enriquecimiento de ventas con inventario"""
    
    st.title("💊 Farmazone - Reporte de Ventas")
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
        TABLE_NAME = "farmazone_ventas_historico"
        
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return
    
    # =====================
    # SUBIR ARCHIVOS
    # =====================
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📤 Archivo de Ventas")
        archivo_ventas = st.file_uploader(
            "Sube el archivo de ventas (CSV o Excel)",
            type=["xlsx", "xls", "csv"],
            key="farmazone_ventas"
        )
    
    with col2:
        st.subheader("📦 Archivo de Inventario")
        archivo_inventario = st.file_uploader(
            "Sube el archivo de inventario (CSV o Excel)",
            type=["xlsx", "xls", "csv"],
            key="farmazone_inventario"
        )
    
    st.markdown("---")
    
    # =====================
    # FUNCIONES DE PROCESAMIENTO
    # =====================
    
    def leer_ventas(archivo):
        """Lee archivo de ventas saltando filas iniciales"""
        # El archivo de ventas tiene encabezados en la fila 6 (índice 5)
        if archivo.name.endswith('.csv'):
            return pd.read_csv(archivo, dtype=str, skiprows=5)
        else:
            return pd.read_excel(archivo, dtype=str, skiprows=5, header=0)
    
    def leer_inventario(archivo):
        """Lee archivo de inventario saltando filas iniciales"""
        # El archivo de inventario tiene encabezados en la fila 5 (índice 4)
        if archivo.name.endswith('.csv'):
            return pd.read_csv(archivo, dtype=str, skiprows=4)
        else:
            return pd.read_excel(archivo, dtype=str, skiprows=4, header=0)
    
    def limpiar_valor(valor):
        """Convierte cualquier valor a float (maneja strings con comas y puntos)"""
        if pd.isna(valor) or valor == "" or valor == "-":
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)
        # Es string: limpiar
        valor_str = str(valor).strip()
        # Reemplazar coma decimal por punto
        valor_str = valor_str.replace(',', '.')
        # Eliminar comas de miles y otros caracteres
        valor_str = ''.join(c for c in valor_str if c.isdigit() or c == '.' or c == '-')
        if valor_str == "" or valor_str == "-":
            return 0.0
        try:
            return float(valor_str)
        except:
            return 0.0
    
    def limpiar_texto(valor):
        """Convierte valor a string limpio"""
        if pd.isna(valor) or valor == "":
            return ""
        return str(valor).strip()
    
    def cargar_claves_existentes(client, project_id, dataset, table_name):
        """Carga las claves únicas existentes para evitar duplicados"""
        try:
            query = f"""
            SELECT DISTINCT clave_unica 
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE activo = TRUE
            """
            df_existentes = client.query(query).to_dataframe()
            return set(df_existentes['clave_unica'].tolist())
        except Exception:
            return set()
    
    # =====================
    # PROCESAR ARCHIVOS
    # =====================
    if archivo_ventas and archivo_inventario:
        
        # Generar ID de carga único
        id_carga = str(uuid.uuid4())
        fecha_carga_lote = datetime.now()
        
        st.info(f"🆔 ID de carga: {id_carga[:8]}...")
        
        if st.button("🚀 Procesar y enriquecer", type="primary"):
            
            with st.spinner("Procesando archivos..."):
                try:
                    # 1. Leer archivos
                    df_ventas = leer_ventas(archivo_ventas)
                    df_inventario = leer_inventario(archivo_inventario)
                    
                    st.success(f"✅ Ventas: {len(df_ventas)} filas | Inventario: {len(df_inventario)} filas")
                    
                    # 2. Crear diccionario de inventario (UPC → datos)
                    dict_inventario = {}
                    for _, row in df_inventario.iterrows():
                        upc = limpiar_texto(row.get('UPC Code', ''))
                        if upc:
                            dict_inventario[upc] = {
                                'Ultimo_Costo_Unitario': limpiar_valor(row.get('Ultimo Costo Unitario', 0)),
                                'Costo_Promedio': limpiar_valor(row.get('Costo Promedio', 0)),
                                'Precio_Lista': limpiar_valor(row.get('Precio de Lista Predeterminada', 0)),
                                'Categoria_L1': limpiar_texto(row.get('Categoria L1', '')),
                                'Categoria_L2': limpiar_texto(row.get('Categoria L2', '')),
                                'Categoria_L3': limpiar_texto(row.get('Categoria L3', ''))
                            }
                    
                    st.info(f"📦 Inventario cargado: {len(dict_inventario)} productos únicos")
                    
                    # 3. Cargar claves existentes
                    claves_existentes = cargar_claves_existentes(client, PROJECT_ID, DATASET, TABLE_NAME)
                    st.info(f"📋 Registros existentes: {len(claves_existentes)}")
                    
                    # 4. Procesar ventas
                    registros_nuevos = []
                    registros_duplicados = 0
                    registros_sin_upc = 0
                    
                    for _, row in df_ventas.iterrows():
                        # Limpiar campos clave
                        no_factura = limpiar_texto(row.get('No. de Factura', ''))
                        upc = limpiar_texto(row.get('UPC', ''))
                        
                        if not upc:
                            upc = limpiar_texto(row.get('Item Number', ''))
                        
                        if not no_factura or not upc:
                            registros_sin_upc += 1
                            continue
                        
                        clave_unica = f"{no_factura}|{upc}"
                        
                        if clave_unica in claves_existentes:
                            registros_duplicados += 1
                            continue
                        
                        # Obtener datos del inventario
                        datos_inv = dict_inventario.get(upc, {})
                        
                        # Limpiar valores numéricos
                        unidades = limpiar_valor(row.get('Unidades', 0))
                        precio_unitario = limpiar_valor(row.get('Precio Unitario', 0))
                        precio_compra_orig = limpiar_valor(row.get('Ult. Precio Compra', 0))
                        
                        # Calcular totalxcompra
                        totalxcompra = unidades * precio_unitario
                        
                        # Corregir precio de compra si es 0
                        if precio_compra_orig <= 0 and upc in dict_inventario:
                            precio_compra_corr = datos_inv.get('Ultimo_Costo_Unitario', 0)
                        else:
                            precio_compra_corr = precio_compra_orig
                        
                        total_costo = precio_compra_corr * unidades
                        utilidad = totalxcompra - total_costo
                        pct_utilidad = (utilidad / totalxcompra * 100) if totalxcompra > 0 else 0
                        
                        # Crear registro
                        nuevo_registro = {
                            'id_registro': str(uuid.uuid4()),
                            'id_carga': id_carga,
                            'fecha_carga_lote': fecha_carga_lote,
                            'clave_unica': clave_unica,
                            'fecha_proceso': datetime.now(),
                            'usuario_proceso': usuario,
                            'archivo_origen': archivo_ventas.name,
                            'no_factura': no_factura,
                            'id_fiscal': limpiar_texto(row.get('ID Fiscal', '')),
                            'documento': limpiar_texto(row.get('Documento', '')),
                            'ano_mes_factura': limpiar_texto(row.get('Año_Mes Factura', '')),
                            'fecha_factura': row.get('Fecha Factura', None),
                            'nombre_cliente': limpiar_texto(row.get('Nombre del Cliente', '')),
                            'pais': limpiar_texto(row.get('Pais', '')),
                            'caja': limpiar_texto(row.get('Caja', '')),
                            'vendedor': limpiar_texto(row.get('Vendedor', '')),
                            'codigo': upc,
                            'upc': upc,
                            'lote': limpiar_texto(row.get('Lote', '')),
                            'arancel': limpiar_texto(row.get('Arancel', '')),
                            'producto': limpiar_texto(row.get('Producto', '')),
                            'marca': limpiar_texto(row.get('Marca', '')),
                            'industria': limpiar_texto(row.get('Industria', '')),
                            'proveedor_id': limpiar_texto(row.get('Proveedor Id.', '')),
                            'proveedor_nombre': limpiar_texto(row.get('Proveedor', '')),
                            'categoria_l1': datos_inv.get('Categoria_L1', limpiar_texto(row.get('Category', ''))),
                            'categoria_l2': datos_inv.get('Categoria_L2', limpiar_texto(row.get('Categoria L2', ''))),
                            'categoria_l3': datos_inv.get('Categoria_L3', limpiar_texto(row.get('Categoria L3', ''))),
                            'bodega': limpiar_texto(row.get('Bodega', '')),
                            'unidades': unidades,
                            'precio_unitario': precio_unitario,
                            'total_factura': limpiar_valor(row.get('Total de factura', 0)),
                            'subtotal': limpiar_valor(row.get('SubTotal', 0)),
                            'por_desc_linea': limpiar_valor(row.get('Por. Desc por Linea', 0)),
                            'des_por_linea': limpiar_valor(row.get('Des. Por Linea', 0)),
                            'total_linea': limpiar_valor(row.get('Total', 0)),
                            'totalxcompra': totalxcompra,
                            'itbms': limpiar_valor(row.get('ITBMS', 0)),
                            'ult_precio_compra_original': precio_compra_orig,
                            'ult_precio_compra': precio_compra_corr,
                            'total_costo': total_costo,
                            'utilidad': utilidad,
                            'porcentaje_utilidad': pct_utilidad,
                            'activo': True,
                            'fecha_actualizacion': datetime.now(),
                            'usuario_actualizacion': usuario
                        }
                        registros_nuevos.append(nuevo_registro)
                    
                    # Estadísticas
                    st.info(f"📊 Nuevos registros: {len(registros_nuevos)} | Duplicados: {registros_duplicados} | Sin UPC: {registros_sin_upc}")
                    
                    # 5. Guardar en BigQuery
                    if registros_nuevos:
                        df_nuevos = pd.DataFrame(registros_nuevos)
                        
                        # Convertir fechas
                        if 'fecha_factura' in df_nuevos.columns:
                            df_nuevos['fecha_factura'] = pd.to_datetime(df_nuevos['fecha_factura'], errors='coerce')
                        
                        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"
                        
                        # Cargar datos
                        job = client.load_table_from_dataframe(df_nuevos, table_id)
                        job.result()
                        st.success(f"✅ {len(registros_nuevos)} registros guardados en BigQuery")
                        
                        # Mostrar comando para reversión
                        st.info(f"🆔 Para revertir: DELETE FROM `{table_id}` WHERE id_carga = '{id_carga}'")
                    
                    # 6. Resumen
                    st.subheader("📊 Resumen de ventas procesadas")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Registros nuevos", len(registros_nuevos))
                    with col2:
                        st.metric("Duplicados omitidos", registros_duplicados)
                    with col3:
                        st.metric("Sin UPC", registros_sin_upc)
                    with col4:
                        st.metric("Utilidad total", f"${sum(r['utilidad'] for r in registros_nuevos):,.2f}")
                    
                    # 7. Descargar CSV enriquecido
                    if registros_nuevos:
                        df_descarga = pd.DataFrame(registros_nuevos)
                        columnas_descarga = ['no_factura', 'codigo', 'producto', 'unidades', 
                                            'precio_unitario', 'ult_precio_compra', 'total_costo',
                                            'totalxcompra', 'utilidad', 'porcentaje_utilidad', 'categoria_l1']
                        df_descarga = df_descarga[[c for c in columnas_descarga if c in df_descarga.columns]]
                        
                        csv = df_descarga.to_csv(index=False)
                        st.download_button(
                            label="📥 Descargar reporte enriquecido (CSV)",
                            data=csv,
                            file_name=f"farmazone_reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    
                except Exception as e:
                    st.error(f"❌ Error al procesar: {e}")
                    st.exception(e)
    
    else:
        st.info("📌 Sube los dos archivos (Ventas e Inventario) para generar el reporte")
    
    st.markdown("---")
    st.caption("💊 Farmazone - Reporte de Ventas | NEXO CRM")

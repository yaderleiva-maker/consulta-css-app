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
            type=["xlsx", "csv"],
            key="farmazone_ventas"
        )
    
    with col2:
        st.subheader("📦 Archivo de Inventario")
        archivo_inventario = st.file_uploader(
            "Sube el archivo de inventario (CSV o Excel)",
            type=["xlsx", "csv"],
            key="farmazone_inventario"
        )
    
    st.markdown("---")
    
    # =====================
    # FUNCIONES DE PROCESAMIENTO
    # =====================
    
    def leer_archivo(archivo):
        """Lee CSV o Excel según extensión"""
        if archivo.name.endswith('.csv'):
            return pd.read_csv(archivo, dtype=str)
        else:
            return pd.read_excel(archivo, dtype=str)
    
    def limpiar_valor(valor):
        """Convierte string con comas a float"""
        if pd.isna(valor) or valor == "":
            return 0.0
        if isinstance(valor, str):
            valor = valor.replace(',', '').strip()
        try:
            return float(valor)
        except:
            return 0.0
    
    def limpiar_valor_moneda(valor):
        """Limpia valores monetarios (ej: '$15.25' → 15.25)"""
        if pd.isna(valor) or valor == "":
            return 0.0
        if isinstance(valor, str):
            valor = valor.replace('$', '').replace(',', '').strip()
        try:
            return float(valor)
        except:
            return 0.0
    
    def cargar_claves_existentes(client, project_id, dataset, table_name, id_carga_actual=None):
        """Carga las claves únicas existentes para evitar duplicados"""
        try:
            # Para cargas grandes, esto podría optimizarse con una tabla temporal
            query = f"""
            SELECT DISTINCT clave_unica 
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE activo = TRUE
            """
            df_existentes = client.query(query).to_dataframe()
            return set(df_existentes['clave_unica'].tolist())
        except Exception:
            return set()  # Tabla no existe o está vacía
    
    # =====================
    # PROCESAR ARCHIVOS
    # =====================
    if archivo_ventas and archivo_inventario:
        
        # Generar ID de carga único para este proceso
        id_carga = str(uuid.uuid4())
        fecha_carga_lote = datetime.now()
        
        st.info(f"🆔 ID de carga: {id_carga[:8]}... (útil para depuración)")
        
        if st.button("🚀 Procesar y enriquecer", type="primary"):
            
            with st.spinner("Procesando archivos..."):
                try:
                    # 1. Leer archivos
                    df_ventas = leer_archivo(archivo_ventas)
                    df_inventario = leer_archivo(archivo_inventario)
                    
                    st.success(f"✅ Ventas: {len(df_ventas)} filas | Inventario: {len(df_inventario)} filas")
                    
                    # 2. Limpiar y convertir tipos
                    cols_numericas_ventas = ['Unidades', 'Precio Unitario', 'Ult. Precio Compra']
                    for col in cols_numericas_ventas:
                        if col in df_ventas.columns:
                            df_ventas[col] = df_ventas[col].apply(limpiar_valor)
                    
                    # 3. Crear diccionario de inventario (UPC → datos)
                    dict_inventario = {}
                    for _, row in df_inventario.iterrows():
                        upc = str(row.get('UPC Code', '')).strip()
                        if upc and upc != 'nan':
                            dict_inventario[upc] = {
                                'Ultimo_Costo_Unitario': limpiar_valor_moneda(row.get('Ultimo Costo Unitario', 0)),
                                'Costo_Promedio': limpiar_valor_moneda(row.get('Costo Promedio', 0)),
                                'Precio_Lista': limpiar_valor_moneda(row.get('Precio de Lista Predeterminada', 0)),
                                'Categoria_L1': row.get('Categoria L1', ''),
                                'Categoria_L2': row.get('Categoria L2', ''),
                                'Categoria_L3': row.get('Categoria L3', '')
                            }
                    
                    st.info(f"📦 Inventario cargado: {len(dict_inventario)} productos únicos")
                    
                    # 4. Cargar claves existentes para evitar duplicados
                    claves_existentes = cargar_claves_existentes(client, PROJECT_ID, DATASET, TABLE_NAME)
                    st.info(f"📋 Registros existentes: {len(claves_existentes)}")
                    
                    # 5. Enriquecer ventas y preparar DataFrame final
                    registros_nuevos = []
                    registros_duplicados = 0
                    registros_sin_upc = 0
                    
                    for _, row in df_ventas.iterrows():
                        no_factura = str(row.get('No. de Factura', '')).strip()
                        if not no_factura or no_factura == 'nan':
                            no_factura = str(row.get('Documento', '')).strip()
                        
                        # 🔥 PRIORIDAD: UPC (más estable que Item Number)
                        upc = str(row.get('UPC', '')).strip()
                        if not upc or upc == 'nan':
                            upc = str(row.get('Item Number', '')).strip()
                            if not upc or upc == 'nan':
                                registros_sin_upc += 1
                                continue
                        
                        # Usar UPC como código principal
                        codigo = upc
                        clave_unica = f"{no_factura}|{codigo}"
                        
                        # Verificar si ya existe
                        if clave_unica in claves_existentes:
                            registros_duplicados += 1
                            continue
                        
                        # Obtener datos del inventario
                        datos_inv = dict_inventario.get(upc, {})
                        
                        # Precios y costos
                        precio_unitario = row.get('Precio Unitario', 0)
                        unidades = row.get('Unidades', 0)
                        precio_compra_orig = row.get('Ult. Precio Compra', 0)
                        
                        # 🔥 CORREGIDO: usar <= 0 en lugar de == 0
                        if precio_compra_orig <= 0 and upc in dict_inventario:
                            precio_compra_corr = datos_inv.get('Ultimo_Costo_Unitario', 0)
                        else:
                            precio_compra_corr = precio_compra_orig
                        
                        total_venta = precio_unitario * unidades
                        total_costo = precio_compra_corr * unidades
                        utilidad = total_venta - total_costo
                        pct_utilidad = (utilidad / total_venta * 100) if total_venta > 0 else 0
                        
                        # Crear registro
                        nuevo_registro = {
                            'id_registro': str(uuid.uuid4()),
                            'id_carga': id_carga,  # 🔥 NUEVO: para eliminar cargas enteras
                            'fecha_carga_lote': fecha_carga_lote,  # 🔥 NUEVO
                            'clave_unica': clave_unica,
                            'fecha_proceso': datetime.now(),
                            'usuario_proceso': usuario,
                            'archivo_origen': archivo_ventas.name,
                            'no_factura': no_factura,
                            'id_fiscal': row.get('ID Fiscal', ''),
                            'documento': row.get('Documento', ''),
                            'ano_mes_factura': row.get('Año_Mes Factura', ''),
                            'fecha_factura': row.get('Fecha Factura', None),
                            'nombre_cliente': row.get('Nombre del Cliente', ''),
                            'pais': row.get('Pais', ''),
                            'caja': row.get('Caja', ''),
                            'vendedor': row.get('Vendedor', ''),
                            'codigo': codigo,  # 🔥 PRIORIDAD: UPC
                            'codigo_item_number': row.get('Item Number', ''),  # 🔥 NUEVO: guardar ambos
                            'upc': upc,
                            'lote': row.get('Lote', ''),
                            'arancel': row.get('Arancel', ''),
                            'producto': row.get('Producto', ''),
                            'marca': row.get('Marca', ''),
                            'industria': row.get('Industria', ''),
                            'proveedor_id': row.get('Proveedor Id.', ''),
                            'proveedor_nombre': row.get('Proveedor Nom.', ''),
                            'categoria_l1': datos_inv.get('Categoria_L1', row.get('Categoria L2', '')),
                            'categoria_l2': datos_inv.get('Categoria_L2', ''),
                            'categoria_l3': datos_inv.get('Categoria_L3', ''),
                            'unidades': unidades,
                            'precio_unitario': precio_unitario,
                            'total_factura': row.get('Total de factura', 0),
                            'subtotal': row.get('SubTotal', 0),
                            'por_desc_linea': row.get('Por. Desc por Linea', 0),
                            'des_por_linea': row.get('Des. Por Linea', 0),
                            'total_linea': row.get('Total', 0),
                            'itbms': (row.get('Des. Por Linea', 0) + row.get('Total', 0)) - (precio_unitario * unidades),
                            'ult_precio_compra_original': precio_compra_orig,
                            'ult_precio_compra': precio_compra_corr,
                            'total_costo': total_costo,
                            'utilidad': utilidad,
                            'porcentaje_utilidad': pct_utilidad,
                            'ultimo_costo_unitario': datos_inv.get('Ultimo_Costo_Unitario', 0),
                            'costo_promedio': datos_inv.get('Costo_Promedio', 0),
                            'precio_lista': datos_inv.get('Precio_Lista', 0),
                            'activo': True,
                            'fecha_actualizacion': datetime.now(),
                            'usuario_actualizacion': usuario
                        }
                        registros_nuevos.append(nuevo_registro)
                    
                    # Estadísticas
                    st.info(f"📊 Nuevos registros: {len(registros_nuevos)} | Duplicados: {registros_duplicados} | Sin UPC: {registros_sin_upc}")
                    
                    # 6. Guardar en BigQuery
                    if registros_nuevos:
                        df_nuevos = pd.DataFrame(registros_nuevos)
                        
                        # Convertir fechas
                        df_nuevos['fecha_factura'] = pd.to_datetime(df_nuevos['fecha_factura'], errors='coerce')
                        
                        # Crear tabla si no existe y cargar datos
                        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"
                        
                        # Cargar datos (la tabla ya debería existir, si no, se crea automáticamente)
                        job = client.load_table_from_dataframe(df_nuevos, table_id)
                        job.result()
                        st.success(f"✅ {len(registros_nuevos)} registros guardados en BigQuery")
                        
                        # Mostrar ID de carga para posible reversión
                        st.info(f"🆔 Para revertir esta carga (si hay error), ejecuta:\n\n"
                               f"```sql\nDELETE FROM `{table_id}` WHERE id_carga = '{id_carga}';\n```")
                    
                    # 7. Mostrar resumen
                    st.subheader("📊 Resumen de ventas procesadas")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Registros nuevos", len(registros_nuevos))
                    with col2:
                        st.metric("Duplicados omitidos", registros_duplicados)
                    with col3:
                        st.metric("Sin UPC", registros_sin_upc)
                    with col4:
                        st.metric("Total unidades", sum(r['unidades'] for r in registros_nuevos))
                    with col5:
                        st.metric("Utilidad total", f"${sum(r['utilidad'] for r in registros_nuevos):,.2f}")
                    
                    # 8. Descargar CSV enriquecido
                    if registros_nuevos:
                        df_descarga = pd.DataFrame(registros_nuevos)
                        columnas_descarga = ['no_factura', 'codigo', 'producto', 'unidades', 
                                            'precio_unitario', 'ult_precio_compra', 'total_costo',
                                            'utilidad', 'porcentaje_utilidad', 'categoria_l1']
                        df_descarga = df_descarga[columnas_descarga]
                        
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
    
    # =====================
    # HISTÓRICO
    # =====================
    st.markdown("---")
    with st.expander("📜 Ver histórico de cargas"):
        try:
            query = f"""
            SELECT 
                DATE(fecha_carga_lote) as fecha_carga,
                id_carga,
                usuario_proceso,
                COUNT(*) as total_registros,
                SUM(unidades) as total_unidades,
                SUM(utilidad) as total_utilidad
            FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}`
            GROUP BY fecha_carga, id_carga, usuario_proceso
            ORDER BY fecha_carga DESC
            LIMIT 20
            """
            df_historico = client.query(query).to_dataframe()
            if not df_historico.empty:
                st.dataframe(df_historico)
                
                # Opción para eliminar una carga completa (solo admin)
                st.markdown("---")
                st.warning("⚠️ **Solo para administradores** - Eliminar una carga completa")
                id_carga_eliminar = st.text_input("ID de carga a eliminar:")
                if st.button("🗑️ Eliminar carga", key="delete_carga"):
                    if id_carga_eliminar:
                        delete_query = f"""
                        DELETE FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}`
                        WHERE id_carga = '{id_carga_eliminar}'
                        """
                        client.query(delete_query).result()
                        st.success(f"✅ Carga {id_carga_eliminar} eliminada")
                        st.rerun()
            else:
                st.info("No hay datos históricos aún")
        except Exception as e:
            st.info("No hay datos históricos aún")
    
    st.markdown("---")
    st.caption("💊 Farmazone - Reporte de Ventas | NEXO CRM")

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
                    # 🔥 LEER VENTAS CON LA ESTRUCTURA CORRECTA
                    df_ventas = leer_excel(archivo_ventas, skiprows=5)
                    df_inventario = leer_excel(archivo_inventario, skiprows=4)
                    
                    # 🔥 MOSTRAR COLUMNAS PARA VERIFICAR
                    st.write("📋 Columnas en ventas:", list(df_ventas.columns))
                    st.write("📊 Muestra de ventas:", df_ventas.head(3))
                    
                    # Crear diccionario de inventario (usando Id como clave)
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
                    con_categoria = 0
                    sin_categoria = 0
                    
                    for _, row in df_ventas.iterrows():
                        no_factura = limpiar_texto(row.get('No. de Factura', ''))
                        
                        # 🔥 PRIORIDAD: Usar 'Codigo' (es el Id del producto)
                        codigo = limpiar_texto(row.get('Codigo', ''))
                        
                        # Si no hay Codigo, intentar con 'Item Number'
                        if not codigo:
                            codigo = limpiar_texto(row.get('Item Number', ''))
                        
                        # Si no hay Item Number, intentar con 'UPC'
                        if not codigo:
                            codigo = limpiar_texto(row.get('UPC', ''))
                        
                        if not no_factura or not codigo:
                            sin_codigo += 1
                            continue
                        
                        clave = f"{no_factura}|{codigo}"
                        if clave in claves_existentes:
                            duplicados += 1
                            continue
                        
                        # Buscar en inventario por el código
                        datos_inv = dict_inventario.get(codigo, {})
                        categoria = datos_inv.get('Categoria_L1', '')
                        
                        if categoria:
                            con_categoria += 1
                        else:
                            sin_categoria += 1
                        
                        # Limpiar valores numéricos
                        unidades = limpiar_valor(row.get('Unidades', 0))
                        precio_unitario = limpiar_valor(row.get('Precio Unitario', 0))
                        precio_compra_orig = limpiar_valor(row.get('Ult. Precio Compra', 0))
                        
                        totalxcompra = unidades * precio_unitario
                        precio_compra_corr = datos_inv.get('Ultimo_Costo_Unitario', 0) if precio_compra_orig <= 0 else precio_compra_orig
                        total_costo = precio_compra_corr * unidades
                        utilidad = totalxcompra - total_costo
                        pct_utilidad = (utilidad / totalxcompra * 100) if totalxcompra > 0 else 0
                        
                        # 🔥 OBTENER UPC CORRECTO
                        upc = limpiar_texto(row.get('UPC', ''))
                        if not upc:
                            upc = limpiar_texto(row.get('Item Number', ''))
                        
                        registros_nuevos.append({
                            'id_registro': str(uuid.uuid4()),
                            'id_carga': id_carga,
                            'fecha_carga_lote': datetime.now(),
                            'clave_unica': clave,
                            'fecha_proceso': datetime.now(),
                            'usuario_proceso': usuario,
                            'archivo_origen': archivo_ventas.name,
                            'no_factura': no_factura,
                            'codigo': codigo,  # 🔥 ESTE ES EL ID DEL PRODUCTO
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
                            'categoria_l1': categoria,
                            'bodega': limpiar_texto(row.get('Bodega', '')),
                            'activo': True,
                            'fecha_actualizacion': datetime.now(),
                            'usuario_actualizacion': usuario
                        })
                    
                    # Guardar en BigQuery
                    if registros_nuevos:
                        df_nuevos = pd.DataFrame(registros_nuevos)
                        df_nuevos['fecha_factura'] = pd.to_datetime(df_ventas['Fecha Factura'], errors='coerce')
                        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_VENTAS}"
                        client.load_table_from_dataframe(df_nuevos, table_id).result()
                        st.success(f"✅ {len(registros_nuevos)} ventas guardadas")
                    
                    # Resumen
                    st.info(f"""
                    📊 **Resumen:**
                    - Nuevos: {len(registros_nuevos)}
                    - Duplicados: {duplicados}
                    - Sin código: {sin_codigo}
                    - Con categoría: {con_categoria}
                    - Sin categoría: {sin_categoria}
                    """)
                    
                    if sin_categoria > 0:
                        st.warning(f"⚠️ {sin_categoria} productos no tienen categoría en el inventario. Verifica que el 'Codigo' en ventas coincida con 'Id' en inventario.")
                    
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.exception(e)

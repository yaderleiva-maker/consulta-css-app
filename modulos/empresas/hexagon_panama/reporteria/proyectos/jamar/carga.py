def guardar_gestiones_jamar(df, proyecto_id):
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    registros_guardados = 0
    
    mapeo = obtener_mapeo_codigos()
    if not mapeo:
        st.warning(
            "No se encontró mapeo de códigos. "
            "Las gestiones se guardarán sin mejor_gestion_jamar ni resultado_gestion."
        )
        mapeo = {}

    if df.empty:
        st.warning("El archivo no contiene datos.")
        return 0, total, "Archivo vacio"
    
    registros = []
    llaves_procesadas = set()  # Para evitar duplicados dentro del mismo archivo
    
    for idx, row in df.iterrows():
        try:
            llave_raw = row.get('Llave')
            
            codigo_agencia = normalizar_texto(
                row.get("Codigo de la Agencia", row.get("Código de la Agencia"))
            )
            numero_cuenta = normalizar_texto(
                row.get("Número de Cuenta", row.get("Numero de Cuenta"))
            )
            codigo_cliente = normalizar_texto(
                row.get("Codigo del Cliente", row.get("Código del Cliente"))
            )
            codigo_cobrador = normalizar_texto(
                row.get("Codigo del cobrador", row.get("Código del cobrador"))
            )
            
            if pd.isna(llave_raw) or not str(llave_raw).strip():
                if codigo_agencia and numero_cuenta:
                    llave = f"{codigo_agencia}{numero_cuenta}"
                else:
                    llave = None
            else:
                llave = normalizar_texto(llave_raw)
            
            # 🔥 IMPORTANTE: Si no hay llave, no podemos identificar la cuenta
            if not llave:
                errores += 1
                detalles.append(f"Fila {idx+2}: No se pudo generar llave")
                continue
            
            codigo_gestion = normalizar_texto(row.get('codigo_gestion'))
            
            mejor_gestion = None
            resultado = None
            if codigo_gestion and codigo_gestion in mapeo:
                mejor_gestion, resultado = mapeo[codigo_gestion]
            
            fechahora = normalizar_fecha_hora(row.get('fechahoragestion'))
            fechapromesa = normalizar_fecha(row.get('fechapromesa'))
            fecha = normalizar_fecha(row.get('Fecha'))
            
            valorpromesa = normalizar_numero(row.get('valorpromesa'))
            min_prioridad = None
            try:
                val = row.get('MinDePrioridad')
                if pd.notna(val):
                    min_prioridad = int(float(val))
            except:
                pass
            
            # 🔥 IMPORTANTE: id_gestion SIEMPRE único (UUID)
            id_gestion = str(uuid.uuid4())
            
            # Verificar duplicados dentro del mismo archivo (misma llave + misma fecha)
            clave_unica = (llave, fechahora) if fechahora else (llave, fecha)
            if clave_unica in llaves_procesadas:
                continue  # Saltar duplicados dentro del mismo archivo
            llaves_procesadas.add(clave_unica)
            
            registro = {
                'id_gestion': id_gestion,
                'id_proyecto': PROYECTO_ID,
                'llave': llave,
                'codigo_agencia': codigo_agencia,
                'numero_cuenta': numero_cuenta,
                'codigo_cliente': codigo_cliente,
                'fechahoragestion': fechahora,
                'codigo_gestion': codigo_gestion,
                'observacion': normalizar_texto(row.get('Observacion')),
                'codigo_cobrador': codigo_cobrador,
                'area_gestion': normalizar_texto(row.get('area_gestion')),
                'tipo_gestion': normalizar_texto(row.get('tipo_gestion')),
                'numeromarcado': normalizar_texto(row.get('numeromarcado')),
                'tipo_telefono': normalizar_texto(row.get('tipo_telefono')),
                'fechapromesa': fechapromesa,
                'valorpromesa': valorpromesa,
                'mejor_gestion_jamar': mejor_gestion,
                'resultado_gestion': resultado,
                'lugar_contacto': normalizar_texto(row.get('lugar_contacto')),
                'tipo_contacto': normalizar_texto(row.get('tipo_contacto')),
                'clave': normalizar_texto(row.get('Clave')),
                'fecha': fecha,
                'min_de_prioridad': min_prioridad,
                'clave_min': normalizar_texto(row.get('ClaveMin')),
                'fecha_carga': datetime.now().isoformat(),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            registros.append(registro)
            
        except Exception as e:
            errores += 1
            st.warning(f"Error en fila {idx+2}: {str(e)}")
    
    if not registros:
        st.warning("No hay datos validos para insertar")
        return 0, total, "No hay datos validos"
    
    df_insert = pd.DataFrame(registros)
    
    # ... (el resto del código de conversión de fechas y carga a BigQuery)

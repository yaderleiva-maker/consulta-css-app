import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
import re

# Importar servicios de Hexagon
from services.bigquery import ejecutar_query
from services.archivos import leer_excel, validar_columnas

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO = "SOL"  # Código interno de Corporación El Sol
COLUMNAS_REQUERIDAS = ['identificacion', 'nombre', 'cuenta', 'saldo']
COLUMNAS_OPCIONALES = ['telefono', 'correo', 'empresa', 'direccion', 'ocupacion', 
                       'fecha_ultimo_pago', 'dias_mora', 'cartera', 'observaciones']

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_identificacion(valor):
    """Mantiene la identificación exactamente como llega (con guiones, puntos, etc.)"""
    if pd.isna(valor):
        return None
    return str(valor).strip()

def normalizar_nombre(valor):
    """Convierte nombre a MAYÚSCULAS y elimina espacios dobles"""
    if pd.isna(valor):
        return None
    nombre = str(valor).strip().upper()
    return ' '.join(nombre.split())

def normalizar_telefonos(valor):
    """Separa teléfonos por coma, limpia espacios y elimina duplicados"""
    if pd.isna(valor):
        return []
    # Si es un string, separar por comas o puntos y comas
    if isinstance(valor, str):
        # Reemplazar ; por , para uniformar
        valor = valor.replace(';', ',')
        # Separar por coma
        telefonos = [t.strip() for t in valor.split(',') if t.strip()]
        # Eliminar duplicados manteniendo orden
        telefonos = list(dict.fromkeys(telefonos))
        return telefonos
    return []

def normalizar_correos(valor):
    """Separa correos por coma, limpia espacios y elimina duplicados"""
    if pd.isna(valor):
        return []
    if isinstance(valor, str):
        valor = valor.replace(';', ',')
        correos = [c.strip().lower() for c in valor.split(',') if c.strip()]
        correos = list(dict.fromkeys(correos))
        return correos
    return []

def normalizar_saldo(valor):
    """Convierte saldo a float, manejando formatos con comas y puntos"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        # Eliminar símbolos de moneda y espacios
        limpiar = re.sub(r'[^\d.,-]', '', valor)
        # Reemplazar coma por punto (formato europeo)
        limpiar = limpiar.replace(',', '.')
        try:
            return float(limpiar)
        except:
            return None
    return None

def normalizar_fecha(valor):
    """Convierte a formato DATE (YYYY-MM-DD)"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.date().isoformat()
    if isinstance(valor, str):
        # Intentar varios formatos
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
            try:
                return datetime.strptime(valor.strip(), fmt).date().isoformat()
            except:
                continue
    return None

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

def obtener_historial_cargas(proyecto, limite=20):
    """Obtiene el historial de cargas de un proyecto"""
    query = f"""
        SELECT 
            fecha_carga,
            registros,
            procesados,
            errores,
            estado
        FROM `proyecto-css-panama.cobranza.historial_cargas`
        WHERE id_proyecto = '{proyecto}'
        ORDER BY fecha_carga DESC
        LIMIT {limite}
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.warning(f"⚠️ No se pudo obtener historial: {str(e)}")
        return pd.DataFrame()

def registrar_carga_en_bigquery(proyecto, registros, procesados, errores, estado, detalle=None):
    """Registra una carga en el historial de BigQuery"""
    id_carga = str(uuid.uuid4())
    query = f"""
        INSERT INTO `proyecto-css-panama.cobranza.historial_cargas`
        (id_carga, id_proyecto, fecha_carga, registros, procesados, errores, estado, detalle)
        VALUES (
            '{id_carga}',
            '{proyecto}',
            CURRENT_TIMESTAMP(),
            {registros},
            {procesados},
            {errores},
            '{estado}',
            '{detalle or ""}'
        )
    """
    try:
        ejecutar_query(query)
        return True
    except Exception as e:
        st.error(f"❌ Error al registrar carga: {str(e)}")
        return False

# ============================================================
# PROCESO DE INGESTA
# ============================================================

def procesar_carga(df, proyecto):
    """
    Procesa el DataFrame y lo descompone en las tablas de BigQuery
    Retorna: (total, procesados, errores, detalle)
    """
    total = len(df)
    procesados = 0
    errores = 0
    detalles = []

    # Validar columnas requeridas
    faltantes = validar_columnas(df, COLUMNAS_REQUERIDAS)
    if faltantes:
        return total, 0, total, f"Faltan columnas: {', '.join(faltantes)}"

    for idx, row in df.iterrows():
        try:
            # 1. Normalizar datos
            identificacion = normalizar_identificacion(row.get('identificacion'))
            nombre = normalizar_nombre(row.get('nombre'))
            cuenta = str(row.get('cuenta', '')).strip()
            saldo = normalizar_saldo(row.get('saldo'))
            
            # Validar datos obligatorios
            if not identificacion or not nombre or not cuenta or saldo is None:
                errores += 1
                detalles.append(f"Fila {idx+2}: Datos obligatorios incompletos")
                continue

            # 2. Buscar o crear PERSONA
            persona_query = f"""
                SELECT id_persona 
                FROM `proyecto-css-panama.cobranza.personas`
                WHERE identificacion = '{identificacion}'
            """
            persona_df = ejecutar_query(persona_query)
            
            if len(persona_df) == 0:
                id_persona = str(uuid.uuid4())
                insert_persona = f"""
                    INSERT INTO `proyecto-css-panama.cobranza.personas`
                    (id_persona, identificacion, nombre)
                    VALUES ('{id_persona}', '{identificacion}', '{nombre}')
                """
                ejecutar_query(insert_persona)
            else:
                id_persona = persona_df['id_persona'].iloc[0]
                # Actualizar nombre si cambió
                update_persona = f"""
                    UPDATE `proyecto-css-panama.cobranza.personas`
                    SET nombre = '{nombre}', updated_at = CURRENT_TIMESTAMP()
                    WHERE id_persona = '{id_persona}'
                """
                ejecutar_query(update_persona)

            # 3. Crear CUENTA
            id_cuenta = str(uuid.uuid4())
            obligacion = str(row.get('obligacion', '')).strip() if pd.notna(row.get('obligacion')) else None
            empresa = str(row.get('empresa', '')).strip() if pd.notna(row.get('empresa')) else None
            direccion = str(row.get('direccion', '')).strip() if pd.notna(row.get('direccion')) else None
            ocupacion = str(row.get('ocupacion', '')).strip() if pd.notna(row.get('ocupacion')) else None
            dias_mora = int(row.get('dias_mora')) if pd.notna(row.get('dias_mora')) else None
            cartera = str(row.get('cartera', '')).strip() if pd.notna(row.get('cartera')) else None
            observaciones = str(row.get('observaciones', '')).strip() if pd.notna(row.get('observaciones')) else None
            fecha_ultimo_pago = normalizar_fecha(row.get('fecha_ultimo_pago')) if pd.notna(row.get('fecha_ultimo_pago')) else None
            
            insert_cuenta = f"""
                INSERT INTO `proyecto-css-panama.cobranza.cuentas`
                (id_cuenta, id_persona, id_proyecto, cuenta, obligacion, saldo, 
                 fecha_ultimo_pago, empresa, direccion, ocupacion, dias_mora, cartera, observaciones)
                VALUES (
                    '{id_cuenta}',
                    '{id_persona}',
                    '{proyecto}',
                    '{cuenta}',
                    {f"'{obligacion}'" if obligacion else 'NULL'},
                    {saldo},
                    {f"'{fecha_ultimo_pago}'" if fecha_ultimo_pago else 'NULL'},
                    {f"'{empresa}'" if empresa else 'NULL'},
                    {f"'{direccion}'" if direccion else 'NULL'},
                    {f"'{ocupacion}'" if ocupacion else 'NULL'},
                    {dias_mora if dias_mora is not None else 'NULL'},
                    {f"'{cartera}'" if cartera else 'NULL'},
                    {f"'{observaciones}'" if observaciones else 'NULL'}
                )
            """
            ejecutar_query(insert_cuenta)

            # 4. Procesar TELÉFONOS
            telefonos = normalizar_telefonos(row.get('telefono'))
            for i, telefono in enumerate(telefonos):
                # Buscar o crear teléfono
                tel_query = f"""
                    SELECT id_telefono 
                    FROM `proyecto-css-panama.cobranza.telefonos`
                    WHERE numero = '{telefono}'
                """
                tel_df = ejecutar_query(tel_query)
                
                if len(tel_df) == 0:
                    id_telefono = str(uuid.uuid4())
                    insert_tel = f"""
                        INSERT INTO `proyecto-css-panama.cobranza.telefonos`
                        (id_telefono, numero)
                        VALUES ('{id_telefono}', '{telefono}')
                    """
                    ejecutar_query(insert_tel)
                else:
                    id_telefono = tel_df['id_telefono'].iloc[0]

                # Crear relación en telefonos_proyecto
                id_rel = str(uuid.uuid4())
                insert_rel_tel = f"""
                    INSERT INTO `proyecto-css-panama.cobranza.telefonos_proyecto`
                    (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
                    VALUES (
                        '{id_telefono}',
                        '{id_persona}',
                        '{proyecto}',
                        'CARGA_INICIAL',
                        {i+1},
                        'ACTIVO'
                    )
                """
                ejecutar_query(insert_rel_tel)

            # 5. Procesar CORREOS
            correos = normalizar_correos(row.get('correo'))
            for i, correo in enumerate(correos):
                # Buscar o crear correo
                corr_query = f"""
                    SELECT id_correo 
                    FROM `proyecto-css-panama.cobranza.correos`
                    WHERE correo = '{correo}'
                """
                corr_df = ejecutar_query(corr_query)
                
                if len(corr_df) == 0:
                    id_correo = str(uuid.uuid4())
                    insert_corr = f"""
                        INSERT INTO `proyecto-css-panama.cobranza.correos`
                        (id_correo, correo)
                        VALUES ('{id_correo}', '{correo}')
                    """
                    ejecutar_query(insert_corr)
                else:
                    id_correo = corr_df['id_correo'].iloc[0]

                # Crear relación en correos_proyecto
                id_rel = str(uuid.uuid4())
                insert_rel_corr = f"""
                    INSERT INTO `proyecto-css-panama.cobranza.correos_proyecto`
                    (id_correo, id_persona, id_proyecto, fuente, prioridad, estado)
                    VALUES (
                        '{id_correo}',
                        '{id_persona}',
                        '{proyecto}',
                        'CARGA_INICIAL',
                        {i+1},
                        'ACTIVO'
                    )
                """
                ejecutar_query(insert_rel_corr)

            procesados += 1

        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")

    detalle = f"{procesados} procesados, {errores} errores"
    if detalles:
        detalle += f" | Primeros errores: {', '.join(detalles[:3])}"
    
    return total, procesados, errores, detalle

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para la vista de Carga de Cartera"""
    
    st.markdown("""
    <style>
        .main-header {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
        }
        .sub-header {
            font-size: 14px;
            color: #6b6b6b;
            margin-bottom: 24px;
        }
        .card {
            background-color: #ffffff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
            border: 1px solid #f0f0f0;
            margin-bottom: 16px;
        }
        .card-title {
            font-size: 16px;
            font-weight: 500;
            color: #1a1a1a;
            margin-bottom: 12px;
        }
        .drag-area {
            border: 2px dashed #d1d5db;
            border-radius: 12px;
            padding: 48px 24px;
            text-align: center;
            background-color: #fafafa;
            transition: border-color 0.2s;
            cursor: pointer;
        }
        .drag-area:hover {
            border-color: #dc2626;
            background-color: #fef2f2;
        }
        .drag-area .icon {
            font-size: 48px;
            color: #9ca3af;
            margin-bottom: 12px;
        }
        .drag-area .text-primary {
            font-size: 16px;
            font-weight: 500;
            color: #1a1a1a;
        }
        .drag-area .text-secondary {
            font-size: 14px;
            color: #6b6b6b;
        }
        .btn-primary {
            background-color: #dc2626;
            color: white;
            border: none;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn-primary:hover {
            background-color: #b91c1c;
        }
        .btn-outline {
            background-color: transparent;
            color: #dc2626;
            border: 1px solid #dc2626;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn-outline:hover {
            background-color: #fef2f2;
        }
        .status-success {
            color: #16a34a;
            font-weight: 500;
        }
        .status-warning {
            color: #ea580c;
            font-weight: 500;
        }
        .status-error {
            color: #dc2626;
            font-weight: 500;
        }
        .history-item {
            display: flex;
            justify-content: space-between;
            padding: 12px 0;
            border-bottom: 1px solid #f3f4f6;
        }
        .history-item:last-child {
            border-bottom: none;
        }
        .history-date {
            color: #6b6b6b;
            font-size: 14px;
        }
        .history-count {
            font-weight: 500;
        }
    </style>
    """, unsafe_allow_html=True)

    # ============================================================
    # HEADER
    # ============================================================
    
    st.markdown('<div class="main-header">📥 Carga de Cartera</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el archivo con la cartera de clientes para procesar en Hexagon. El sistema validará, normalizará y distribuirá la información automáticamente.</div>', unsafe_allow_html=True)

    # ============================================================
    # INSTRUCCIONES + PLANTILLA
    # ============================================================
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("""
        <div class="card">
            <div class="card-title">📋 Instrucciones para la carga</div>
            <ul style="margin: 0; padding-left: 20px; color: #4b5563; font-size: 14px; line-height: 1.8;">
                <li>Usa el <strong>formato oficial de Hexagon</strong> para la carga.</li>
                <li>Los teléfonos y correos deben ir en <strong>una sola columna</strong>, separados por coma.</li>
                <li>Las columnas obligatorias son: <strong>identificacion, nombre, cuenta, saldo</strong>.</li>
                <li>Formatos permitidos: <strong>.xlsx, .xls, .csv</strong></li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div style="display: flex; justify-content: flex-end; height: 100%; align-items: center;">
            <button class="btn-outline" style="width: 100%;">📄 Descargar Plantilla</button>
        </div>
        """, unsafe_allow_html=True)

    # ============================================================
    # ÁREA DE CARGA
    # ============================================================
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
        key="carga_cartera_uploader"
    )

    if uploaded_file is not None:
        with st.spinner("📊 Procesando archivo..."):
            try:
                # Leer archivo
                df = leer_excel(uploaded_file)
                
                # Validar columnas
                faltantes = validar_columnas(df, COLUMNAS_REQUERIDAS)
                
                if faltantes:
                    st.error(f"⚠️ Faltan columnas obligatorias: {', '.join(faltantes)}")
                    st.stop()
                
                # Mostrar vista previa
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown('<div class="card-title">📊 Vista previa del archivo</div>', unsafe_allow_html=True)
                
                st.dataframe(df.head(10), use_container_width=True)
                
                # Estadísticas básicas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    st.metric("Teléfonos", f"{df['telefono'].notna().sum() if 'telefono' in df.columns else 0:,}")
                with col3:
                    st.metric("Correos", f"{df['correo'].notna().sum() if 'correo' in df.columns else 0:,}")
                with col4:
                    st.metric("Empresas", f"{df['empresa'].notna().sum() if 'empresa' in df.columns else 0:,}")
                
                # Botón para procesar
                if st.button("🚀 Procesar carga", type="primary", use_container_width=True):
                    with st.spinner("🔄 Procesando carga en BigQuery..."):
                        total, procesados, errores, detalle = procesar_carga(df, PROYECTO)
                        
                        # Registrar en historial
                        estado = "completada" if errores == 0 else "con_errores"
                        registrar_carga_en_bigquery(PROYECTO, total, procesados, errores, estado, detalle)
                        
                        # Mostrar resultados
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📊 Total", f"{total:,}")
                        with col2:
                            st.metric("✅ Procesados", f"{procesados:,}", delta=f"{procesados/total*100:.1f}%")
                        with col3:
                            st.metric("❌ Errores", f"{errores:,}", delta=f"{-errores/total*100:.1f}%" if errores > 0 else "0%")
                        
                        if errores == 0:
                            st.success("🎉 Carga completada exitosamente. Todos los registros fueron procesados.")
                        else:
                            st.warning(f"⚠️ Carga completada con {errores} errores. Revisa el detalle: {detalle}")
                        
                        # Botón para ir al dashboard
                        if st.button("📊 Ver Dashboard", use_container_width=True):
                            st.session_state['pagina_actual'] = "Dashboard"
                            st.rerun()
                
                st.markdown('</div>', unsafe_allow_html=True)

            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)

    # ============================================================
    # HISTORIAL DE CARGAS
    # ============================================================
    
    st.markdown("""
    <div class="card">
        <div class="card-title">📋 Últimas cargas</div>
    """, unsafe_allow_html=True)

    historial_df = obtener_historial_cargas(PROYECTO)
    
    if len(historial_df) > 0:
        for _, row in historial_df.iterrows():
            fecha = row['fecha_carga'].strftime('%d/%m/%Y %H:%M') if hasattr(row['fecha_carga'], 'strftime') else str(row['fecha_carga'])
            registros = int(row['registros'])
            estado = row['estado']
            icono = "✅" if estado == "completada" else "⚠️"
            clase = "status-success" if estado == "completada" else "status-warning"
            
            st.markdown(f"""
            <div class="history-item">
                <div>
                    <span class="history-date">{fecha}</span>
                    <span style="margin-left: 16px;" class="history-count">{registros:,} registros</span>
                </div>
                <div>
                    <span class="{clase}">{icono} {estado.replace('_', ' ').title()}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="text-align: center; padding: 24px; color: #9ca3af; font-size: 14px;">
            No hay cargas registradas aún.
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # ============================================================
    # FOOTER
    # ============================================================
    
    st.markdown("""
    <div style="text-align: center; margin-top: 32px; font-size: 12px; color: #9ca3af; border-top: 1px solid #f0f0f0; padding-top: 16px;">
        Hexagon · Cobranza · Corporación El Sol · Versión 1.0
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# EJECUCIÓN DIRECTA (para pruebas)
# ============================================================

if __name__ == "__main__":
    render()

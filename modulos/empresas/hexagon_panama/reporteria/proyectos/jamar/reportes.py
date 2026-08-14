import streamlit as st
import pandas as pd
from datetime import datetime
import io

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"

# ============================================================
# FUNCIÓN PARA PREPARAR DATAFRAME PARA EXCEL
# ============================================================

def preparar_para_excel(df):
    """Excel no acepta timestamps con zona horaria."""
    resultado = df.copy()
    for columna in resultado.select_dtypes(include=["datetimetz"]).columns:
        resultado[columna] = resultado[columna].dt.tz_localize(None)
    return resultado

# ============================================================
# FUNCIÓN PARA OBTENER MAPEO DE CÓDIGOS
# ============================================================

def obtener_mapeo_codigos():
    """Obtiene el mapeo de códigos de gestión desde BigQuery"""
    query = """
    SELECT 
        codigo_gestion,
        resultado,
        prioridad
    FROM `proyecto-css-panama.cobranza.mapeo_codigos_gestion`
    """
    df = ejecutar_query(query)
    return df

# ============================================================
# REPORTE ÚNICO: Resumen de Cartera (Consolidado)
# ============================================================

def generar_resumen_cartera(proyecto_id, fecha_reporte=None):
    """
    Genera un Excel consolidado con:
    - Hoja 1: Resumen ejecutivo (métricas clave)
    - Hoja 2: Cuadro de Gestión por RANK
    - Hoja 3: Cuadro de Resultado de Gestión
    - Hoja 4: Recaudo y Compromisos
    - Hoja 5: DATOS COMPLETOS (TABLA MAESTRA - Fuente de la verdad)
    """
    
    output = io.BytesIO()
    mensajes = []
    df_mapeo = obtener_mapeo_codigos()
    # 🔥 CONSTRUIR FILTRO DE FECHA
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"""
        AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'
        AND DATE(p.fecha_up) = '{fecha_reporte_str}'
        """
    
    # ============================================================
    # PASO 1: CONSTRUIR DATOS COMPLETOS (TABLA MAESTRA) CON FILTRO
    # ============================================================
    
    query_datos_completos = f"""
    WITH pagos_agrupados AS (
        SELECT 
            llave,
            COUNT(*) AS cantidad_pagos,
            SUM(recaudo_periodo) AS total_recaudo,
            MAX(fecha_up) AS fecha_ultimo_pago_real,
            SUM(saldo) AS saldo_pagos
        FROM `{PROYECTO_BQ}.pagos_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          -- 🔥 FILTRO DE FECHA PARA PAGOS
          {filtro_fechas.replace('DATE(g.fechahoragestion)', 'DATE(fecha_up)').replace("AND DATE(p.fecha_up)", "AND DATE(fecha_up)") if fecha_reporte else ''}
        GROUP BY llave
    ),
    
    gestiones_agrupadas AS (
        SELECT 
            llave,
            COUNT(*) AS total_toques,
            MAX(fechahoragestion) AS fecha_ultima_gestion,
            COUNT(CASE WHEN resultado_gestion = 'COMPROMISO DE PAGO' THEN 1 END) AS promesas,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO EFECTIVO' THEN 1 END) AS contactos,
            COUNT(CASE WHEN resultado_gestion = 'NO CONTACTOS' THEN 1 END) AS no_contactos,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO CON TERCERO' THEN 1 END) AS contactos_tercero,
            
            -- Últimos valores
            ARRAY_AGG(resultado_gestion ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultimo_resultado,
            ARRAY_AGG(mejor_gestion_jamar ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultima_mejor_gestion,
            ARRAY_AGG(SAFE_CAST(codigo_gestion AS STRING) ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultimo_codigo_gestion,
            ARRAY_AGG(fechahoragestion ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultima_fecha_gestion,
            ARRAY_AGG(numeromarcado ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultimo_numeromarcado,
            
            -- Canal de la última gestión
            ARRAY_AGG(
                CASE 
                    WHEN SAFE_CAST(codigo_gestion AS STRING) = '90' AND (numeromarcado IS NULL OR numeromarcado IN ('0', '00000000', '')) THEN 'CORREO'
                    WHEN SAFE_CAST(codigo_gestion AS STRING) = '90' AND numeromarcado IS NOT NULL AND numeromarcado NOT IN ('0', '00000000', '') AND LENGTH(numeromarcado) >= 7 THEN 'WHATSAPP'
                    ELSE 'LLAMADA'
                END
                ORDER BY fechahoragestion DESC 
                LIMIT 1
            )[OFFSET(0)] AS ultimo_canal,
            
            -- Totales por canal
            COUNT(CASE WHEN SAFE_CAST(codigo_gestion AS STRING) = '90' AND (numeromarcado IS NULL OR numeromarcado IN ('0', '00000000', '')) THEN 1 END) AS total_correos,
            COUNT(CASE WHEN SAFE_CAST(codigo_gestion AS STRING) = '90' AND numeromarcado IS NOT NULL AND numeromarcado NOT IN ('0', '00000000', '') AND LENGTH(numeromarcado) >= 7 THEN 1 END) AS total_whatsapps,
            COUNT(CASE WHEN SAFE_CAST(codigo_gestion AS STRING) != '90' THEN 1 END) AS total_llamadas
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
          -- 🔥 FILTRO DE FECHA PARA GESTIONES
          {filtro_fechas.replace('DATE(p.fecha_up)', 'DATE(g.fechahoragestion)').replace("AND DATE(g.fechahoragestion)", "AND DATE(g.fechahoragestion)") if fecha_reporte else ''}
        GROUP BY llave
    ),
    
    ultima_promesa_valor AS (
        SELECT 
            llave,
            valorpromesa,
            fechapromesa
        FROM `{PROYECTO_BQ}.gestiones_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          AND SAFE_CAST(codigo_gestion AS STRING) IN ('1', '88', '89')
          AND valorpromesa IS NOT NULL
          AND valorpromesa > 0
          -- 🔥 FILTRO DE FECHA PARA PROMESAS
          {filtro_fechas.replace('DATE(p.fecha_up)', 'DATE(fechapromesa)').replace('DATE(g.fechahoragestion)', 'DATE(fechapromesa)').replace("AND DATE(g.fechahoragestion)", "AND DATE(fechapromesa)") if fecha_reporte else ''}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY llave ORDER BY fechapromesa DESC) = 1
    ),
    
    datos_con_categoria AS (
        SELECT 
            c.llave,
            c.codigo_agencia,
            c.numero_cuenta,
            c.codigo_cliente,
            c.nombre_cliente,
            c.estado_inicial,
            c.tramo_inicial,
            c.rank,
            c.saldo_total_adeudado,
            c.saldo_total_vencido,
            c.fecha_ultimo_pago AS fecha_ultimo_pago_cartera,
            
            -- Datos de pagos
            p.cantidad_pagos,
            p.total_recaudo,
            p.fecha_ultimo_pago_real,
            
            -- Datos de gestiones (raw)
            g.total_toques,
            g.fecha_ultima_gestion,
            g.promesas,
            g.contactos,
            g.no_contactos,
            g.contactos_tercero,
            g.ultimo_resultado,
            g.ultima_mejor_gestion,
            g.ultimo_codigo_gestion,
            g.ultima_fecha_gestion,
            g.ultimo_numeromarcado,
            
            -- Datos de canal
            g.ultimo_canal,
            g.total_correos,
            g.total_whatsapps,
            g.total_llamadas,
            
            -- Promesas
            up.valorpromesa AS ultimo_valor_promesa,
            up.fechapromesa AS fecha_ultima_promesa,
            
            -- CATEGORÍA FINAL
            CASE 
                WHEN g.llave IS NULL THEN 'SIN GESTION AL CORTE'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) = '90' THEN 
                    CASE 
                        WHEN g.ultimo_numeromarcado IS NOT NULL 
                             AND g.ultimo_numeromarcado NOT IN ('0', '00000000', '')
                             AND LENGTH(g.ultimo_numeromarcado) >= 7 
                        THEN 'WHATSAPP'
                        ELSE 'CONTACTO EFECTIVO'
                    END
                WHEN g.ultimo_resultado IS NOT NULL THEN
                    CASE 
                        WHEN g.ultimo_resultado = 'CONTACTO EFECTIVO' THEN 'CONTACTO EFECTIVO'
                        WHEN g.ultimo_resultado = 'COMPROMISO DE PAGO' THEN 'COMPROMISO DE PAGO'
                        WHEN g.ultimo_resultado = 'NO CONTACTOS' THEN 'SIN CONTACTO'
                        WHEN g.ultimo_resultado = 'CONTACTO CON TERCERO' THEN 'CONTACTO CON TERCERO'
                        WHEN g.ultimo_resultado IN ('Tono ocupado', 'Equivocado', 'Ilocalizable') THEN 'NO CONTACTOS'
                        ELSE 'SIN CONTACTO'
                    END
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) IN ('0') THEN 'CONTACTO CON TERCERO'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) IN ('1', '01', '88', '89') THEN 'COMPROMISO DE PAGO'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) IN ('14', '81', '84') THEN 'CONTACTO EFECTIVO'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) = '86' THEN 'SIN CONTACTO'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) IN ('10', '11', '15') THEN 'NO CONTACTOS'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) = '83' THEN 'NO CONTACTOS'
                ELSE 'SIN CONTACTO'
            END AS categoria_final,
            
            -- RAZÓN DE LA CATEGORÍA
            CASE 
                WHEN g.llave IS NULL THEN 'Sin gestión en el período'
                WHEN SAFE_CAST(g.ultimo_codigo_gestion AS STRING) = '90' THEN 
                    CASE 
                        WHEN g.ultimo_numeromarcado IS NOT NULL 
                             AND g.ultimo_numeromarcado NOT IN ('0', '00000000', '')
                             AND LENGTH(g.ultimo_numeromarcado) >= 7 
                        THEN CONCAT('codigo_gestion=90 con teléfono → WHATSAPP (', g.ultimo_numeromarcado, ')')
                        ELSE 'codigo_gestion=90 sin teléfono → CORREO'
                    END
                WHEN g.ultimo_resultado IS NOT NULL THEN 
                    CONCAT('resultado_gestion=', g.ultimo_resultado)
                WHEN g.ultimo_codigo_gestion IS NOT NULL THEN 
                    CONCAT('codigo_gestion=', SAFE_CAST(g.ultimo_codigo_gestion AS STRING), ' (sin resultado_gestion)')
                ELSE 'Sin clasificación'
            END AS razon_categoria,
            
            -- ESTADO DE LA PROMESA
            CASE 
                WHEN up.fechapromesa IS NULL THEN 'SIN PROMESA'
                WHEN up.fechapromesa >= CURRENT_DATE('America/Bogota') THEN 'ACTIVA'
                ELSE 'INCUMPLIDA'
            END AS estado_promesa
            
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN pagos_agrupados p ON c.llave = p.llave
        LEFT JOIN gestiones_agrupadas g ON c.llave = g.llave
        LEFT JOIN ultima_promesa_valor up ON c.llave = up.llave
        WHERE c.id_proyecto = '{proyecto_id}'
    )
    
    SELECT 
        llave,
        codigo_agencia,
        numero_cuenta,
        codigo_cliente,
        nombre_cliente,
        estado_inicial,
        tramo_inicial,
        rank,
        saldo_total_adeudado,
        saldo_total_vencido,
        fecha_ultimo_pago_cartera,
        cantidad_pagos,
        total_recaudo,
        fecha_ultimo_pago_real,
        total_toques,
        fecha_ultima_gestion,
        promesas,
        contactos,
        no_contactos,
        contactos_tercero,
        ultimo_resultado,
        ultima_mejor_gestion,
        ultimo_codigo_gestion,
        ultima_fecha_gestion,
        ultimo_canal,
        total_correos,
        total_whatsapps,
        total_llamadas,
        ultimo_valor_promesa,
        fecha_ultima_promesa,
        estado_promesa,
        categoria_final,
        razon_categoria
    FROM datos_con_categoria
    ORDER BY saldo_total_adeudado DESC
    """
    
    df_datos = ejecutar_query(query_datos_completos)
    df_datos = preparar_para_excel(df_datos)
    
    if df_datos.empty:
        return None, "⚠️ No hay datos disponibles."
    
    mensajes.append(f"✅ Datos completos: {len(df_datos):,} cuentas")

    
    # ============================================================
    # PASO 2: GENERAR RESÚMENES A PARTIR DE df_datos
    # ============================================================
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        # ============================================================
        # HOJA 1: RESUMEN EJECUTIVO
        # ============================================================
        
        resumen_data = {
            'Métrica': [
                'Proyecto',
                'Fecha de generación',
                'Total de cuentas',
                'Saldo total adeudado',
                'Saldo promedio',
                'Saldo máximo',
                'Saldo mínimo',
                'Total de ranks',
                'Total de gestiones',
                'Cuentas gestionadas',
                'Cuentas sin gestión',
                'Total de pagos',
                'Total recaudado',
                'Cuentas con pago',
                'Cuentas con promesa activa',
                'Cuentas con promesa incumplida'
            ],
            'Valor': [
                proyecto_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                f"{len(df_datos):,}",
                f"${df_datos['saldo_total_adeudado'].sum():,.2f}",
                f"${df_datos['saldo_total_adeudado'].mean():,.2f}",
                f"${df_datos['saldo_total_adeudado'].max():,.2f}",
                f"${df_datos['saldo_total_adeudado'].min():,.2f}",
                f"{df_datos['rank'].nunique():,}",
                f"{df_datos['total_toques'].sum():,}",
                f"{df_datos[df_datos['total_toques'] > 0].shape[0]:,}",
                f"{df_datos[df_datos['total_toques'].isna()].shape[0]:,}",
                f"{df_datos['cantidad_pagos'].sum():,}",
                f"${df_datos['total_recaudo'].sum():,.2f}",
                f"{df_datos[df_datos['cantidad_pagos'] > 0].shape[0]:,}",
                f"{df_datos[df_datos['estado_promesa'] == 'ACTIVA'].shape[0]:,}",
                f"{df_datos[df_datos['estado_promesa'] == 'INCUMPLIDA'].shape[0]:,}"
            ]
        }
        df_resumen = pd.DataFrame(resumen_data)
        df_resumen.to_excel(writer, sheet_name='Resumen Ejecutivo', index=False)
        mensajes.append("✅ Resumen ejecutivo generado")
        
        # ============================================================
        # HOJA 2: CUADRO DE GESTIÓN POR RANK
        # ============================================================
        
        df_gestion_rank = df_datos.groupby('rank').agg({
            'llave': 'count',
            'total_toques': 'sum',
            'total_correos': 'sum',
            'total_whatsapps': 'sum',
            'total_llamadas': 'sum'
        }).reset_index()
        
        # Calcular intensidades
        df_gestion_rank['correo_intensidad'] = df_gestion_rank['total_correos'] / df_gestion_rank['llave']
        df_gestion_rank['whatsapp_intensidad'] = df_gestion_rank['total_whatsapps'] / df_gestion_rank['llave']
        df_gestion_rank['llamada_intensidad'] = df_gestion_rank['total_llamadas'] / df_gestion_rank['llave']
        df_gestion_rank['total_intensidad'] = df_gestion_rank['total_toques'] / df_gestion_rank['llave']
        
        df_gestion_rank.columns = [
            'rank', 'total_cuentas', 'total_toques', 
            'correo_cantidad', 'whatsapp_cantidad', 'llamada_cantidad',
            'correo_intensidad', 'whatsapp_intensidad', 'llamada_intensidad', 'total_intensidad'
        ]
        df_gestion_rank.to_excel(writer, sheet_name='Cuadro Gestión', index=False)
        mensajes.append(f"✅ Cuadro Gestión: {len(df_gestion_rank)} ranks")
        
        # ============================================================
        # HOJA 3: CUADRO DE RESULTADO DE GESTIÓN
        # ============================================================
        
        pivot_resultado = pd.crosstab(
            df_datos['categoria_final'], 
            df_datos['rank'],
            margins=True,
            margins_name='TOTAL'
        ).reset_index().rename(columns={'categoria_final': 'resultado_gestion'})
        
        # Reordenar según prioridad
        orden_categorias = [
            'CONTACTO EFECTIVO', 
            'WHATSAPP', 
            'COMPROMISO DE PAGO', 
            'SIN CONTACTO', 
            'CONTACTO CON TERCERO', 
            'NO CONTACTOS', 
            'SIN GESTION AL CORTE'
        ]
        
        pivot_resultado['orden'] = pivot_resultado['resultado_gestion'].map(
            {cat: i for i, cat in enumerate(orden_categorias)}
        )
        pivot_resultado['orden'] = pivot_resultado['orden'].fillna(999)
        pivot_resultado = pivot_resultado.sort_values('orden').drop('orden', axis=1)
        
        pivot_resultado.to_excel(writer, sheet_name='Resultado Gestión', index=False)
        mensajes.append(f"✅ Resultado Gestión: {len(pivot_resultado)} categorías")
        
        # ============================================================
        # HOJA 4: RECAUDO Y COMPROMISOS
        # ============================================================
        
        df_recaudo_final = pd.DataFrame({
            'resultado_gestion': ['RECAUDO', 'COMPROMISOS ACTIVOS', 'COMPROMISOS INCUMPLIDOS'],
            'A': [
                df_datos[df_datos['rank'] == 'A']['total_recaudo'].sum(),
                df_datos[(df_datos['rank'] == 'A') & (df_datos['estado_promesa'] == 'ACTIVA')]['ultimo_valor_promesa'].sum(),
                df_datos[(df_datos['rank'] == 'A') & (df_datos['estado_promesa'] == 'INCUMPLIDA')]['ultimo_valor_promesa'].sum()
            ],
            'B': [
                df_datos[df_datos['rank'] == 'B']['total_recaudo'].sum(),
                df_datos[(df_datos['rank'] == 'B') & (df_datos['estado_promesa'] == 'ACTIVA')]['ultimo_valor_promesa'].sum(),
                df_datos[(df_datos['rank'] == 'B') & (df_datos['estado_promesa'] == 'INCUMPLIDA')]['ultimo_valor_promesa'].sum()
            ],
            'C': [
                df_datos[df_datos['rank'] == 'C']['total_recaudo'].sum(),
                df_datos[(df_datos['rank'] == 'C') & (df_datos['estado_promesa'] == 'ACTIVA')]['ultimo_valor_promesa'].sum(),
                df_datos[(df_datos['rank'] == 'C') & (df_datos['estado_promesa'] == 'INCUMPLIDA')]['ultimo_valor_promesa'].sum()
            ],
            'D': [
                df_datos[df_datos['rank'] == 'D']['total_recaudo'].sum(),
                df_datos[(df_datos['rank'] == 'D') & (df_datos['estado_promesa'] == 'ACTIVA')]['ultimo_valor_promesa'].sum(),
                df_datos[(df_datos['rank'] == 'D') & (df_datos['estado_promesa'] == 'INCUMPLIDA')]['ultimo_valor_promesa'].sum()
            ],
            'TOTAL': [
                df_datos['total_recaudo'].sum(),
                df_datos[df_datos['estado_promesa'] == 'ACTIVA']['ultimo_valor_promesa'].sum(),
                df_datos[df_datos['estado_promesa'] == 'INCUMPLIDA']['ultimo_valor_promesa'].sum()
            ]
        })
        
        df_recaudo_final.to_excel(writer, sheet_name='Recaudo y Compromisos', index=False)
        mensajes.append("✅ Recaudo y Compromisos generado")
        
        # ============================================================
        # HOJA 5: DATOS COMPLETOS (TABLA MAESTRA)
        # ============================================================
        
        df_datos.to_excel(writer, sheet_name='Datos Completos', index=False)
        mensajes.append(f"✅ Datos completos: {len(df_datos):,} registros")
        
        # Ajustar ancho de columnas para todas las hojas
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 25, 18)
    
    mensaje_final = " | ".join(mensajes)
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Resumen de cartera generado ({fecha_str}) - {mensaje_final}"


# ============================================================
# REGISTRO DE REPORTES (SOLO EL CONSOLIDADO)
# ============================================================

REPORTES = {
    "📋 Resumen de Cartera (Consolidado)": generar_resumen_cartera,
}

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
# REPORTE 1: Resumen de Cuentas Pre-Demanda
# ============================================================

def generar_resumen_predemanda(proyecto_id, fecha_reporte=None):
    """Resumen de cuentas pre-demanda (acumulado)"""
    query = f"""
    WITH pagos_agrupados AS (
        SELECT 
            llave,
            COUNT(*) AS cantidad_pagos,
            SUM(saldo) AS total_saldo,
            SUM(recaudo_periodo) AS total_recaudo,
            MAX(fecha_up) AS ultimo_pago
        FROM `{PROYECTO_BQ}.pagos_jamar`
        WHERE id_proyecto = '{proyecto_id}'
        GROUP BY llave
    ),
    gestiones_agrupadas AS (
        SELECT 
            llave,
            MAX(fechahoragestion) AS ultima_gestion_fecha,
            MAX(CASE WHEN resultado_gestion = 'COMPROMISO DE PAGO' THEN fechahoragestion END) AS ultima_promesa,
            COUNT(*) AS total_gestiones,
            COUNT(CASE WHEN resultado_gestion = 'COMPROMISO DE PAGO' THEN 1 END) AS total_promesas,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO EFECTIVO' THEN 1 END) AS total_contactos,
            COUNT(CASE WHEN resultado_gestion = 'NO CONTACTOS' THEN 1 END) AS total_no_contactos,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO CON TERCERO' THEN 1 END) AS total_contactos_tercero
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
        GROUP BY llave
    )
    SELECT 
        c.llave,
        c.estado_inicial,
        c.tramo_inicial,
        c.codigo_agencia,
        c.numero_cuenta,
        c.codigo_cliente,
        c.nombre_cliente,
        c.rank,
        c.saldo_total_adeudado,
        c.saldo_total_vencido,
        c.fecha_ultimo_pago AS fecha_ultimo_pago_cartera,
        p.cantidad_pagos,
        p.total_recaudo,
        p.ultimo_pago AS fecha_ultimo_pago_real,
        g.ultima_gestion_fecha,
        g.ultima_promesa,
        g.total_gestiones,
        g.total_promesas,
        g.total_contactos,
        g.total_no_contactos,
        g.total_contactos_tercero,
        CASE 
            WHEN g.total_promesas > 0 AND p.cantidad_pagos > 0 THEN 'CUMPLE'
            WHEN g.total_promesas > 0 AND p.cantidad_pagos = 0 THEN 'PROMESA SIN PAGO'
            WHEN g.total_contactos > 0 AND p.cantidad_pagos = 0 THEN 'CONTACTADO SIN PAGO'
            ELSE 'SIN CONTACTO'
        END AS estado_cobranza
    FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
    LEFT JOIN pagos_agrupados p ON c.llave = p.llave
    LEFT JOIN gestiones_agrupadas g ON c.llave = g.llave
    WHERE c.id_proyecto = '{proyecto_id}'
    ORDER BY c.saldo_total_adeudado DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_resumen = pd.DataFrame({
            'Métrica': [
                'Proyecto',
                'Fecha de generación',
                'Total de cuentas',
                'Saldo total adeudado',
                'Cuentas con pago',
                'Cuentas con promesa',
                'Cuentas sin contacto',
                'Cuentas con gestión'
            ],
            'Valor': [
                proyecto_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                f"{len(df):,}",
                f"${df['saldo_total_adeudado'].sum():,.2f}",
                f"{df['cantidad_pagos'].notna().sum():,}",
                f"{df['total_promesas'].notna().sum():,}",
                f"{len(df[df['total_gestiones'].isna()]):,}",
                f"{df['total_gestiones'].notna().sum():,}"
            ]
        })
        df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        df.to_excel(writer, sheet_name='Datos', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Reporte generado con {len(df):,} registros"

# ============================================================
# REPORTE 2: Cartera Comercial
# ============================================================

def generar_cartera_comercial(proyecto_id, fecha_reporte=None):
    """Reporte simple de cartera comercial"""
    query = f"""
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
        fecha_ultimo_pago
    FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
    WHERE id_proyecto = '{proyecto_id}'
    ORDER BY saldo_total_adeudado DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Cartera', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Cartera generada con {len(df):,} registros"

# ============================================================
# REPORTE 3: Seguimiento de Cobro
# ============================================================

def generar_seguimiento_cobro(proyecto_id, fecha_reporte=None):
    """Reporte de seguimiento de cobro"""
    query = f"""
    SELECT 
        g.llave,
        g.codigo_cliente,
        g.fechahoragestion,
        g.codigo_gestion,
        g.mejor_gestion_jamar,
        g.resultado_gestion,
        g.observacion,
        g.numeromarcado,
        g.valorpromesa,
        g.fechapromesa,
        c.nombre_cliente,
        c.saldo_total_adeudado
    FROM `{PROYECTO_BQ}.gestiones_jamar` g
    LEFT JOIN `{PROYECTO_BQ}.cartera_predemanda_jamar` c 
        ON g.llave = c.llave AND c.id_proyecto = '{proyecto_id}'
    WHERE g.id_proyecto = '{proyecto_id}'
    ORDER BY g.llave, g.fechahoragestion DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay gestiones disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Seguimiento', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Seguimiento generado con {len(df):,} registros"

# ============================================================
# REPORTE 4: Cuadro de Gestión por RANK (individual)
# ============================================================

def generar_cuadro_gestion_rank(proyecto_id, fecha_reporte=None):
    """Genera el Excel del Cuadro de Gestión por RANK (reporte individual)"""
    
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'"
    
    query = f"""
    WITH gestiones_filtradas AS (
        SELECT 
            g.llave,
            g.codigo_cliente,
            g.codigo_gestion,
            g.tipo_gestion,
            g.area_gestion,
            c.rank,
            CASE 
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion = 'T' THEN 'WHATSAPP'
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion != 'T' THEN 'CORREO'
                ELSE 'LLAMADA'
            END AS tipo_gestion_real
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        LEFT JOIN `{PROYECTO_BQ}.cartera_predemanda_jamar` c 
            ON g.llave = c.llave AND c.id_proyecto = '{proyecto_id}'
        WHERE g.id_proyecto = '{proyecto_id}'
          {filtro_fechas}
    ),
    conteo_rank AS (
        SELECT 
            rank,
            tipo_gestion_real,
            COUNT(*) AS cantidad,
            COUNT(DISTINCT llave) AS cuentas_gestionadas
        FROM gestiones_filtradas
        WHERE rank IS NOT NULL
        GROUP BY rank, tipo_gestion_real
    ),
    total_cuentas_rank AS (
        SELECT 
            rank,
            COUNT(DISTINCT llave) AS total_cuentas
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          AND rank IS NOT NULL
        GROUP BY rank
    )
    SELECT 
        c.rank,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) AS correo_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS correo_intensidad,
        0 AS mensaje_cantidad,
        0.00 AS mensaje_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) AS whatsapp_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS whatsapp_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) AS llamada_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS llamada_intensidad,
        COALESCE(SUM(cantidad), 0) AS total_cantidad,
        ROUND(COALESCE(SUM(cantidad), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS total_intensidad,
        t.total_cuentas
    FROM conteo_rank c
    LEFT JOIN total_cuentas_rank t ON c.rank = t.rank
    GROUP BY c.rank, t.total_cuentas
    ORDER BY c.rank
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles para la fecha seleccionada."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Cuadro Gestión', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Cuadro de gestión generado ({fecha_str})"

# ============================================================
# REPORTE 4b: Cuadro de Gestión por RANK (DATAFRAME para consolidado)
# ============================================================

def generar_cuadro_gestion_rank_df(proyecto_id, fecha_reporte=None):
    """Devuelve DataFrame del Cuadro de Gestión por RANK (para consolidar)"""
    
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'"
    
    query = f"""
    WITH gestiones_filtradas AS (
        SELECT 
            g.llave,
            g.codigo_cliente,
            g.codigo_gestion,
            g.tipo_gestion,
            g.area_gestion,
            c.rank,
            CASE 
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion = 'T' THEN 'WHATSAPP'
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion != 'T' THEN 'CORREO'
                ELSE 'LLAMADA'
            END AS tipo_gestion_real
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        LEFT JOIN `{PROYECTO_BQ}.cartera_predemanda_jamar` c 
            ON g.llave = c.llave AND c.id_proyecto = '{proyecto_id}'
        WHERE g.id_proyecto = '{proyecto_id}'
          {filtro_fechas}
    ),
    conteo_rank AS (
        SELECT 
            rank,
            tipo_gestion_real,
            COUNT(*) AS cantidad,
            COUNT(DISTINCT llave) AS cuentas_gestionadas
        FROM gestiones_filtradas
        WHERE rank IS NOT NULL
        GROUP BY rank, tipo_gestion_real
    ),
    total_cuentas_rank AS (
        SELECT 
            rank,
            COUNT(DISTINCT llave) AS total_cuentas
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          AND rank IS NOT NULL
        GROUP BY rank
    )
    SELECT 
        c.rank,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) AS correo_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS correo_intensidad,
        0 AS mensaje_cantidad,
        0.00 AS mensaje_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) AS whatsapp_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS whatsapp_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) AS llamada_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS llamada_intensidad,
        COALESCE(SUM(cantidad), 0) AS total_cantidad,
        ROUND(COALESCE(SUM(cantidad), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS total_intensidad,
        t.total_cuentas
    FROM conteo_rank c
    LEFT JOIN total_cuentas_rank t ON c.rank = t.rank
    GROUP BY c.rank, t.total_cuentas
    ORDER BY c.rank
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    return df, f"✅ {len(df)} ranks"

# ============================================================
# REPORTE 5: Cuadro de Resultado de Gestión (individual)
# ============================================================

def generar_cuadro_resultado_gestion(proyecto_id, fecha_reporte=None):
    """Genera el Excel del Cuadro de Resultado de Gestión (reporte individual)"""
    
    df, mensaje = generar_cuadro_resultado_gestion_df(proyecto_id, fecha_reporte)
    
    if df is None:
        return None, mensaje
    
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(
            writer,
            sheet_name="Resultado Gestión",
            index=False,
        )
        
        worksheet = writer.sheets["Resultado Gestión"]
        worksheet.set_column(0, 25, 22)
        for i in range(1, 6):
            worksheet.set_column(i, i, 15)
    
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Cuadro de resultado generado ({fecha_str})"

# ============================================================
# REPORTE 5b: Cuadro de Resultado de Gestión (DATAFRAME para consolidado)
# ============================================================

def generar_cuadro_resultado_gestion_df(proyecto_id, fecha_reporte=None):
    """Devuelve DataFrame del Cuadro de Resultado de Gestión (para consolidar)"""
    
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'"
    
    query = f"""
    WITH ultima_gestion_por_cuenta AS (
        SELECT 
            g.llave,
            g.resultado_gestion,
            g.codigo_gestion AS ultimo_codigo_gestion,
            g.fechahoragestion,
            ROW_NUMBER() OVER (PARTITION BY g.llave ORDER BY g.fechahoragestion DESC) AS rn
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
          {filtro_fechas}
    ),
    ultima_gestion AS (
        SELECT 
            llave,
            resultado_gestion,
            ultimo_codigo_gestion,
            fechahoragestion
        FROM ultima_gestion_por_cuenta
        WHERE rn = 1
    ),
    cuentas_con_gestion AS (
        SELECT 
            c.llave,
            c.rank,
            CASE 
                WHEN ug.resultado_gestion IS NOT NULL THEN
                    CASE 
                        WHEN ug.resultado_gestion = 'CONTACTO EFECTIVO' THEN 'CONTACTO EFECTIVO'
                        WHEN ug.resultado_gestion = 'COMPROMISO DE PAGO' THEN 'COMPROMISO DE PAGO'
                        WHEN ug.resultado_gestion = 'NO CONTACTOS' THEN 'SIN CONTACTO'
                        WHEN ug.resultado_gestion = 'CONTACTO CON TERCERO' THEN 'CONTACTO TERCERO'
                        WHEN ug.resultado_gestion IN ('Tono ocupado', 'Equivocado', 'Ilocalizable') THEN 'BUZON DE VOZ'
                        ELSE 'SIN CONTACTO'
                    END
                WHEN ug.ultimo_codigo_gestion IN ('0', '1', '00', '01', '88', '89') THEN 'COMPROMISO DE PAGO'
                WHEN ug.ultimo_codigo_gestion IN ('14', '81', '84', '90') THEN 'CONTACTO EFECTIVO'
                WHEN ug.ultimo_codigo_gestion = '86' THEN 'SIN CONTACTO'
                WHEN ug.ultimo_codigo_gestion = '00' THEN 'CONTACTO CON TERCERO'
                WHEN ug.ultimo_codigo_gestion IN ('10', '11', '15') THEN 'BUZON DE VOZ'
                ELSE 'SIN CONTACTO'
            END AS categoria_resultado
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN ultima_gestion ug ON c.llave = ug.llave
        WHERE c.id_proyecto = '{proyecto_id}'
    )
    SELECT 
        categoria_resultado AS resultado_gestion,
        COALESCE(SUM(CASE WHEN rank = 'A' THEN 1 ELSE 0 END), 0) AS A,
        COALESCE(SUM(CASE WHEN rank = 'B' THEN 1 ELSE 0 END), 0) AS B,
        COALESCE(SUM(CASE WHEN rank = 'C' THEN 1 ELSE 0 END), 0) AS C,
        COALESCE(SUM(CASE WHEN rank = 'D' THEN 1 ELSE 0 END), 0) AS D,
        COALESCE(COUNT(*), 0) AS TOTAL
    FROM cuentas_con_gestion
    GROUP BY categoria_resultado
    ORDER BY 
        CASE categoria_resultado
            WHEN 'CONTACTO EFECTIVO' THEN 1
            WHEN 'COMPROMISO DE PAGO' THEN 2
            WHEN 'SIN CONTACTO' THEN 3
            WHEN 'CONTACTO TERCERO' THEN 4
            WHEN 'BUZON DE VOZ' THEN 5
            WHEN 'SIN GESTION AL CORTE' THEN 6
            ELSE 7
        END
    """
    
    df = preparar_para_excel(ejecutar_query(query))
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    return df, f"✅ {len(df)} categorías"

# ============================================================
# REPORTE 6: Recaudo y Compromisos (individual)
# ============================================================

def generar_recaudo_compromisos(proyecto_id, fecha_reporte=None):
    """Genera el Excel de Recaudo y Compromisos (reporte individual)"""
    
    df, mensaje = generar_recaudo_compromisos_df(proyecto_id)
    
    if df is None:
        return None, mensaje
    
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(
            writer,
            sheet_name="Recaudo y Compromisos",
            index=False,
        )
        
        worksheet = writer.sheets["Recaudo y Compromisos"]
        worksheet.set_column(0, 10, 22)
    
    return output.getvalue(), "✅ Reporte de recaudo y compromisos generado"

# ============================================================
# REPORTE 6b: Recaudo y Compromisos (DATAFRAME para consolidado)
# ============================================================

def generar_recaudo_compromisos_df(proyecto_id):
    """
    Devuelve DataFrame de Recaudo y Compromisos en formato pivote:
    Filas: RECAUDO, COMPROMISOS ACTIVOS, COMPROMISOS INCUMPLIDOS
    Columnas: A, B, C, D, TOTAL
    """
    
    query = f"""
    WITH recaudo_por_rank AS (
        SELECT
            c.rank,
            SUM(COALESCE(p.recaudo_periodo, 0)) AS recaudo
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN `{PROYECTO_BQ}.pagos_jamar` p
            ON c.llave = p.llave
           AND p.id_proyecto = '{proyecto_id}'
        WHERE c.id_proyecto = '{proyecto_id}'
        GROUP BY c.rank
    ),

    ultima_promesa AS (
        SELECT
            llave,
            valorpromesa,
            fechapromesa
        FROM `{PROYECTO_BQ}.gestiones_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          AND codigo_gestion IN ('1', '88', '89')
          AND valorpromesa IS NOT NULL
          AND valorpromesa > 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY llave
            ORDER BY fechahoragestion DESC
        ) = 1
    ),

    compromisos_por_rank AS (
        SELECT
            c.rank,
            SUM(
                IF(
                    up.fechapromesa >= CURRENT_DATE('America/Bogota'),
                    COALESCE(up.valorpromesa, 0),
                    0
                )
            ) AS compromisos_activos,
            SUM(
                IF(
                    up.fechapromesa < CURRENT_DATE('America/Bogota'),
                    COALESCE(up.valorpromesa, 0),
                    0
                )
            ) AS compromisos_incumplidos
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN ultima_promesa up ON c.llave = up.llave
        WHERE c.id_proyecto = '{proyecto_id}'
        GROUP BY c.rank
    ),

    datos_por_rank AS (
        SELECT
            c.rank,
            ROUND(COALESCE(r.recaudo, 0), 2) AS recaudo,
            ROUND(COALESCE(cp.compromisos_activos, 0), 2) AS compromisos_activos,
            ROUND(COALESCE(cp.compromisos_incumplidos, 0), 2) AS compromisos_incumplidos
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN recaudo_por_rank r ON c.rank = r.rank
        LEFT JOIN compromisos_por_rank cp ON c.rank = cp.rank
        WHERE c.id_proyecto = '{proyecto_id}'
        GROUP BY c.rank, r.recaudo, cp.compromisos_activos, cp.compromisos_incumplidos
    )

    SELECT 
        'RECAUDO' AS resultado_gestion,
        ROUND(SUM(IF(rank = 'A', recaudo, 0)), 2) AS A,
        ROUND(SUM(IF(rank = 'B', recaudo, 0)), 2) AS B,
        ROUND(SUM(IF(rank = 'C', recaudo, 0)), 2) AS C,
        ROUND(SUM(IF(rank = 'D', recaudo, 0)), 2) AS D,
        ROUND(SUM(recaudo), 2) AS TOTAL
    FROM datos_por_rank

    UNION ALL

    SELECT 
        'COMPROMISOS ACTIVOS' AS resultado_gestion,
        ROUND(SUM(IF(rank = 'A', compromisos_activos, 0)), 2) AS A,
        ROUND(SUM(IF(rank = 'B', compromisos_activos, 0)), 2) AS B,
        ROUND(SUM(IF(rank = 'C', compromisos_activos, 0)), 2) AS C,
        ROUND(SUM(IF(rank = 'D', compromisos_activos, 0)), 2) AS D,
        ROUND(SUM(compromisos_activos), 2) AS TOTAL
    FROM datos_por_rank

    UNION ALL

    SELECT 
        'COMPROMISOS INCUMPLIDOS' AS resultado_gestion,
        ROUND(SUM(IF(rank = 'A', compromisos_incumplidos, 0)), 2) AS A,
        ROUND(SUM(IF(rank = 'B', compromisos_incumplidos, 0)), 2) AS B,
        ROUND(SUM(IF(rank = 'C', compromisos_incumplidos, 0)), 2) AS C,
        ROUND(SUM(IF(rank = 'D', compromisos_incumplidos, 0)), 2) AS D,
        ROUND(SUM(compromisos_incumplidos), 2) AS TOTAL
    FROM datos_por_rank

    ORDER BY 
        CASE resultado_gestion
            WHEN 'RECAUDO' THEN 1
            WHEN 'COMPROMISOS ACTIVOS' THEN 2
            WHEN 'COMPROMISOS INCUMPLIDOS' THEN 3
        END
    """

    df = preparar_para_excel(ejecutar_query(query))

    if df.empty:
        return None, "⚠️ No hay datos disponibles."

    return df, f"✅ Recaudo y compromisos generado"

# ============================================================
# REPORTE 7: Resumen de Cartera (CONSOLIDADO CON DATOS COMPLETOS)
# ============================================================

def generar_resumen_cartera(proyecto_id, fecha_reporte=None):
    """
    Genera un Excel consolidado con:
    - Hoja 1: Resumen ejecutivo
    - Hoja 2: Cuadro de Gestión por RANK (con filtro de fecha)
    - Hoja 3: Cuadro de Resultado de Gestión (con filtro de fecha)
    - Hoja 4: Recaudo y Compromisos (sin filtro - acumulado) - FORMATO PIVOTE
    - Hoja 5: Datos Completos (las 1,076 cuentas con todos los datos)
    """
    
    output = io.BytesIO()
    mensajes = []
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        # ============================================================
        # HOJA 1: RESUMEN EJECUTIVO
        # ============================================================
        
        query_cartera = f"""
            SELECT 
                COUNT(*) AS total_cuentas,
                SUM(saldo_total_adeudado) AS saldo_total,
                AVG(saldo_total_adeudado) AS saldo_promedio,
                MAX(saldo_total_adeudado) AS saldo_max,
                MIN(saldo_total_adeudado) AS saldo_min,
                COUNT(DISTINCT rank) AS total_ranks
            FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
            WHERE id_proyecto = '{proyecto_id}'
        """
        df_cartera = ejecutar_query(query_cartera)
        
        query_gestiones = f"""
            SELECT 
                COUNT(*) AS total_gestiones,
                COUNT(DISTINCT llave) AS cuentas_gestionadas
            FROM `{PROYECTO_BQ}.gestiones_jamar`
            WHERE id_proyecto = '{proyecto_id}'
        """
        df_gestiones = ejecutar_query(query_gestiones)
        
        query_pagos = f"""
            SELECT 
                COUNT(*) AS total_pagos,
                SUM(recaudo_periodo) AS total_recaudo,
                COUNT(DISTINCT llave) AS cuentas_con_pago
            FROM `{PROYECTO_BQ}.pagos_jamar`
            WHERE id_proyecto = '{proyecto_id}'
        """
        df_pagos = ejecutar_query(query_pagos)
        
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
                'Total de pagos',
                'Total recaudado',
                'Cuentas con pago'
            ],
            'Valor': [
                proyecto_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                f"{df_cartera['total_cuentas'].iloc[0]:,}" if not df_cartera.empty else "0",
                f"${df_cartera['saldo_total'].iloc[0]:,.2f}" if not df_cartera.empty else "$0.00",
                f"${df_cartera['saldo_promedio'].iloc[0]:,.2f}" if not df_cartera.empty else "$0.00",
                f"${df_cartera['saldo_max'].iloc[0]:,.2f}" if not df_cartera.empty else "$0.00",
                f"${df_cartera['saldo_min'].iloc[0]:,.2f}" if not df_cartera.empty else "$0.00",
                f"{df_cartera['total_ranks'].iloc[0]:,}" if not df_cartera.empty else "0",
                f"{df_gestiones['total_gestiones'].iloc[0]:,}" if not df_gestiones.empty else "0",
                f"{df_gestiones['cuentas_gestionadas'].iloc[0]:,}" if not df_gestiones.empty else "0",
                f"{df_pagos['total_pagos'].iloc[0]:,}" if not df_pagos.empty else "0",
                f"${df_pagos['total_recaudo'].iloc[0]:,.2f}" if not df_pagos.empty else "$0.00",
                f"{df_pagos['cuentas_con_pago'].iloc[0]:,}" if not df_pagos.empty else "0"
            ]
        }
        df_resumen = pd.DataFrame(resumen_data)
        df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        mensajes.append("✅ Resumen generado")
        
        # ============================================================
        # HOJA 2: CUADRO DE GESTIÓN POR RANK
        # ============================================================
        
        df_gestion_rank, _ = generar_cuadro_gestion_rank_df(proyecto_id, fecha_reporte)
        
        if df_gestion_rank is not None and not df_gestion_rank.empty:
            df_gestion_rank.to_excel(writer, sheet_name='Cuadro Gestión', index=False)
            mensajes.append(f"✅ Cuadro Gestión: {len(df_gestion_rank)} ranks")
        else:
            pd.DataFrame({'Mensaje': ['No hay datos disponibles']}).to_excel(
                writer, sheet_name='Cuadro Gestión', index=False
            )
        
        # ============================================================
        # HOJA 3: CUADRO DE RESULTADO DE GESTIÓN
        # ============================================================
        
        df_resultado, _ = generar_cuadro_resultado_gestion_df(proyecto_id, fecha_reporte)
        
        if df_resultado is not None and not df_resultado.empty:
            df_resultado.to_excel(writer, sheet_name='Resultado Gestión', index=False)
            mensajes.append(f"✅ Resultado Gestión: {len(df_resultado)} categorías")
        else:
            pd.DataFrame({'Mensaje': ['No hay datos disponibles']}).to_excel(
                writer, sheet_name='Resultado Gestión', index=False
            )
        
        # ============================================================
        # HOJA 4: RECAUDO Y COMPROMISOS (FORMATO PIVOTE)
        # ============================================================
        
        df_recaudo, _ = generar_recaudo_compromisos_df(proyecto_id)
        
        if df_recaudo is not None and not df_recaudo.empty:
            df_recaudo.to_excel(writer, sheet_name='Recaudo y Compromisos', index=False)
            
            # Extraer totales
            for _, row in df_recaudo.iterrows():
                if row['resultado_gestion'] == 'RECAUDO':
                    mensajes.append(f"✅ Recaudo: ${row['TOTAL']:,.2f}")
                elif row['resultado_gestion'] == 'COMPROMISOS ACTIVOS':
                    mensajes.append(f"✅ Compromisos Activos: ${row['TOTAL']:,.2f}")
                elif row['resultado_gestion'] == 'COMPROMISOS INCUMPLIDOS':
                    mensajes.append(f"✅ Compromisos Incumplidos: ${row['TOTAL']:,.2f}")
        else:
            pd.DataFrame({'Mensaje': ['No hay datos disponibles']}).to_excel(
                writer, sheet_name='Recaudo y Compromisos', index=False
            )
        
        # ============================================================
        # HOJA 5: DATOS COMPLETOS (LAS 1,076 CUENTAS)
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
                ARRAY_AGG(resultado_gestion ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultimo_resultado,
                ARRAY_AGG(mejor_gestion_jamar ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultima_mejor_gestion,
                ARRAY_AGG(codigo_gestion ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultimo_codigo_gestion,
                ARRAY_AGG(fechahoragestion ORDER BY fechahoragestion DESC LIMIT 1)[OFFSET(0)] AS ultima_fecha_gestion
            FROM `{PROYECTO_BQ}.gestiones_jamar` g
            WHERE g.id_proyecto = '{proyecto_id}'
            GROUP BY llave
        ),
        ultima_promesa_valor AS (
            SELECT 
                llave,
                valorpromesa,
                fechapromesa
            FROM `{PROYECTO_BQ}.gestiones_jamar`
            WHERE id_proyecto = '{proyecto_id}'
              AND codigo_gestion IN ('1', '88', '89')
              AND valorpromesa IS NOT NULL
              AND valorpromesa > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY llave ORDER BY fechapromesa DESC) = 1
        )
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
            p.cantidad_pagos,
            p.total_recaudo,
            p.fecha_ultimo_pago_real,
            g.total_toques,
            g.fecha_ultima_gestion,
            g.promesas,
            g.contactos,
            g.no_contactos,
            g.contactos_tercero,
            g.ultimo_resultado,
            g.ultima_mejor_gestion,
            g.ultimo_codigo_gestion,
            up.valorpromesa AS ultimo_valor_promesa,
            up.fechapromesa AS fecha_ultima_promesa_valor
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN pagos_agrupados p ON c.llave = p.llave
        LEFT JOIN gestiones_agrupadas g ON c.llave = g.llave
        LEFT JOIN ultima_promesa_valor up ON c.llave = up.llave
        WHERE c.id_proyecto = '{proyecto_id}'
        ORDER BY c.saldo_total_adeudado DESC
        """
        
        df_datos = ejecutar_query(query_datos_completos)
        df_datos = preparar_para_excel(df_datos)
        
        if not df_datos.empty:
            df_datos.to_excel(writer, sheet_name='Datos Completos', index=False)
            mensajes.append(f"✅ Datos completos: {len(df_datos):,} cuentas")
        else:
            pd.DataFrame({'Mensaje': ['No hay datos disponibles']}).to_excel(
                writer, sheet_name='Datos Completos', index=False
            )
        
        # Ajustar ancho de columnas para todas las hojas
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 25, 18)
    
    mensaje_final = " | ".join(mensajes)
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Resumen de cartera generado ({fecha_str}) - {mensaje_final}"

# ============================================================
# REGISTRO DE REPORTES
# ============================================================

REPORTES = {
    "📊 Resumen de Cuentas Pre-Demanda": generar_resumen_predemanda,
    "📋 Cartera Comercial": generar_cartera_comercial,
    "📈 Seguimiento de Cobro": generar_seguimiento_cobro,
    "📊 Cuadro de Gestión por RANK": generar_cuadro_gestion_rank,
    "📊 Cuadro de Resultado de Gestión": generar_cuadro_resultado_gestion,
    "💰 Recaudo y Compromisos": generar_recaudo_compromisos,
    "📋 Resumen de Cartera (Consolidado)": generar_resumen_cartera,
}
